package com.ifood.dataarchitect.etl.layer.trusted

object ProcessTrustedOrderStatus extends ProcessTrustedDatasets {

  override val appName: String = "Process trusted order status dataset"

  override val databaseName: String = "trusted_layer"

  override val tableName: String = "order_status"

  override def processTrustedDataset(datasetPath: String,
                                     datasetFormat: String): Unit = {
    useDatabase(databaseName = "raw_layer")

    createOrderJourneyView()
    createOrderChecksView()
    createOrderStatusView()

    createTableFromView(viewName = "order_status",
                        datasetPath,
                        datasetFormat,
                        partitionInfo,
                        partitionNumber)

    dropTempView(viewName = "order_journey")
    dropTempView(viewName = "orders_check")
    dropTempView(viewName = "order_status")
  }

  def createOrderJourneyView(): Unit = {
    val orderJourneySql: String =
      """
        |SELECT
        |    order_id,
        |    MAX(IF(value='REGISTERED', created_at, NULL)) AS registered_at,
        |    MAX(IF(value='PLACED', created_at, NULL)) AS placed_at,
        |    MAX(IF(value='CONCLUDED', created_at, NULL)) AS concluded_at,
        |    MAX(IF(value='CANCELLED', created_at, NULL)) AS cancelled_at
        |FROM
        |    raw_layer.order_status
        |GROUP
        |    BY order_id""".stripMargin

    createTempViewFromSQL(orderJourneySql, viewName = "order_journey")
  }

  def createOrderChecksView(): Unit = {
    val validOrdersSql: String =
      """
        |SELECT
        |    order_id,
        |    (
        |    	   (registered_at is not null
        |    		     AND placed_at is not null
        |    		     AND registered_at < placed_at
        |    		     AND concluded_at is not null
        |    		     AND concluded_at > placed_at)
        |    	   OR (registered_at is not null
        |    		     AND placed_at is not null
        |    		     AND placed_at > registered_at
        |    		     AND cancelled_at is not null
        |    		     AND cancelled_at > placed_at)
        |        OR (registered_at is not null
        |	           AND placed_at is not null
        |	           AND cancelled_at is null
        |	           AND concluded_at is null)
        |        OR (registered_at is not null
        |	           AND placed_at is not null
        |	           AND concluded_at is null
        |	           AND cancelled_at is not null
        |	           AND cancelled_at > placed_at)
        |        OR (registered_at is not null
        |	           AND placed_at is not null
        |	           AND concluded_at is not null
        |	           AND cancelled_at is null
        |	           AND concluded_at > placed_at)
        |	       OR (registered_at is not null
        |    		     AND placed_at is null
        |    		     AND concluded_at is null
        |    		     AND cancelled_at is not null
        |    		     AND cancelled_at > registered_at)
        |		     OR (registered_at is not null
        |    		     AND placed_at is null
        |    		     AND concluded_at is null
        |    		     AND cancelled_at is null)
        |	   ) AS is_valid
        |FROM
        |    order_journey""".stripMargin

    createTempViewFromSQL(validOrdersSql, viewName = "orders_check")
  }

  def createOrderStatusView(): Unit = {
    val orderStatusSql: String =
      """
      |SELECT
      |    order_id,
      |    MAX(IF(value='REGISTERED', created_at, NULL)) AS registered_at,
      |    MAX(IF(value='PLACED', created_at, NULL)) AS placed_at,
      |    MAX(IF(value='CONCLUDED', created_at, NULL)) AS concluded_at,
      |    MAX(IF(value='CANCELLED', created_at, NULL)) AS cancelled_at
      |FROM
      |    raw_layer.order_status
      |WHERE
      |    order_id IN (SELECT order_id FROM orders_check WHERE is_valid)
      GROUP
      |    BY order_id""".stripMargin

    createTempViewFromSQL(orderStatusSql, viewName = "order_status")
  }
}
