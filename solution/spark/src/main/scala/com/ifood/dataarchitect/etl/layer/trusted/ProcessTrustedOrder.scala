package com.ifood.dataarchitect.etl.layer.trusted

import org.apache.spark.sql.types.DateType

object ProcessTrustedOrder extends ProcessTrustedDatasets {

  override val appName: String = "Process trusted order dataset"

  override val databaseName: String = "trusted_layer"

  override val tableName: String = "order"

  override val partitionInfo: Map[String, Any] = Map(
    "partitionColumn" -> "restaurant_order_created_at",
    "partitionType" -> DateType,
    "partitionName" -> "restaurant_order_date",
    "dropSourceColumn" -> true
  )

  override def processTrustedDataset(datasetPath: String,
                                     datasetFormat: String): Unit = {
    createDatabase(databaseName = "raw_layer")

    createAnonConsumersView()
    createOrderJourneyView()
    createOrderChecksView()
    createValidOrdersView()
    createOrderRegisterView()
    createOrderWithStatusView()
    createTrustedOrderView()

    createTableFromView(viewName = "trusted_order",
                        datasetPath,
                        datasetFormat,
                        partitionInfo,
                        partitionNumber)

    dropTempView(viewName = "anon_consumer")
    dropTempView(viewName = "order_journey")
    dropTempView(viewName = "orders_check")
    dropTempView(viewName = "valid_orders")
    dropTempView(viewName = "order_register")
    dropTempView(viewName = "order_with_status")
    dropTempView(viewName = "trusted_order")
  }

  def createAnonConsumersView(): Unit = {
    val anonConsumerSql: String =
      """
        |SELECT
        |    c.`(customer_name|customer_phone_number|language|created_at|active)?+.+`,
        |    c.language AS customer_language,
        |    c.created_at AS customer_created_at,
        |    c.active AS customer_active,
        |    SHA2(c.customer_phone_number, 224) AS costumer_phone_number,
        |    SHA2(c.customer_name, 224) AS customer_name
        |FROM
        |    consumer c""".stripMargin

    createTempViewFromSQL(anonConsumerSql, viewName = "anon_consumer")
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

  def createValidOrdersView(): Unit = {
    val validOrdersSql: String =
      """
        |SELECT
        |    order_id,
        |    COALESCE(CAST(CAST(MAX(IF(value='REGISTERED', created_at, NULL)) AS DATE) AS STRING), '') AS registered_at,
        |    SUBSTR(MAX(CONCAT(CAST(created_at AS STRING), '==', value)), 22) AS status
        |FROM
        |    raw_layer.order_status
        |WHERE
        |    order_id IN (SELECT order_id FROM orders_check WHERE is_valid)
        |GROUP
        |    BY order_id""".stripMargin

    createTempViewFromSQL(validOrdersSql, viewName = "valid_orders")
  }

  def createOrderRegisterView(): Unit = {
    val orderRegisterSql: String =
      """
        |SELECT
        |    *,
        |    COALESCE(created_date, '') AS registered_at
        |FROM
        |    raw_layer.order""".stripMargin

    createTempViewFromSQL(orderRegisterSql, viewName = "order_register")
  }

  def createOrderWithStatusView(): Unit = {
    val orderWithStatusSql: String =
      """
        |SELECT
        |    or.`(cpf|customer_name|registration_date)?+.+`,
        |    SHA2(or.cpf, 224) AS cpf,
        |    vo.status AS status
        |FROM order_register or
        |LEFT JOIN
        |    valid_orders vo ON
        |    vo.order_id = or.order_id
        |    AND or.registered_at = vo.registered_at
        |WHERE
        |    vo.order_id IS NOT NULL""".stripMargin

    createTempViewFromSQL(orderWithStatusSql, viewName = "order_with_status")
  }

  def createTrustedOrderView(): Unit = {
    val trustedOrderSql: String =
      """
        |SELECT
        |    os.*,
        |    r.`(id)?+.+`,
        |    r.id as restaurant_id,
        |    ac.`(customer_id)?+.+`,
        |    CAST(FROM_UTC_TIMESTAMP(CAST(os.order_created_at AS STRING), os.merchant_timezone) AS DATE) AS restaurant_order_created_at
        |FROM
        |    order_with_status os
        |LEFT JOIN raw_layer.restaurant r ON
        |    os.merchant_id = r.id
        |LEFT JOIN anon_consumer ac ON
        |    os.customer_id = ac.customer_id""".stripMargin

    createTempViewFromSQL(trustedOrderSql, viewName = "trusted_order")
  }
}
