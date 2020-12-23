package com.ifood.dataarchitect.etl.layer.trusted

object ProcessTrustedOrderItems extends ProcessTrustedDatasets {

  override val appName: String = "Process trusted order items dataset"

  override val databaseName: String = "trusted_layer"

  override val tableName: String = "order_items"

  override def processTrustedDataset(datasetPath: String,
                                     datasetFormat: String): Unit = {
    useDatabase(databaseName = "raw_layer")

    createOrderJourneyView()
    createOrderChecksView()
    createItemsExpandedView()
    createGarnishExpandedView()
    createOrderItemsView()

    createTableFromView(viewName = "order_items",
                        datasetPath,
                        datasetFormat,
                        partitionInfo,
                        partitionNumber)

    dropTempView(viewName = "order_journey")
    dropTempView(viewName = "orders_check")
    dropTempView(viewName = "items_expanded")
    dropTempView(viewName = "garnish_expanded")
    dropTempView(viewName = "order_items")
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

  def createItemsExpandedView(): Unit = {
    val itemsExpandedSql =
      """
        |SELECT
        |    order_id,
        |    EXPLODE(items) AS items
        |FROM
        |    raw_layer.order
        |WHERE order_id IN (SELECT order_id FROM orders_check WHERE is_valid)""".stripMargin

    createTempViewFromSQL(itemsExpandedSql, viewName = "items_expanded")
  }

  def createGarnishExpandedView(): Unit = {
    val garnishExpandedSql =
      """
        |SELECT
        |    order_id,
        |    items.externalId AS external_id,
        |    items.name AS name,
        |    items.addition.currency AS currency,
        |    items.addition.value AS addition_value,
        |    items.discount.value AS discount_value,
        |    items.quantity AS quantity,
        |    items.sequence AS sequence,
        |    items.unitPrice.value AS unit_price_value,
        |    items.totalValue.value AS total_value,
        |    items.customerNote AS customer_note,
        |    EXPLODE(items.garnishItems) AS garnish_item,
        |    items.integrationId AS integration_id,
        |    items.totalAddition.value AS total_addition_value,
        |    items.totalDiscount.value AS total_discount_value
        |FROM
        |    items_expanded""".stripMargin

    createTempViewFromSQL(garnishExpandedSql, viewName = "garnish_expanded")
  }

  def createOrderItemsView(): Unit = {
    val orderItemsSql =
      """
        |SELECT
        |    order_id,
        |    external_id,
        |    name,
        |    currency,
        |    addition_value,
        |    discount_value,
        |    quantity,
        |    sequence,
        |    unit_price_value,
        |    total_value,
        |    customer_note,
        |    integration_id,
        |    total_addition_value,
        |    total_discount_value,
        |    garnish_item.name AS garnish_name,
        |    garnish_item.externalId AS garnish_external_id,
        |    garnish_item.categoryId AS garnish_category_id,
        |    garnish_item.categoryName AS garnish_category_name,
        |    garnish_item.addition.value AS garnish_addition_value,
        |    garnish_item.discount.value AS garnish_discount_value,
        |    garnish_item.quantity AS garnish_quantity,
        |    garnish_item.sequence AS garnish_sequence,
        |    garnish_item.unitPrice.value AS garnish_unit_price_value,
        |    garnish_item.totalValue.value AS garnish_total_value,
        |    garnish_item.integrationId AS garnish_integration_id
        |FROM
        |    garnish_expanded""".stripMargin

    createTempViewFromSQL(orderItemsSql, viewName = "order_items")
  }
}
