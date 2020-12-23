package com.ifood.dataarchitect.etl.layer.raw

import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DateType,
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

object ImportRawOrder extends ImportRawDataset {

  override val appName: String = "Import raw order dataset"

  override val databaseName: String = "raw_layer"

  override val tableName: String = "order"

  val currencySchema: StructType = StructType(
    List(
      StructField("value", StringType, nullable = true),
      StructField("currency", StringType, nullable = true)
    )
  )

  val garnishItems: ArrayType = ArrayType(
    StructType(
      List(
        StructField("name", StringType, nullable = true),
        StructField("addition", currencySchema, nullable = true),
        StructField("discount", currencySchema, nullable = true),
        StructField("quantity", DoubleType, nullable = true),
        StructField("sequence", IntegerType, nullable = true),
        StructField("unitPrice", currencySchema, nullable = true),
        StructField("categoryId", StringType, nullable = true),
        StructField("externalId", StringType, nullable = true),
        StructField("totalValue", currencySchema, nullable = true),
        StructField("categoryName", StringType, nullable = true),
        StructField("integrationId", StringType, nullable = true)
      )
    )
  )

  val schemaItems: ArrayType = ArrayType(
    StructType(
      List(
        StructField("name", StringType, nullable = true),
        StructField("addition", currencySchema, nullable = true),
        StructField("discount", currencySchema, nullable = true),
        StructField("quantity", DoubleType, nullable = true),
        StructField("sequence", IntegerType, nullable = true),
        StructField("unitPrice", currencySchema, nullable = true),
        StructField("externalId", StringType, nullable = true),
        StructField("totalValue", currencySchema, nullable = true),
        StructField("customerNote", StringType, nullable = true),
        StructField("garnishItems", garnishItems, nullable = true),
        StructField("integrationId", StringType, nullable = true),
        StructField("totalAddition", currencySchema, nullable = true),
        StructField("totalDiscount", currencySchema, nullable = true)
      )
    )
  )

  override val schema: StructType = StructType(
    List(
      StructField("cpf", StringType, nullable = true),
      StructField("customer_id", StringType, nullable = true),
      StructField("customer_name", StringType, nullable = true),
      StructField("delivery_address_city", StringType, nullable = true),
      StructField("delivery_address_country", StringType, nullable = true),
      StructField("delivery_address_district", StringType, nullable = true),
      StructField("delivery_address_external_id", StringType, nullable = true),
      StructField("delivery_address_latitude", StringType, nullable = true),
      StructField("delivery_address_longitude", StringType, nullable = true),
      StructField("delivery_address_state", StringType, nullable = true),
      StructField("delivery_address_zip_code", StringType, nullable = true),
      StructField("items", StringType, nullable = true),
      StructField("merchant_id", StringType, nullable = true),
      StructField("merchant_latitude", StringType, nullable = true),
      StructField("merchant_longitude", StringType, nullable = true),
      StructField("merchant_timezone", StringType, nullable = true),
      StructField("order_created_at", TimestampType, nullable = true),
      StructField("order_id", StringType, nullable = true),
      StructField("order_scheduled", BooleanType, nullable = true),
      StructField("order_scheduled_date", TimestampType, nullable = true),
      StructField("order_total_amount", DoubleType, nullable = true),
      StructField("origin_platform", StringType, nullable = true)
    )
  )

  override val partitionInfo: Map[String, Any] = Map(
    "partitionColumn" -> "order_created_at",
    "partitionType" -> DateType,
    "partitionName" -> "created_date"
  )

  override val partitionNumber: Integer = 10
}
