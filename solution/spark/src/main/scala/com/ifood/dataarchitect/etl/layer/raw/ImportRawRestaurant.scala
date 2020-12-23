package com.ifood.dataarchitect.etl.layer.raw

import org.apache.spark.sql.types.{
  BooleanType,
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

object ImportRawRestaurant extends ImportRawDataset {

  override val appName: String = "Import raw restaurant dataset"

  override val databaseName: String = "raw_layer"

  override val tableName: String = "restaurant"

  override val schema: StructType = StructType(
    List(
      StructField("id", StringType, nullable = true),
      StructField("created_at", TimestampType, nullable = true),
      StructField("enabled", BooleanType, nullable = true),
      StructField("price_range", IntegerType, nullable = true),
      StructField("average_ticket", DoubleType, nullable = true),
      StructField("takeout_time", IntegerType, nullable = true),
      StructField("delivery_time", IntegerType, nullable = true),
      StructField("minimum_order_value", DoubleType, nullable = true),
      StructField("merchant_zip_code", StringType, nullable = true),
      StructField("merchant_city", StringType, nullable = true),
      StructField("merchant_state", StringType, nullable = true),
      StructField("merchant_country", StringType, nullable = true)
    )
  )
}
