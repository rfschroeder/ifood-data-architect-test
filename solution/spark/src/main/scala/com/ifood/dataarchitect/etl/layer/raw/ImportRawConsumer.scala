package com.ifood.dataarchitect.etl.layer.raw

import org.apache.spark.sql.types.{
  BooleanType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

object ImportRawConsumer extends ImportRawDataset {

  override val appName: String = "Import raw consumer dataset"

  override val databaseName: String = "raw_layer"

  override val tableName: String = "consumer"

  override val schema: StructType = StructType(
    List(
      StructField("customer_id", StringType, nullable = true),
      StructField("language", StringType, nullable = true),
      StructField("created_at", TimestampType, nullable = true),
      StructField("active", BooleanType, nullable = true),
      StructField("customer_name", StringType, nullable = true),
      StructField("customer_phone_area", StringType, nullable = true),
      StructField("customer_phone_number", StringType, nullable = true)
    )
  )
}
