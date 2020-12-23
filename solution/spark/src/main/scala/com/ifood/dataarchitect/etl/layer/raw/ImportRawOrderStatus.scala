package com.ifood.dataarchitect.etl.layer.raw

import org.apache.spark.sql.types.{
  StringType,
  StructField,
  StructType,
  TimestampType
}

object ImportRawOrderStatus extends ImportRawDataset {

  override val appName: String = "Import raw order status dataset"

  override val databaseName: String = "raw_layer"

  override val tableName: String = "order_status"

  override val partitionNumber: Integer = 4

  override val schema: StructType = StructType(
    List(
      StructField("created_at", TimestampType, nullable = true),
      StructField("order_id", StringType, nullable = true),
      StructField("status_id", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )
  )
}
