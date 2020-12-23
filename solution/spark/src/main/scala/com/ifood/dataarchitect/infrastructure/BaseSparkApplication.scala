package com.ifood.dataarchitect.infrastructure

import org.apache.spark.sql.SparkSession

trait BaseSparkApplication {

  val appName: String = "Ifood Data Architect test"

  lazy val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .appName(name = appName)
    .getOrCreate()

  def loadConfigs(): Unit = {
    val sc = spark.sparkContext
    sc.hadoopConfiguration
      .set("spark.network.timeout", "1000000")
    sc.hadoopConfiguration
      .set("parquet.enable.dictionary", "false")
  }

  def stopSparkApplication(): Unit = {
    spark.sparkContext.stop()
  }
}
