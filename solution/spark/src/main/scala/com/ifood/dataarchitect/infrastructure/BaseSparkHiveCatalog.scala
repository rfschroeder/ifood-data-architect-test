package com.ifood.dataarchitect.infrastructure

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.sql.{DataFrame, SaveMode}

trait BaseSparkHiveCatalog extends BaseSparkApplication {

  val databaseName: String

  val tableName: String

  val partitionInfo: Map[String, Any] = null

  val partitionNumber: Integer = 1

  def createDatabase(databaseName: String): Unit = {
    spark.sql(sqlText = s"CREATE DATABASE IF NOT EXISTS $databaseName")
    spark.sql(sqlText = s"USE $databaseName")
  }

  def useDatabase(databaseName: String): Unit = {
    spark.sql(sqlText = s"USE $databaseName")
  }

  def dropTable(tableName: String): Unit = {
    spark.sql(sqlText = s"DROP TABLE IF EXISTS $tableName")
  }

  def dropTempView(viewName: String): Unit = {
    spark.catalog.dropTempView(viewName)
  }

  def createTempViewFromSQL(sql: String, viewName: String): DataFrame = {
    spark.sql(sqlText = "set spark.sql.parser.quotedRegexColumnNames=true")
    val createTableStmt = s"""
      CREATE OR REPLACE TEMPORARY VIEW $viewName AS
      $sql
    """
    spark.sql(createTableStmt)
  }

  def storeDataset(datasetDF: DataFrame,
                   datasetPath: String,
                   datasetFormat: String,
                   partitionInfo: Map[String, Any] = null,
                   partitionNumber: Integer = 1): Unit = {
    var rawDatasetDF = datasetDF.repartition(numPartitions = partitionNumber)

    partitionInfo match {
      case null =>
        rawDatasetDF.write
          .format(datasetFormat)
          .mode(SaveMode.Overwrite)
          .save(path = s"$datasetPath/$tableName")
      case _ =>
        rawDatasetDF = rawDatasetDF
          .withColumn(
            partitionInfo("partitionName").toString,
            col(partitionInfo("partitionColumn").toString)
              .cast(partitionInfo("partitionType").asInstanceOf[DataType]))
        if (partitionInfo isDefinedAt "dropSourceColumn") {
          if (partitionInfo("dropSourceColumn")
                .asInstanceOf[Boolean]) {
            rawDatasetDF =
              rawDatasetDF.drop(partitionInfo("partitionColumn").toString)
          }
        }

        rawDatasetDF.write
          .partitionBy(partitionInfo("partitionName").toString)
          .format(datasetFormat)
          .mode(SaveMode.Overwrite)
          .save(path = s"$datasetPath/$tableName")

        rawDatasetDF =
          rawDatasetDF.drop(partitionInfo("partitionName").toString)
    }

    createTableFromDataFrame(rawDatasetDF,
                             datasetPath,
                             datasetFormat,
                             partitionInfo)
  }

  def createTableFromDataFrame(tableDF: DataFrame,
                               tablePath: String,
                               targetFormat: String,
                               partitionInfo: Map[String, Any] = null): Unit = {

    createDatabase(databaseName)
    dropTable(tableName)

    var createTableStmt: String =
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS $databaseName.$tableName (
         |${getDatasetSchemaFields(tableDF)}
         |)""".stripMargin

    partitionInfo match {
      case null =>
        createTableStmt = createTableStmt.concat(s"""
           |STORED AS ${targetFormat.toUpperCase}
           |LOCATION '$tablePath/$tableName'""".stripMargin)

        spark.sql(createTableStmt)
      case _ =>
        createTableStmt = createTableStmt.concat(
          s"""
          |PARTITIONED BY (${partitionInfo("partitionName").toString + ' ' + partitionInfo(
               "partitionType").asInstanceOf[DataType].typeName})
          |STORED AS ${targetFormat.toUpperCase}
          |LOCATION '$tablePath/$tableName'""".stripMargin
        )

        spark.sql(createTableStmt)
        spark.sql(sqlText = s"MSCK REPAIR TABLE $databaseName.$tableName")
    }
  }

  def createTableFromView(viewName: String,
                          tablePath: String,
                          tableFormat: String,
                          partitionInfo: Map[String, Any] = null,
                          partitionNumber: Integer = 1): Unit = {
    val tableDF = spark.sql(sqlText = s"SELECT * FROM $viewName")

    storeDataset(tableDF,
                 tablePath,
                 tableFormat,
                 partitionInfo,
                 partitionNumber)

    createTableFromDataFrame(tableDF, tablePath, tableFormat, partitionInfo)
  }

  def getDatasetSchemaFields(datasetDF: DataFrame): String =
    datasetDF.schema.fields
      .map((field: StructField) =>
        field.name + " " + (field.dataType.typeName match {
          case "array"  => field.dataType.catalogString
          case "struct" => field.dataType.catalogString
          case _        => field.dataType.typeName
        }))
      .mkString(",\n")
}
