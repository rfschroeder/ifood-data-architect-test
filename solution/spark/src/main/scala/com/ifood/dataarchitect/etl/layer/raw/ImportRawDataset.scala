package com.ifood.dataarchitect.etl.layer.raw

import com.ifood.dataarchitect.etl.layer.raw.ImportRawOrder.schemaItems
import com.ifood.dataarchitect.infrastructure.BaseSparkHiveCatalog
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

trait ImportRawDataset extends BaseSparkHiveCatalog {

  val schema: StructType

  def main(args: Array[String]): Unit = {
    val sourceDatasetPath = args(0)
    val sourceFormat = args(1)
    val targetDatasetPath = args(2)
    val targetFormat = args(3)

    loadConfigs()

    processImportDataset(sourceDatasetPath,
                         sourceFormat,
                         targetDatasetPath,
                         targetFormat)

    stopSparkApplication()
  }

  def processImportDataset(sourceDatasetPath: String,
                           sourceFormat: String,
                           targetDatasetPath: String,
                           targetFormat: String): Unit = {
    var datasetDF =
      getSourceDataset(sourceDatasetPath, sourceFormat)

    if (tableName.equals("order")) {
      // To ensure that `items` col will be of type ArrayType[StructType]
      datasetDF = datasetDF
        .withColumn(colName = "items",
                    from_json(col(colName = "items"), schemaItems))
    }

    processLoadDataset(datasetDF, targetDatasetPath, targetFormat)
  }

  def getSourceDataset(sourceDatasetPath: String,
                       sourceFormat: String): DataFrame = sourceFormat match {
    case "json" => spark.read.schema(schema).json(sourceDatasetPath)
    case _ =>
      spark.read
        .schema(schema)
        .option("header", value = true)
        .csv(sourceDatasetPath)
  }

  def processLoadDataset(datasetDF: DataFrame,
                         datasetPath: String,
                         datasetFormat: String): Unit = {

    storeDataset(datasetDF.dropDuplicates(),
                 datasetPath,
                 datasetFormat,
                 partitionInfo,
                 partitionNumber)
  }
}
