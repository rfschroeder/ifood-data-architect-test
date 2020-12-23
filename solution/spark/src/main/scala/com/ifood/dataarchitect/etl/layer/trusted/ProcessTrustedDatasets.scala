package com.ifood.dataarchitect.etl.layer.trusted

import com.ifood.dataarchitect.infrastructure.BaseSparkHiveCatalog

trait ProcessTrustedDatasets extends BaseSparkHiveCatalog {

  def main(args: Array[String]): Unit = {
    val datasetPath = args(0)
    val datasetFormat = args(1)

    loadConfigs()

    processTrustedDataset(
      datasetPath,
      datasetFormat
    )

    stopSparkApplication()
  }

  def processTrustedDataset(datasetPath: String, datasetFormat: String): Unit
}
