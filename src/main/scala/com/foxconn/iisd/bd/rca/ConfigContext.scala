package com.foxconn.iisd.bd.rca

import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 *
 *
 * @author JasonLai
 * @date 2019/10/3 下午3:49
 * @description 程式所需的所有設定值
 */
class ConfigContext extends Serializable {

  //env
  var env = ""
  //flag
  var flag = ""
  //job
  var job: Job = null
  var jobDateFmt = ""
  var isJobState = false
  // data processing
  var dataProcessingMode = ""
  //file
  var wipPath = ""
  var wipPartsPath = ""
  var bobcatPath = ""
  var wipColumns = ""
  var wipPartsColumns = ""
  var bobcatColumns = ""
  var wipDtFmt = ""
  var wipPartsDtFmt = ""
  var bobcatDtFmt = ""
  var dataSeperator = ""
  var mbLimits = 0
  var residualCapacityDataSize: Long = 0
  //spark
  var sparkJobName = ""
  var sparkMaster = ""
  var sparkSession: SparkSession = null
  var sparkNumExcutors = 1
  //minio
  var minioEndpoint = ""
  var minioAccessKey = ""
  var minioSecretKey = ""
  var minioBucket = ""
  var minioConnectionSslEnabled = false
  //bu
  var factory = ""
  //cockroachdb
  var cockroachDbConnUrlStr = ""
  var cockroachDbSslMode = ""
  var cockroachDbUserName = ""
  var cockroachDbPassword = ""
  var cockroachDbMasterTable = ""
  var cockroachDbDetailTable = ""
  var cockroachDbTestTable = ""
  //mysqldb
  var mysqlDbConnUrlStr = ""
  var mysqlDatabase = ""
  var mysqlDbUserName = ""
  var mysqlDbPassword = ""
  var mysqlProductBwListTable = ""
  var mysqlProductInfoTable = ""
  var mysqlProductFloorLineTable = ""
  var mysqlRiskAssemblyStationSnTable = ""
  var mysqlRiskPartSnTable = ""
  var mysqlRiskTestStationSnTable = ""
  //micro batch time group
  var microBatchTimeGroupDurationMin = 10
  //datafrmae
  var productBwListTableDf: DataFrame = null
  var productInfoTableDf: DataFrame = null
  //datasource
  var fileDataSource: FileSource = null
  var cockroachDBIo: CockroachDBIo = null
  var mysqlDBIo: MysqlDBIo = null
  //summaryfile
  var summaryFileLogBasePath = ""
  var summaryFileLogTag = ""
  var summaryFileExtension = ""
  var summaryFileJobFmt = ""
  //daily
  var dailyCalculationStartHour = 0
  var dailyCalculationDurationHour = 0
  //record runtime
  var processingStartTime: Long = _
  var processingEndTime: Long = _
}
