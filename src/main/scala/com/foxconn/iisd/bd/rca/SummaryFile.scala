package com.foxconn.iisd.bd.rca.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import com.foxconn.iisd.bd.rca.{ConfigContext, KEConstants, MinioIo}
import com.google.gson.Gson
import org.apache.spark.sql.{DataFrame, SparkSession}

object SummaryFile {

//  case class PartMaster(firstTime: String, lastTime: String, totalCnt: Int)
//  case class PartDetail(firstTime: String, lastTime: String, totalCnt: Int)
//  case class PartTest(firstTime: String, lastTime: String, totalCnt: Int)
//  case class RiskAssemblyStationSn(firstTime: String, lastTime: String, totalCnt: Int)
//  case class RiskPartSn(firstTime: String, lastTime: String, totalCnt: Int)
//  case class RiskTestStationSn(firstTime: String, lastTime: String, totalCnt: Int)

//  case class FilesName(masterFilesNameList: util.ArrayList[String], detailFilesNameList: util.ArrayList[String], testFilesNameList: util.ArrayList[String])
  case class InputFileNames(masterFilesNameList: util.ArrayList[String], detailFilesNameList: util.ArrayList[String], testFilesNameList: util.ArrayList[String])
  case class FilesTotalRows(masterFilesTotalRows: Long, detailFilesTotalRows: Long, testFilesTotalRows: Long)
  case class FilesTotalRowsDist(masterFilesTotalRowsDist: Long, detailFilesTotalRowsDist: Long, testFilesTotalRowsDist: Long)
  case class FirstRowTimeInCockroachdb(masterFirstRowTimeInCockroachdb: String, detailFirstRowTimeInCockroachdb: String, testFirstRowTimeInCockroachdb: String)
  case class LastRowTimeInCockroachdb(masterLastRowTimeInCockroachdb: String, detailLastRowTimeInCockroachdb: String, testLastRowTimeInCockroachdb: String)
  case class TotalRowsInCockroachdb(masterTotalRowsInCockroachdb: Long, detailTotalRowsInCockroachdb: Long, testTotalRowsInCockroachdb: Long)
  case class TotalRowsDistInCockroachdb(masterTotalRowsDistInCockroachdb: Long, detailTotalRowsDistInCockroachdb: Long, testTotalRowsDistInCockroachdb: Long)
  case class TotalRowsValidateInCockroachdb(masterTotalRowsValidateInCockroachdb: Long, detailTotalRowsValidateInCockroachdb: Long, testTotalRowsValidateInCockroachdb: Long)
  case class FirstRowTimeInMysqldb(riskAssemblyStationFirstRowTimeInMysqldb: String, riskPartFirstRowTimeInMysqldb: String, riskTestStationFirstRowTimeInMysqldb: String)
  case class LastRowTimeInMysqldb(riskAssemblyStationLastRowTimeInMysqldb: String, riskPartLastRowTimeInMysqldb: String, riskTestStationLastRowTimeInMysqldb: String)
  case class TotalRowsInMysqldb(riskAssemblyStationTotalRowsInMysqldb: Long, riskPartTotalRowsInMysqldb: Long, riskTestStationTotalRowsInMysqldb: Long)
  case class Job(id: String, startTime: String, endTime: String, status: String, message: String)
  case class KeSummaryJson(template_json: String, ke: util.ArrayList[SummaryJson])
  case class SummaryJson(
                          inputFileNames: InputFileNames,
                          filesTotalRows: FilesTotalRows,
                          filesTotalRowsDist: FilesTotalRowsDist,
                          firstRowTimeInCockroachdb: FirstRowTimeInCockroachdb,
                          lastRowTimeInCockroachdb: LastRowTimeInCockroachdb,
                          totalRowsInCockroachdb: TotalRowsInCockroachdb,
                          totalRowsDistInCockroachdb: TotalRowsDistInCockroachdb,
                          totalRowsValidateInCockroachdb: TotalRowsValidateInCockroachdb,
                          firstRowTimeInMysqldb: FirstRowTimeInMysqldb,
                          lastRowTimeInMysqldb: LastRowTimeInMysqldb,
                          totalRowsInMysqldb: TotalRowsInMysqldb,
                          job: Job)

  var masterFilesRows: util.HashMap[String, Integer] = null
  var detailFilesRows: util.HashMap[String, Integer] = null
  var testFilesRows: util.HashMap[String, Integer] = null

  var masterFilesNameList: util.ArrayList[String] = null
  var detailFilesNameList: util.ArrayList[String] = null
  var testFilesNameList: util.ArrayList[String] = null

  var masterFilesTotalRows: Long = 0
  var detailFilesTotalRows: Long = 0
  var testFilesTotalRows: Long = 0

  var masterFilesTotalRowsDist: Long = 0
  var detailFilesTotalRowsDist: Long = 0
  var testFilesTotalRowsDist: Long = 0

  var masterFirstRowTimeInCockroachdb: String = null
  var detailFirstRowTimeInCockroachdb: String = null
  var testFirstRowTimeInCockroachdb: String = null

  var masterLastRowTimeInCockroachdb: String = null
  var detailLastRowTimeInCockroachdb: String = null
  var testLastRowTimeInCockroachdb: String = null

  var masterTotalRowsInCockroachdb: Long = 0
  var detailTotalRowsInCockroachdb: Long = 0
  var testTotalRowsInCockroachdb: Long = 0

  var masterTotalRowsDistInCockroachdb: Long = 0
  var detailTotalRowsDistInCockroachdb: Long = 0
  var testTotalRowsDistInCockroachdb: Long = 0

  var masterTotalRowsValidateInCockroachdb: Long = 0
  var detailTotalRowsValidateInCockroachdb: Long = 0
  var testTotalRowsValidateInCockroachdb: Long = 0

  var riskAssemblyStationFirstRowTimeInMysqldb: String = null
  var riskPartFirstRowTimeInMysqldb: String = null
  var riskTestStationFirstRowTimeInMysqldb: String = null

  var riskAssemblyStationLastRowTimeInMysqldb: String = null
  var riskPartLastRowTimeInMysqldb: String = null
  var riskTestStationLastRowTimeInMysqldb: String = null

  var riskAssemblyStationTotalRowsInMysqldb: Long = 0
  var riskPartTotalRowsInMysqldb: Long = 0
  var riskTestStationTotalRowsInMysqldb: Long = 0

  var startTime: String = null
  var endTime: String = null
  var id: String = null
  var status: String = ""
  var message: String = ""

  var summaryFileJobFmt = ""

//  def setMasterFilesNameList(masterFilesNameList: util.ArrayList[String]): Unit = {
//    this.masterFilesNameList = masterFilesNameList
//  }
//
//  def setMasterFilesRows(map: util.HashMap[String, Integer]): Unit = {
//    this.masterFilesRows = map;
//  }
//
//  def setDetailFilesNameList(detailFilesNameList: util.ArrayList[String]): Unit = {
//    this.detailFilesNameList = detailFilesNameList
//  }
//
//  def setDetailFilesRows(map: util.HashMap[String, Integer]): Unit = {
//    this.detailFilesRows = map;
//  }
//
//  def setTestFilesNameList(testFilesNameList: util.ArrayList[String]): Unit = {
//    this.testFilesNameList = testFilesNameList
//  }
//
//  def setTestFilesRows(map: util.HashMap[String, Integer]): Unit = {
//    this.testFilesRows = map;
//  }
//
//  def setMasterFilesTotalRows(masterFilesTotalRows: Long): Unit = {
//    this.masterFilesTotalRows = masterFilesTotalRows
//  }
//
//  def setDetailFilesTotalRows(detailFilesTotalRows: Long): Unit = {
//    this.detailFilesTotalRows = detailFilesTotalRows
//  }
//
//  def setTestFilesTotalRows(testFilesTotalRows: Long): Unit = {
//    this.testFilesTotalRows = testFilesTotalRows
//  }
//
//  def setMasterFilesTotalRowsDist(masterFilesTotalRowsDist: Long): Unit = {
//    this.masterFilesTotalRowsDist = masterFilesTotalRowsDist
//  }
//
//  def getMasterFilesTotalRowsDist(): Long = {
//    masterFilesTotalRowsDist
//  }
//
//  def setDetailFilesTotalRowsDist(detailFilesTotalRowsDist: Long): Unit = {
//    this.detailFilesTotalRowsDist = detailFilesTotalRowsDist
//  }
//
//  def setTestFilesTotalRowsDist(testFilesTotalRowsDist: Long): Unit = {
//    this.testFilesTotalRowsDist = testFilesTotalRowsDist
//  }
//
//  def setMasterFirstRowTimeInCockroachdb(masterFirstRowTimeInCockroachdb: String): Unit = {
//    this.masterFirstRowTimeInCockroachdb = masterFirstRowTimeInCockroachdb
//  }
//
//  def getMasterFirstRowTimeInCockroachdb(): String = {
//    masterFirstRowTimeInCockroachdb
//  }
//
//  def setDetailFirstRowTimeInCockroachdb(detailFirstRowTimeInCockroachdb: String): Unit = {
//    this.detailFirstRowTimeInCockroachdb = detailFirstRowTimeInCockroachdb
//  }
//
//  def getDetailFirstRowTimeInCockroachdb(): String = {
//    detailFirstRowTimeInCockroachdb
//  }
//
//  def setTestFirstRowTimeInCockroachdb(testFirstRowTimeInCockroachdb: String): Unit = {
//    this.testFirstRowTimeInCockroachdb = testFirstRowTimeInCockroachdb
//  }
//
//  def getTestFirstRowTimeInCockroachdb(): String = {
//    testFirstRowTimeInCockroachdb
//  }
//
//  def setMasterLastRowTimeInCockroachdb(masterLastRowTimeInCockroachdb: String): Unit = {
//    this.masterLastRowTimeInCockroachdb = masterLastRowTimeInCockroachdb
//  }
//
//  def getMasterLastRowTimeInCockroachdb(): String = {
//    masterLastRowTimeInCockroachdb
//  }
//
//  def setDetailLastRowTimeInCockroachdb(detailLastRowTimeInCockroachdb: String): Unit = {
//    this.detailLastRowTimeInCockroachdb = detailLastRowTimeInCockroachdb
//  }
//
//  def getDetailLastRowTimeInCockroachdb(): String = {
//    detailLastRowTimeInCockroachdb
//  }
//
//  def setTestLastRowTimeInCockroachdb(testLastRowTimeInCockroachdb: String): Unit = {
//    this.testLastRowTimeInCockroachdb = testLastRowTimeInCockroachdb
//  }
//
//  def getTestLastRowTimeInCockroachdb(): String = {
//    testLastRowTimeInCockroachdb
//  }
//
//  def setMasterTotalRowsInCockroachdb(masterTotalRowsInCockroachdb: Long): Unit = {
//    this.masterTotalRowsInCockroachdb = masterTotalRowsInCockroachdb
//  }
//
//  def setDetailTotalRowsInCockroachdb(detailTotalRowsInCockroachdb: Long): Unit = {
//    this.detailTotalRowsInCockroachdb = detailTotalRowsInCockroachdb
//  }
//
//  def setTestTotalRowsInCockroachdb(testTotalRowsInCockroachdb: Long): Unit = {
//    this.testTotalRowsInCockroachdb = testTotalRowsInCockroachdb
//  }
//
//  def setMasterTotalRowsDistInCockroachdb(masterTotalRowsDistInCockroachdb: Long): Unit = {
//    this.masterTotalRowsDistInCockroachdb = masterTotalRowsDistInCockroachdb
//  }
//
//  def setDetailTotalRowsDistInCockroachdb(detailTotalRowsDistInCockroachdb: Long): Unit = {
//    this.detailTotalRowsDistInCockroachdb = detailTotalRowsDistInCockroachdb
//  }
//
//  def setTestTotalRowsDistInCockroachdb(testTotalRowsDistInCockroachdb: Long): Unit = {
//    this.testTotalRowsDistInCockroachdb = testTotalRowsDistInCockroachdb
//  }
//
//  def setMasterTotalRowsValidateInCockroachdb(masterTotalRowsValidateInCockroachdb: Long): Unit = {
//    this.masterTotalRowsValidateInCockroachdb = masterTotalRowsValidateInCockroachdb
//  }
//
//  def setDetailTotalRowsValidateInCockroachdb(detailTotalRowsValidateInCockroachdb: Long): Unit = {
//    this.detailTotalRowsValidateInCockroachdb = detailTotalRowsValidateInCockroachdb
//  }
//
//  def setTestTotalRowsValidateInCockroachdb(testTotalRowsValidateInCockroachdb: Long): Unit = {
//    this.testTotalRowsValidateInCockroachdb = testTotalRowsValidateInCockroachdb
//  }
//
//  def setMasterFirstRowTimeInMysqldb(riskAssemblyStationFirstRowTimeInMysqldb: String): Unit = {
//    this.riskAssemblyStationFirstRowTimeInMysqldb = riskAssemblyStationFirstRowTimeInMysqldb
//  }
//
//  def getMasterFirstRowTimeInMysqldb(): String = {
//    riskAssemblyStationFirstRowTimeInMysqldb
//  }
//
//  def setDetailFirstRowTimeInMysqldb(riskPartFirstRowTimeInMysqldb: String): Unit = {
//    this.riskPartFirstRowTimeInMysqldb = riskPartFirstRowTimeInMysqldb
//  }
//
//  def getDetailFirstRowTimeInMysqldb(): String = {
//    riskPartFirstRowTimeInMysqldb
//  }
//
//  def setTestFirstRowTimeInMysqldb(riskTestStationFirstRowTimeInMysqldb: String): Unit = {
//    this.riskTestStationFirstRowTimeInMysqldb = riskTestStationFirstRowTimeInMysqldb
//  }
//
//  def getTestFirstRowTimeInMysqldb(): String = {
//    riskTestStationFirstRowTimeInMysqldb
//  }
//
//  def setMasterLastRowTimeInMysqldb(riskAssemblyStationLastRowTimeInMysqldb: String): Unit = {
//    this.riskAssemblyStationLastRowTimeInMysqldb = riskAssemblyStationLastRowTimeInMysqldb
//  }
//
//  def getMasterLastRowTimeInMysqldb(): String = {
//    riskAssemblyStationLastRowTimeInMysqldb
//  }
//
//  def setDetailLastRowTimeInMysqldb(riskPartLastRowTimeInMysqldb: String): Unit = {
//    this.riskPartLastRowTimeInMysqldb = riskPartLastRowTimeInMysqldb
//  }
//
//  def getDetailLastRowTimeInMysqldb(): String = {
//    riskPartLastRowTimeInMysqldb
//  }
//
//  def setTestLastRowTimeInMysqldb(riskTestStationLastRowTimeInMysqldb: String): Unit = {
//    this.riskTestStationLastRowTimeInMysqldb = riskTestStationLastRowTimeInMysqldb
//  }
//
//  def getTestLastRowTimeInMysqldb(): String = {
//    riskTestStationLastRowTimeInMysqldb
//  }
//
//  def setMasterTotalRowsInMysqldb(riskAssemblyStationTotalRowsInMysqldb: Long): Unit = {
//    this.riskAssemblyStationTotalRowsInMysqldb = riskAssemblyStationTotalRowsInMysqldb
//  }
//
//  def getMasterTotalRowsInMysqldb(): Long = {
//    riskAssemblyStationTotalRowsInMysqldb
//  }
//
//  def setDetailTotalRowsInMysqldb(riskPartTotalRowsInMysqldb: Long): Unit = {
//    this.riskPartTotalRowsInMysqldb = riskPartTotalRowsInMysqldb
//  }
//
//  def getDetailTotalRowsInMysqldb(): Long = {
//    riskPartTotalRowsInMysqldb
//  }
//
//  def setTestTotalRowsInMysqldb(riskTestStationTotalRowsInMysqldb: Long): Unit = {
//    this.riskTestStationTotalRowsInMysqldb = riskTestStationTotalRowsInMysqldb
//  }
//
//  def getTestTotalRowsInMysqldb(): Long = {
//    riskTestStationTotalRowsInMysqldb
//  }
//
//  def setJobStartTime(jobStartTime: String): Unit = {
//    this.jobStartTime = jobStartTime
//  }
//
//  def setJobEndTime(jobEndTime: String): Unit = {
//    this.jobEndTime = jobEndTime
//  }
//
//  def setJobId(jobId: String): Unit = {
//    this.jobId = jobId
//  }

  def save(configContext: ConfigContext): Unit = {

    summaryFileJobFmt = configContext.summaryFileJobFmt

    //get job
    this.id = configContext.job.jobId
    this.startTime = ((new SimpleDateFormat(configContext.summaryFileJobFmt)).format(configContext.job.jobStartTime))
    this.endTime = ((new SimpleDateFormat(configContext.summaryFileJobFmt)).format(configContext.job.jobEndTime))
    configContext.isJobState match {
      case true =>
        this.status = KEConstants.SUMMARYFILE_SUCCEEDED
      case false =>
        this.status = KEConstants.SUMMARYFILE_FAILED
      case _ => new IllegalArgumentException
    }
    this.message = ""

    //firstRowTimeInMysqldb - riskAssemblyStationFirstRowTimeInMysqldb
    val riskAssemblyStationFirstRowTimeSql = "SELECT start_time FROM " + configContext.mysqlRiskAssemblyStationSnTable + " where ke_flag = '" + configContext.flag + "' order by start_time asc limit 1"
    val riskAssemblyStationFirstRowTimeStartTimeDf = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession, riskAssemblyStationFirstRowTimeSql, 1)
    if(riskAssemblyStationFirstRowTimeStartTimeDf.count() > 0) {
      this.riskAssemblyStationFirstRowTimeInMysqldb = getStartTime(riskAssemblyStationFirstRowTimeStartTimeDf)
    }

    //firstRowTimeInMysqldb - riskPartFirstRowTimeInMysqldb
    val riskPartFirstRowTimeSql = "SELECT start_time FROM " + configContext.mysqlRiskPartSnTable + " where ke_flag = '" + configContext.flag + "' order by start_time asc limit 1"
    val riskPartFirstRowTimeStartTimeDf = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession, riskPartFirstRowTimeSql, 1)
    if(riskPartFirstRowTimeStartTimeDf.count() > 0) {
      this.riskPartFirstRowTimeInMysqldb  = getStartTime(riskPartFirstRowTimeStartTimeDf)
    }

    //firstRowTimeInMysqldb - riskTestStationFirstRowTimeInMysqldb
    val riskTestStationFirstRowTimeSql = "SELECT start_time FROM " + configContext.mysqlRiskTestStationSnTable + " where ke_flag = '" + configContext.flag + "' order by start_time asc limit 1"
    val riskTestStationFirstRowTimeStartTimeDf = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession, riskTestStationFirstRowTimeSql, 1)
    if(riskTestStationFirstRowTimeStartTimeDf.count() > 0) {
      this.riskTestStationFirstRowTimeInMysqldb = getStartTime(riskTestStationFirstRowTimeStartTimeDf)
    }

    //lastRowTimeInMysqldb - riskAssemblyStationLastRowTimeInMysqldb
    val riskAssemblyStationLastRowTimeSql = "SELECT start_time FROM " + configContext.mysqlRiskAssemblyStationSnTable + " where ke_flag = '" + configContext.flag + "' order by start_time desc limit 1"
    val riskAssemblyStationLastRowTimeStartTimeDf = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession, riskAssemblyStationLastRowTimeSql, 1)
    if(riskAssemblyStationLastRowTimeStartTimeDf.count() > 0) {
      this.riskAssemblyStationLastRowTimeInMysqldb = getStartTime(riskAssemblyStationLastRowTimeStartTimeDf)
    }

    //lastRowTimeInMysqldb - riskPartLastRowTimeInMysqldb
    val riskPartLastRowTimeSql = "SELECT start_time FROM " + configContext.mysqlRiskPartSnTable + " where ke_flag = '" + configContext.flag + "' order by start_time desc limit 1"
    val riskPartLastRowTimeStartTimeDf = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession, riskPartLastRowTimeSql, 1)
    if(riskPartLastRowTimeStartTimeDf.count() > 0) {
      this.riskPartLastRowTimeInMysqldb  = getStartTime(riskPartLastRowTimeStartTimeDf)
    }

    //lastRowTimeInMysqldb - riskTestStationLastRowTimeInMysqldb
    val riskTestStationLastRowTimeSql = "SELECT start_time FROM " + configContext.mysqlRiskTestStationSnTable + " where ke_flag = '" + configContext.flag + "' order by start_time desc limit 1"
    val riskTestStationLastRowTimeStartTimeDf = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession, riskTestStationLastRowTimeSql, 1)
    if(riskTestStationLastRowTimeStartTimeDf.count() > 0) {
      this.riskTestStationLastRowTimeInMysqldb = getStartTime(riskTestStationLastRowTimeStartTimeDf)
    }

    //totalRowsInMysqldb - riskAssemblyStationTotalRowsInMysqldb
    val riskAssemblyStationTotalRowsSql = "SELECT count(*) as cnt FROM " + configContext.mysqlRiskAssemblyStationSnTable + " where ke_flag = '" + configContext.flag + "'"
    val riskAssemblyStationTotalRowsDf = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession, riskAssemblyStationTotalRowsSql, 1)
    if(riskAssemblyStationTotalRowsDf.count() > 0) {
      this.riskAssemblyStationTotalRowsInMysqldb = getTotalRows(riskAssemblyStationTotalRowsDf)
    }

    //totalRowsInMysqldb - riskPartTotalRowsInMysqldb
    val riskPartTotalRowsSql = "SELECT count(*) as cnt FROM " + configContext.mysqlRiskPartSnTable + " where ke_flag = '" + configContext.flag + "'"
    val riskPartTotalRowsDf = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession, riskPartTotalRowsSql, 1)
    if(riskPartTotalRowsDf.count() > 0) {
      this.riskPartTotalRowsInMysqldb = getTotalRows(riskPartTotalRowsDf)
    }

    //totalRowsInMysqldb - riskTestStationTotalRowsInMysqldb
    val riskTestStationTotalRowsSql = "SELECT count(*) as cnt FROM " + configContext.mysqlRiskTestStationSnTable + " where ke_flag = '" + configContext.flag + "'"
    val riskTestStationTotalRowsDf = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession, riskTestStationTotalRowsSql, 1)
    if(riskTestStationTotalRowsDf.count() > 0) {
      this.riskTestStationTotalRowsInMysqldb = getTotalRows(riskTestStationTotalRowsDf)
    }

    //empty data
    handleSummaryFileEmpty()
    val minioIo = new MinioIo(configContext)
    minioIo.saveSummaryFileToMinio(configContext.sparkSession, getJsonString())
  }

  def getJsonString(): String = {
    val inputFileNames = new InputFileNames(masterFilesNameList, detailFilesNameList, testFilesNameList)
    val filesTotalRows = new FilesTotalRows(masterFilesTotalRows, detailFilesTotalRows, testFilesTotalRows)
    val filesTotalRowsDist = FilesTotalRowsDist(masterFilesTotalRowsDist, detailFilesTotalRowsDist, testFilesTotalRowsDist)
    val firstRowTimeInCockroachdb = new FirstRowTimeInCockroachdb(masterFirstRowTimeInCockroachdb, detailFirstRowTimeInCockroachdb, testFirstRowTimeInCockroachdb)
    val lastRowTimeInCockroachdb = new LastRowTimeInCockroachdb(masterLastRowTimeInCockroachdb, detailLastRowTimeInCockroachdb, testLastRowTimeInCockroachdb)
    val totalRowsInCockroachdb = new TotalRowsInCockroachdb(masterTotalRowsInCockroachdb, detailTotalRowsInCockroachdb, testTotalRowsInCockroachdb)
    val totalRowsDistInCockroachdb = new TotalRowsDistInCockroachdb(masterTotalRowsDistInCockroachdb, detailTotalRowsDistInCockroachdb, testTotalRowsDistInCockroachdb)
    val totalRowsValidateInCockroachdb = new TotalRowsValidateInCockroachdb(masterTotalRowsValidateInCockroachdb, detailTotalRowsValidateInCockroachdb, testTotalRowsValidateInCockroachdb)
    val firstRowTimeInMysqldb = new FirstRowTimeInMysqldb(riskAssemblyStationFirstRowTimeInMysqldb, riskPartFirstRowTimeInMysqldb, riskTestStationFirstRowTimeInMysqldb)
    val lastRowTimeInMysqldb = new LastRowTimeInMysqldb(riskAssemblyStationLastRowTimeInMysqldb, riskPartLastRowTimeInMysqldb, riskTestStationLastRowTimeInMysqldb)
    val totalRowsInMysqldb = new TotalRowsInMysqldb(riskAssemblyStationTotalRowsInMysqldb, riskPartTotalRowsInMysqldb, riskTestStationTotalRowsInMysqldb)
    val job = new Job(id, startTime, endTime, status, message)
    val summaryJson = new SummaryJson(inputFileNames,
                                      filesTotalRows,
                                      filesTotalRowsDist,
                                      firstRowTimeInCockroachdb,
                                      lastRowTimeInCockroachdb,
                                      totalRowsInCockroachdb,
                                      totalRowsDistInCockroachdb,
                                      totalRowsValidateInCockroachdb,
                                      firstRowTimeInMysqldb,
                                      lastRowTimeInMysqldb,
                                      totalRowsInMysqldb,
                                      job)
    val summaryJsonList = new util.ArrayList[SummaryJson]()
    summaryJsonList.add(summaryJson)
    val keSummaryJson = new KeSummaryJson("rca-ke", summaryJsonList)
    val gson = new Gson
    val jsonString = gson.toJson(keSummaryJson)
    jsonString
  }

  def handleSummaryFileEmpty(): Unit = {
    if(riskAssemblyStationFirstRowTimeInMysqldb == null) {
      riskAssemblyStationFirstRowTimeInMysqldb = "N/A"
    }
    if(riskPartFirstRowTimeInMysqldb == null) {
      riskPartFirstRowTimeInMysqldb = "N/A"
    }
    if(riskTestStationFirstRowTimeInMysqldb == null) {
      riskTestStationFirstRowTimeInMysqldb = "N/A"
    }
    if(riskAssemblyStationLastRowTimeInMysqldb == null) {
      riskAssemblyStationLastRowTimeInMysqldb = "N/A"
    }
    if(riskPartLastRowTimeInMysqldb == null) {
      riskPartLastRowTimeInMysqldb = "N/A"
    }
    if(riskTestStationLastRowTimeInMysqldb == null) {
      riskTestStationLastRowTimeInMysqldb = "N/A"
    }
  }

  def getStartTime(df: DataFrame): String = {
    val startTimeList = df
      .select("start_time")
      .limit(1)
      .rdd
      .map(r => r(0))
      .collect
      .toList
      .asInstanceOf[List[Timestamp]]

    (new SimpleDateFormat(summaryFileJobFmt)).format(startTimeList(0))
  }

  def getTotalRows(df: DataFrame): Long = {
    val totalRowsList = df
      .select("cnt")
      .limit(1)
      .rdd
      .map(r => r(0))
      .collect
      .toList
      .asInstanceOf[List[Long]]

    totalRowsList(0)
  }
}
