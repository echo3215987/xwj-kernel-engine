package com.foxconn.iisd.bd.rca

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import com.google.gson.Gson
import org.apache.spark.sql.DataFrame

object SummaryFile {
  case class InputFileNames(testDetailFilesNameList: util.ArrayList[String], woFilesNameList: util.ArrayList[String], matFilesNameList: util.ArrayList[String])
  case class FilesTotalRows(testDetailFilesTotalRows: Long, woFilesTotalRows: Long, matFilesTotalRows: Long)
  case class FilesTotalRowsDist(testDetailFilesTotalRowsDist: Long, woFilesTotalRowsDist: Long, matFilesTotalRowsDist: Long)
  case class FirstRowTestStartTimeByProductInCockroachdb(testDetailFirstRowTestStartTimeByProductInCockroachdb: util.Map[String, String])
  case class LastRowTestStartTimeByProductInCockroachdb(testDetailLastRowTestStartTimeByProductInCockroachdb: util.Map[String, String])
  case class TotalRowsInCockroachdb(testDetailTotalRowsInCockroachdb: Long, woTotalRowsInCockroachdb: Long, matTotalRowsInCockroachdb: Long)
  case class TotalRowsDistInCockroachdb(testDetailTotalRowsDistInCockroachdb: Long, woTotalRowsDistInCockroachdb: Long, matTotalRowsDistInCockroachdb: Long)
  case class TotalRowsValidateInCockroachdb(testDetailTotalRowsValidateInCockroachdb: Long, woTotalRowsValidateInCockroachdb: Long, matTotalRowsValidateInCockroachdb: Long)
  case class Job(id: String, startTime: String, endTime: String, status: String, message: String)
  case class XWJKeSummaryJson(template_json: String, xwjKE: util.ArrayList[SummaryJson])
  case class SummaryJson(
                          inputFileNames: InputFileNames,
                          filesTotalRows: FilesTotalRows,
                          filesTotalRowsDist: FilesTotalRowsDist,
                          firstRowTestStartTimeByProductInCockroachdb: FirstRowTestStartTimeByProductInCockroachdb,
                          lastRowTestStartTimeByProductInCockroachdb: LastRowTestStartTimeByProductInCockroachdb,
                          totalRowsInCockroachdb: TotalRowsInCockroachdb,
                          totalRowsDistInCockroachdb: TotalRowsDistInCockroachdb,
                          totalRowsValidateInCockroachdb: TotalRowsValidateInCockroachdb,
                          job: Job,
                          bu: String)

  var testDetailFilesRows: util.HashMap[String, Integer] = null
  var woFilesRows: util.HashMap[String, Integer] = null
  var matFilesRows: util.HashMap[String, Integer] = null

  var testDetailFilesNameList: util.ArrayList[String] = null
  var woFilesNameList: util.ArrayList[String] = null
  var matFilesNameList: util.ArrayList[String] = null

  var testDetailFilesTotalRows: Long = 0
  var woFilesTotalRows: Long = 0
  var matFilesTotalRows: Long = 0

  var testDetailFilesTotalRowsDist: Long = 0
  var woFilesTotalRowsDist: Long = 0
  var matFilesTotalRowsDist: Long = 0

  var testDetailFirstRowTimeInCockroachdb: String = null
  var woFirstRowTimeInCockroachdb: String = null
  var matFirstRowTimeInCockroachdb: String = null

  var testDetailLastRowTimeInCockroachdb: String = null
  var woLastRowTimeInCockroachdb: String = null
  var matLastRowTimeInCockroachdb: String = null

  var testDetailTotalRowsInCockroachdb: Long = 0
  var woTotalRowsInCockroachdb: Long = 0
  var matTotalRowsInCockroachdb: Long = 0

  var testDetailTotalRowsDistInCockroachdb: Long = 0
  var woTotalRowsDistInCockroachdb: Long = 0
  var matTotalRowsDistInCockroachdb: Long = 0

  var testDetailTotalRowsValidateInCockroachdb: Long = 0
  var woTotalRowsValidateInCockroachdb: Long = 0
  var matTotalRowsValidateInCockroachdb: Long = 0

  var testDetailFirstRowTestStartTimeByProductInCockroachdb: util.Map[String, String] = null
  var testDetailLastRowTestStartTimeByProductInCockroachdb: util.Map[String, String] = null

  var startTime: String = null
  var endTime: String = null
  var id: String = null
  var status: String = ""
  var message: String = ""

  var summaryFileJobFmt = ""
  var summaryFileBuName = ""


  def save(configContext: ConfigContext): Unit = {

    summaryFileJobFmt = configContext.summaryFileJobFmt
    summaryFileBuName = configContext.summaryFileBuName

    //get job
    this.id = configContext.job.jobId
    this.startTime = ((new SimpleDateFormat(configContext.summaryFileJobFmt)).format(configContext.job.jobStartTime))
    this.endTime = ((new SimpleDateFormat(configContext.summaryFileJobFmt)).format(configContext.job.jobEndTime))
    configContext.isJobState match {
      case true =>
        this.status = XWJKEConstants.SUMMARYFILE_SUCCEEDED
      case false =>
        this.status = XWJKEConstants.SUMMARYFILE_FAILED
      case _ => new IllegalArgumentException
    }
    this.message = ""

    val minioIo = new MinioIo(configContext)
    minioIo.saveSummaryFileToMinio(configContext.sparkSession, getJsonString())
  }

  def getJsonString(): String = {
    val inputFileNames = new InputFileNames(testDetailFilesNameList, woFilesNameList, matFilesNameList)
    val filesTotalRows = new FilesTotalRows(testDetailFilesTotalRows, woFilesTotalRows, matFilesTotalRows)
    val filesTotalRowsDist = FilesTotalRowsDist(testDetailFilesTotalRowsDist, woFilesTotalRowsDist, matFilesTotalRowsDist)
    val firstRowTestStartTimeByProductInCockroachdb = FirstRowTestStartTimeByProductInCockroachdb(testDetailFirstRowTestStartTimeByProductInCockroachdb)
    val lastRowTestStartTimeByProductInCockroachdb = LastRowTestStartTimeByProductInCockroachdb(testDetailLastRowTestStartTimeByProductInCockroachdb)
    val totalRowsInCockroachdb = new TotalRowsInCockroachdb(testDetailTotalRowsInCockroachdb, woTotalRowsInCockroachdb, matTotalRowsInCockroachdb)
    val totalRowsDistInCockroachdb = new TotalRowsDistInCockroachdb(testDetailTotalRowsDistInCockroachdb, woTotalRowsDistInCockroachdb, matTotalRowsDistInCockroachdb)
    val totalRowsValidateInCockroachdb = new TotalRowsValidateInCockroachdb(testDetailTotalRowsValidateInCockroachdb, woTotalRowsValidateInCockroachdb, matTotalRowsValidateInCockroachdb)
    val job = new Job(id, startTime, endTime, status, message)
    val summaryJson = new SummaryJson(inputFileNames,
                                      filesTotalRows,
                                      filesTotalRowsDist,
                                      firstRowTestStartTimeByProductInCockroachdb,
                                      lastRowTestStartTimeByProductInCockroachdb,
                                      totalRowsInCockroachdb,
                                      totalRowsDistInCockroachdb,
                                      totalRowsValidateInCockroachdb,
                                      job,
                                      summaryFileBuName)
    val summaryJsonList = new util.ArrayList[SummaryJson]()
    summaryJsonList.add(summaryJson)
    val xwjKeSummaryJson = new XWJKeSummaryJson("rca-xwj-ke", summaryJsonList)
    val gson = new Gson
    val jsonString = gson.toJson(xwjKeSummaryJson)

    jsonString
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
