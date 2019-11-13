package com.foxconn.iisd.bd.rca

import java.io.FileNotFoundException
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import com.foxconn.iisd.bd.rca.RecordProcessingTime.{setStartPoint, showRuntime}
import com.foxconn.iisd.bd.rca.SparkUDF.{parseArrayToString, parseStringToJSONString}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel
import com.foxconn.iisd.bd.rca.SparkUDF._
import org.apache.hadoop.fs.{FileSystem, Path}


object XWJKernelEngine {
  // create configLoader object
  var configLoader = new ConfigLoader()
  // create job object
  val job = new Job()
  // create configContext object
  val configContext = new ConfigContext()

  def main(args: Array[String]): Unit = {
    println("xwj-kernel-engine-v4")

    //關閉不必要的Log資訊
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Job開始
    println("======> Kernel Engine Job Start")
    job.jobStartTime = new java.util.Date()

    ////////////////////////////////////////////////////////////////////////////////////////
    //初始化作業
    ////////////////////////////////////////////////////////////////////////////////////////
    initialize(args)
    job.setJobId(configContext)
    val jobStartTime: String = new SimpleDateFormat(configContext.jobDateFmt).format(job.jobStartTime.getTime())
    println("job start time : " + jobStartTime)

    ////////////////////////////////////////////////////////////////////////////////////////
    //core engine處理
    ////////////////////////////////////////////////////////////////////////////////////////

    //(1)處理測試細表
    setStartPoint(configContext)
    val testDetailDataProcessing = new TestDetailDataProcessing(configContext)
    testDetailDataProcessing.calculation()
    showRuntime(configContext)

    //(2)處理工單數據
    setStartPoint(configContext)
    val woDataProcessing = new WoDataProcessing(configContext)
    woDataProcessing.calculation()
    showRuntime(configContext)

    //(3)處理關鍵物料
    setStartPoint(configContext)
    val matDataProcessing = new MatDataProcessing(configContext)
    matDataProcessing.calculation()
    showRuntime(configContext)


    ////////////////////////////////////////////////////////////////////////////////////////
    //資料驗證
    ////////////////////////////////////////////////////////////////////////////////////////
    setStartPoint(configContext)
    if(ValidationData.validateEmptyData()) {
      println(XWJKEConstants.JOB_EMPTY_DATA)
    }
    ValidationData.validateCockroachDB(configContext)
    showRuntime(configContext)

    //Job結束
    job.jobEndTime = new java.util.Date()
    println("======> Kernel Engine Job End")
    configContext.isJobState = true
    val jobEndTime: String = new SimpleDateFormat(configContext.jobDateFmt).format(job.jobEndTime.getTime())
    println("job end time : " + jobEndTime)

    ////////////////////////////////////////////////////////////////////////////////////////
    //將TMP檔案依照Job State搬到Succeeded or Failed
    ////////////////////////////////////////////////////////////////////////////////////////
    val minioIo = new MinioIo(configContext)
//    minioIo.moveFilesByJobStatus()

    ////////////////////////////////////////////////////////////////////////////////////////
    //SummaryFile輸出
    ////////////////////////////////////////////////////////////////////////////////////////
    SummaryFile.save(configContext)

  }

  /*
   *
   *
   * @author JasonLai
   * @date 2019/9/27 下午4:50
   * @param [args]
   * @return void
   * @description 初始化 configloader, spark, configContext
   */
  def initialize(args: Array[String]): Unit = {

    //load config yaml
    configLoader.setDefaultConfigPath("""conf/default.yaml""")
    if(args.length == 1) {
      configLoader.setDefaultConfigPath(args(0))
    }

    //env
    configContext.env = configLoader.getString("general", "env")

    //spark
    configContext.sparkJobName = configLoader.getString("spark", "job_name")
    configContext.sparkMaster = configLoader.getString("spark", "master")

    //create spark session object
    val sparkBuilder = SparkSession
      .builder
      .appName(configContext.sparkJobName)

    if(configContext.env.equals(XWJKEConstants.ENV_LOCAL)) {
      sparkBuilder.master(configContext.sparkMaster)
    }

    val spark = sparkBuilder.getOrCreate()

    //spark executor add config file
    configLoader.setConfig2SparkAddFile(spark)

    //executor instances number
    configContext.sparkNumExcutors = spark.conf.get("spark.executor.instances", "1").toInt

    //add sparkSession into configContext
    configContext.sparkSession = spark

    //初始化context
    initializeconfigContext()

    println(s"======> Job Start Time : ${configContext.job.jobStartTime}")
    println(s"======> Job Flag : ${configContext.flag}")
  }

  /*
   *
   *
   * @author EchoLee
   * @date 2019/9/19 上午9:37
   * @description configContext初始化基本設定值
   */
  def initializeconfigContext(): Unit = {
    //job
    configContext.job = job
    configContext.jobDateFmt = configLoader.getString("summary_log_path", "job_fmt")
    //flag
    configContext.flag = job.jobStartTime.getTime.toString
    //file
    configContext.testDetailPath = configLoader.getString("minio_log_path", "test_detail_path")
    configContext.woPath = configLoader.getString("minio_log_path", "wo_path")
    configContext.matPath = configLoader.getString("minio_log_path", "mat_path")
    configContext.testDetailColumns = configLoader.getString("log_prop", "test_detail_col")
    configContext.woColumns = configLoader.getString("log_prop", "wo_col")
    configContext.matColumns = configLoader.getString("log_prop", "mat_col")
    configContext.dataSeperator = configLoader.getString("log_prop", "log_seperator")
    configContext.mbLimits = configLoader.getString("log_prop", "mb_limits").toInt
    configContext.testDetailDtFmt = configLoader.getString("log_prop", "test_detail_dt_fmt")
    configContext.woDtFmt = configLoader.getString("log_prop", "wo_dt_fmt")
    configContext.matDtFmt = configLoader.getString("log_prop", "mat_dt_fmt")

    //minio
    configContext.minioEndpoint = configLoader.getString("minio", "endpoint")
    configContext.minioConnectionSslEnabled = configLoader.getString("minio", "connectionSslEnabled").toBoolean
    configContext.minioAccessKey = configLoader.getString("minio", "accessKey")
    configContext.minioSecretKey = configLoader.getString("minio", "secretKey")
    //to summaryfile use
    configContext.minioBucket = configLoader.getString("minio", "bucket")
    //cockroach db
    configContext.cockroachDbConnUrlStr = configLoader.getString("cockroachdb", "conn_str")
    configContext.cockroachDbSslMode = configLoader.getString("cockroachdb", "sslmode")
    configContext.cockroachDbUserName = configLoader.getString("cockroachdb", "username")
    configContext.cockroachDbPassword = configLoader.getString("cockroachdb", "password")
    configContext.cockroachDbMasterTable = configLoader.getString("log_prop", "wip_table")
    configContext.cockroachDbDetailTable = configLoader.getString("log_prop", "wip_parts_table")
    configContext.cockroachDbTestTable = configLoader.getString("log_prop", "bobcat_table")
    configContext.cockroachDbTestDetailTable = configLoader.getString("log_prop", "test_detail_table")
    configContext.cockroachDbWoTable = configLoader.getString("log_prop", "wo_table")
    configContext.cockroachDbMatTable = configLoader.getString("log_prop", "mat_table")
    configContext.cockroachDbMasterColumns = configLoader.getString("log_prop", "wip_filter_col")
    configContext.cockroachDbDetailColumns = configLoader.getString("log_prop", "wip_parts_line_col")

    //mysql db
    configContext.mysqlDbConnUrlStr = configLoader.getString("mariadb", "conn_str")
    configContext.mysqlDatabase = configLoader.getString("mariadb", "database")
    configContext.mysqlDbUserName = configLoader.getString("mariadb", "username")
    configContext.mysqlDbPassword = configLoader.getString("mariadb", "password")
    configContext.mysqlProductItemSpecTable = configLoader.getString("mariadb", "product_item_spec_table")
    configContext.mysqlProductStationTable = configLoader.getString("mariadb", "product_station_table")
    configContext.mysqlProductMatTable = configLoader.getString("mariadb", "config_component_table")
    configContext.mysqlProductBigtableDatatypeTable = configLoader.getString("mariadb", "bigtable_datatype_table")
    //summary file
    configContext.summaryFileLogBasePath = configLoader.getString("summary_log_path", "data_base_path")
    configContext.summaryFileLogTag = configLoader.getString("summary_log_path", "tag")
    configContext.summaryFileExtension = configLoader.getString("summary_log_path", "file_extension")
    configContext.summaryFileJobFmt = configLoader.getString("summary_log_path", "job_fmt")
    configContext.summaryFileBuName = configLoader.getString("summary_log_path", "bu_name")

    //object
    val fileDataSource = new FileSource(configContext)
    fileDataSource.init()
    configContext.fileDataSource = fileDataSource

    val cockroachDBIo = new CockroachDBIo(configContext)
    configContext.cockroachDBIo = cockroachDBIo

    val mysqlDBIo = new MysqlDBIo(configContext)
    configContext.mysqlDBIo = mysqlDBIo

  }

}
