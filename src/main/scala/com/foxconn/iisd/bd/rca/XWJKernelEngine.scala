package com.foxconn.iisd.bd.rca

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.foxconn.iisd.bd.rca.KernelEngineBackup.configLoader
import com.foxconn.iisd.bd.rca.SparkUDF.{parseArrayToString, parseStringToJSONString}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel
import com.foxconn.iisd.bd.rca.SparkUDF._

object XWJKernelEngine {

  var configLoader = new ConfigLoader()
  val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.TAIWAN)

  def main(args: Array[String]): Unit = {


    val limit = 1
    var count = 0

    println("xwj-bigtable-v1:")

    while (count < limit) {
      println(s"count: $count")

      try {
        configLoader.setDefaultConfigPath("""conf/default.yaml""")
        if (args.length == 1) {
          configLoader.setDefaultConfigPath(args(0))
        }
        XWJKernelEngine.start()
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      }

      count = count + 1

      Thread.sleep(5000)
    }

  }

  def start(): Unit = {

    var date: java.util.Date = new java.util.Date()
    val flag = date.getTime().toString

    val jobStartTime: String = new SimpleDateFormat(
        configLoader.getString("summary_log_path","job_fmt")).format(date.getTime())
    println("job start time : " + jobStartTime)
//    Summary.setJobStartTime(jobStartTime)

    println(s"flag: $flag" + ": xwj")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkBuilder = SparkSession
      .builder
      .appName(configLoader.getString("spark", "job_name"))
      .master(configLoader.getString("spark", "master"))

    val confStr = configLoader.getString("spark", "conf")

    val confAry = confStr.split(";").map(_.trim)
    for (i <- 0 until confAry.length) {
      val configKeyValue = confAry(i).split("=").map(_.trim)
      println("conf ===> " + configKeyValue(0) + " : " + configKeyValue(1))
      sparkBuilder.config(configKeyValue(0), configKeyValue(1))
    }

    val spark = sparkBuilder.getOrCreate()

    val configMap = spark.conf.getAll
    for ((k, v) <- configMap) {
      println("[" + k + " = " + v + "]")
    }

    configLoader.setConfig2SparkAddFile(spark)

    var logPathSection = "local_log_path"
    val isFromMinio = configLoader.getString("general", "from_minio").toBoolean
    println("isFromMinio : " + isFromMinio)

    if (isFromMinio) {
      logPathSection = "minio_log_path"

      val endpoint = configLoader.getString("minio", "endpoint")
      val accessKey = configLoader.getString("minio", "accessKey")
      val secretKey = configLoader.getString("minio", "secretKey")
      val bucket = configLoader.getString("minio", "bucket")

      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    }
    import spark.implicits._
    val numExecutors = spark.conf.get("spark.executor.instances", "1").toInt

    //val factory = configLoader.getString("general", "factory")

    //val failCondition: Int = configLoader.getString("analysis", "fail_condition").toInt

    //s3a://" + bucket + "/
    val testDetailPath = configLoader.getString(logPathSection, "test_detail_path")

    val testDetailFileLmits = configLoader.getString(logPathSection, "test_detail_file_limits").toInt

    val testDetailColumns = configLoader.getString("log_prop", "test_detail_col")

    val testDetailTestColumns = configLoader.getString("log_prop", "test_detail_test_cols")

    val ctrlACode = "\001"
    val ctrlAValue = "^A"

    val ctrlCCode = "\003"

    val ctrlDCode = "\004"
    val ctrlDValue = "^D"


    val dataSeperator = configLoader.getString("log_prop", "log_seperator")

    ///////////
    //載入資料//
    ///////////

    try {

      //(1)測試結果表
      val testDetailDestPath = IoUtils.flatMinioFiles(spark,
        flag,
        testDetailPath,
        testDetailFileLmits)

      val testDetailSourceDf = IoUtils.getDfFromPath(spark, testDetailDestPath.toString, testDetailColumns, dataSeperator)


      var testDetailTempDf = testDetailSourceDf.distinct()
        .withColumn("test_item", split(trim($"test_item"), ctrlACode))
        .withColumn("test_value", split(trim($"test_value"), ctrlACode))
        .withColumn("test_upper", split(trim($"test_upper"), ctrlACode))
        .withColumn("test_lower", split(trim($"test_lower"), ctrlACode))
        .withColumn("test_unit", split(trim($"test_unit"), ctrlACode))
//        新增兩個欄位test_item_result, test_item_result_detail
        .withColumn("test_item_result", split(trim($"test_item_result"), ctrlACode))
        .withColumn("test_item_result_detail", split(trim($"test_item_result_detail"), ctrlACode))

        .withColumn("list_of_failure", regexp_replace($"list_of_failure", ctrlACode, ctrlAValue))
        .withColumn("list_of_failure_detail", regexp_replace($"list_of_failure_detail", ctrlACode, ctrlAValue))
        .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

      val testDetailSourceDfDistCnt = testDetailSourceDf.count()

      def  testDetailDateStringToTimestamp (colName:String, configkey: String, configValue: String): Column  = {
        unix_timestamp(trim(col(colName)),
          configLoader.getString(configkey, configValue)).cast(TimestampType)

      }

      //TODO: summary file
//      Summary.setMasterFilesNameList(IoUtils.getFilesNameList(spark, testDetailDestPath))
//                        testDetailSourceDf.select("test_upper").show(false)
//                        testDetailSourceDf.select("test_lower").show(false)
//                        testDetailSourceDf.select("test_unit").show(false)
//                  testDetailSourceDf.printSchema()

      val testDetailCockroachDf = testDetailTempDf
        .withColumn("test_starttime",
          testDetailDateStringToTimestamp("test_starttime", "log_prop", "test_detail_dt_fmt"))
        .withColumn("test_endtime",
          testDetailDateStringToTimestamp("test_endtime", "log_prop", "test_detail_dt_fmt"))
        .withColumn("create_time",
          testDetailDateStringToTimestamp("create_time", "log_prop", "test_detail_dt_fmt"))
        .withColumn("start_date",
          testDetailDateStringToTimestamp("start_date", "log_prop", "test_detail_dt_fmt"))
        .withColumn("update_time",
          testDetailDateStringToTimestamp("update_time", "log_prop", "test_detail_dt_fmt"))
        .withColumn("test_item", parseArrayToString($"test_item"))
        .withColumn("test_item", concat(lit("ARRAY["), $"test_item", lit("]")))
        .withColumn("test_value", parseStringToJSONString($"test_value"))
        .withColumn("test_upper", parseStringToJSONString($"test_upper"))
        .withColumn("test_lower", parseStringToJSONString($"test_lower"))
        .withColumn("test_unit", parseStringToJSONString($"test_unit"))
        .withColumn("test_item_result", parseStringToJSONString($"test_item_result"))
        .withColumn("test_item_result_detail", parseStringToJSONString($"test_item_result_detail"))
        //存入upsert time
        .withColumn("upsert_time", lit(jobStartTime).cast(TimestampType))

      //將測試結果表資料儲存進Cockroachdb
      println("saveToCockroachdb --> testDetailCockroachDf")
      IoUtils.saveToCockroachdb(testDetailCockroachDf,
        configLoader.getString("log_prop", "test_detail_table"),
        numExecutors)

      //insert product station to mysql
      //將測項上下界撈出來之後, 根據測試版號與時間選最新
      testDetailTempDf = testDetailTempDf
        .withColumn("temp", arrays_zip($"test_item", $"test_upper", $"test_lower", $"test_unit", $"test_value"))
        .withColumn("temp", explode($"temp"))
        .selectExpr("product", "station_name", "temp.test_item as test_item", "temp.test_upper as test_upper",
          "temp.test_lower as test_lower", "temp.test_unit as test_unit", "temp.test_value as test_value",
          "test_version", "test_starttime")
        .withColumn("test_item", regexp_replace($"test_item", ctrlDCode, ctrlDValue))
        .withColumn("test_upper", split(split(col("test_upper"), ctrlDCode).getItem(1), ctrlCCode).getItem(1))
        .withColumn("test_lower", split(split(col("test_lower"), ctrlDCode).getItem(1), ctrlCCode).getItem(1))
        .withColumn("test_unit", split(split(col("test_unit"), ctrlDCode).getItem(1), ctrlCCode).getItem(1))
        .withColumn("test_value", split(split(col("test_value"), ctrlDCode).getItem(1), ctrlCCode).getItem(1))
        .withColumn("test_item_datatype", castColumnDataType(col("test_value")))
        .drop("test_value")
      testDetailTempDf.show(false)

      val productList = testDetailTempDf.select("product").dropDuplicates().as(Encoders.STRING).collect()

      val mariadbUtils = new MariadbUtils()

      //insert product item spec
      val itemSpecColumnStr = "product,station_name,test_item,test_upper,test_lower,test_unit,test_version,test_starttime,test_item_datatype"
//      var itemSpecColumn = List("product", "station_name", "test_item",
//        "test_upper", "test_lower", "test_unit", "test_version", "test_starttime", "test_item_datatype")
      val itemSpecColumn = itemSpecColumnStr.split(",")
      val productItemSpecDf = mariadbUtils
        .getDfFromMariadb(spark, "product_item_spec")
//        .select(itemSpecColumn.head, itemSpecColumn.tail:_*)
        .selectExpr(itemSpecColumn: _*)
        .where(col("product").isin(productList:_*))

      testDetailTempDf = productItemSpecDf.union(testDetailTempDf)

      val wSpec = Window.partitionBy(col("product"), col("station_name"),
        col("test_item"))
        .orderBy(desc("test_version"), desc("test_starttime"))

      testDetailTempDf = testDetailTempDf
        .withColumn("rank", rank().over(wSpec))
        .where($"rank".equalTo(1)).drop("rank")
        .drop("test_value")

      //將資料儲存進Mariadb
      println("saveToMariadb --> testDetailTempDf")

      mariadbUtils.saveToMariadb(
        testDetailTempDf,
        "product_item_spec",
        numExecutors
      )

      //insert product station
      var productStationDf = mariadbUtils
        .getDfFromMariadb(spark, "product_station")
        .select("product", "station_name", "flag", "station_name_user")
        .where(col("product").isin(productList:_*))

      productStationDf = productStationDf.union(
          testDetailSourceDf.select("product", "station_name")
          .dropDuplicates()
          .withColumn("flag", lit(1))
          .withColumn("station_name_user", lit(null)))
      productStationDf = productStationDf.dropDuplicates("product", "station_name")

      println("saveToMariadb --> productStationDf")

      mariadbUtils.saveToMariadb(
        productStationDf,
        "product_station",
        numExecutors
      )

      //(2)工單
      val woPath = configLoader.getString(logPathSection, "wo_path")

      val woFileLmits = configLoader.getString(logPathSection, "wo_file_limits").toInt

      val woColumns = configLoader.getString("log_prop", "wo_col")

      val woDtfmt = configLoader.getString("log_prop", "wo_dt_fmt")


      val woDestPath = IoUtils.flatMinioFiles(spark,
        flag,
        woPath,
        woFileLmits)

      var woSourceDf = IoUtils.getDfFromPath(spark, woDestPath.toString, woColumns, dataSeperator)
      woSourceDf = woSourceDf.drop("prodversion","create_date")
        .withColumn("release_date", unix_timestamp(trim($"release_date"), woDtfmt)
          .cast(TimestampType))
//      woSourceDf = woSourceDf.drop("release_date","prodversion","create_date")
        .withColumn("upsert_time", lit(jobStartTime).cast(TimestampType))
      woSourceDf.show(false)

      //將工單資料儲存進Cockroachdb
      println("saveToCockroachdb --> woSourceDf")
      IoUtils.saveToCockroachdb(woSourceDf,
        configLoader.getString("log_prop", "wo_table"),
        numExecutors)

      //(3)關鍵物料
      val matPath = configLoader.getString(logPathSection, "mat_path")

      val matFileLmits = configLoader.getString(logPathSection, "mat_file_limits").toInt

      val matColumns = configLoader.getString("log_prop", "mat_col")

      val matTable = configLoader.getString("log_prop", "mat_table")
      val matDestPath = IoUtils.flatMinioFiles(spark,
        flag,
        matPath,
        matFileLmits)

      var matSourceDf = IoUtils.getDfFromPath(spark, matDestPath.toString, matColumns, dataSeperator)
      matSourceDf = matSourceDf.withColumn("upsert_time", lit(jobStartTime).cast(TimestampType))

      //將關鍵物料資料儲存進Cockroachdb
      println("saveToCockroachdb --> matSourceDf")
      IoUtils.saveToCockroachdb(matSourceDf, matTable, numExecutors)
      //將關鍵物料資料儲存進mariadb
      println("saveToMariadb --> matSourceDf")
      mariadbUtils.saveToMariadb(matSourceDf.drop("upsert_time"), matTable, numExecutors)


    } catch {
      case ex: FileNotFoundException => {
        // ex.printStackTrace()
        println("===> FileNotFoundException !!!")
      }
    }
  }

}
