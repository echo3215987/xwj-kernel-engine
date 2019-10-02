package com.foxconn.iisd.bd.rca

import java.io.FileNotFoundException
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import com.foxconn.iisd.bd.rca.SparkUDF.{parseArrayToString, parseStringToJSONString}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel
import com.foxconn.iisd.bd.rca.SparkUDF._
import org.apache.hadoop.fs.{FileSystem, Path}

object XWJKernelEngine {

  var configLoader = new ConfigLoader()
  val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.TAIWAN)

  var totalRawDataSize: Long = 0
  val mb = 1024*1024
  val runtime = Runtime.getRuntime
  var jobId = ""
  var jobYear = ""
  var jobMonth = ""
  var jobDay = ""
  var jobHour = ""
  var jobMinute = ""
  var jobSecond = ""
  var jobStatus = false
  var testDetailPath = ""
  var woPath = ""
  var matPath = ""

  val ctrlACode = "\001"
  val ctrlAValue = "^A"

  val ctrlCCode = "\003"

  val ctrlDCode = "\004"
  val ctrlDValue = "^D"

  def main(args: Array[String]): Unit = {


    val limit = 1
    var count = 0

    println("xwj-kernel-engine-v3")

    while(count < limit) {

      println(s"count: $count")
      configLoader.setDefaultConfigPath("""conf/default.yaml""")
      if(args.length == 1) {
        configLoader.setDefaultConfigPath(args(0))
      }

      jobId = getHostName()
      println("job id : " + jobId)

      val sparkBuilder = SparkSession
        .builder
        .appName(configLoader.getString("spark", "job_name"))
        .master(configLoader.getString("spark", "master"))

      val confStr = configLoader.getString("spark", "conf")

      val confAry = confStr.split(";").map(_.trim)
      for(i <- 0 until confAry.length) {
        val configKeyValue = confAry(i).split("=").map(_.trim)
        println("conf ===> " + configKeyValue(0) + " : " + configKeyValue(1))
        sparkBuilder.config(configKeyValue(0), configKeyValue(1))
      }

      val spark = sparkBuilder.getOrCreate()

      val configMap = spark.conf.getAll
      for ((k,v) <- configMap) {
        println("[" + k + " = " + v + "]")
      }

      try {
//        jobId = "rca-ke-dev-uuid-20190830100000-driver"
        jobYear = jobId.split("-uuid-")(1).split("-")(0).slice(0, 4)
        jobMonth = jobId.split("-uuid-")(1).split("-")(0).slice(4, 6)
        jobDay = jobId.split("-uuid-")(1).split("-")(0).slice(6, 8)
        jobHour = jobId.split("-uuid-")(1).split("-")(0).slice(8, 10)
        jobMinute = jobId.split("-uuid-")(1).split("-")(0).slice(10, 12)
        jobSecond = jobId.split("-uuid-")(1).split("-")(0).slice(12, 14)
//        Summary.setJobId(jobId) TODO
        XWJKernelEngine.start(spark)
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      } finally {
        IoUtils.moveFilesByJobStatus(
          spark,
          testDetailPath,
          woPath,
          matPath,
          jobStatus,
          jobId,
          jobYear + jobMonth,
          jobDay,
          jobHour + jobMinute + jobSecond)
      }

      count = count + 1

      Thread.sleep(5000)
    }

  }

  def start(spark: SparkSession): Unit = {

    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb + " MB")
    println("** Free Memory:  " + runtime.freeMemory / mb + " MB")
    println("** Total Memory: " + runtime.totalMemory / mb + " MB")
    println("** Max Memory:   " + runtime.maxMemory / mb + " MB")

    var date: java.util.Date = new java.util.Date()
    val flag = date.getTime().toString

    val jobStartTime: String = new SimpleDateFormat(
        configLoader.getString("summary_log_path","job_fmt")).format(date.getTime())
    println("job start time : " + jobStartTime)
//    Summary.setJobStartTime(jobStartTime)

    println(s"flag: $flag")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

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

    testDetailPath = configLoader.getString(logPathSection, "test_detail_path")

    val testDetailFileLmits = configLoader.getString(logPathSection, "test_detail_file_limits").toInt

    val testDetailColumns = configLoader.getString("log_prop", "test_detail_col")

    val testDetailTestColumns = configLoader.getString("log_prop", "test_detail_test_cols")

    val dataSeperator = configLoader.getString("log_prop", "log_seperator")

    val productItemSpecTable = configLoader.getString("dataset", "product_item_spec_table")

    val masterFilterColumn = configLoader.getString("log_prop", "wip_filter_col")

    val masterTable = configLoader.getString("log_prop", "wip_table")

    val detailLineColumn = configLoader.getString("log_prop", "wip_parts_line_col")

    val detailTable = configLoader.getString("log_prop", "wip_parts_table")

    woPath = configLoader.getString(logPathSection, "wo_path")

    val woFileLmits = configLoader.getString(logPathSection, "wo_file_limits").toInt

    val woColumns = configLoader.getString("log_prop", "wo_col")

    val woDtfmt = configLoader.getString("log_prop", "wo_dt_fmt")

    matPath = configLoader.getString(logPathSection, "mat_path")

    val matFileLmits = configLoader.getString(logPathSection, "mat_file_limits").toInt

    val matColumns = configLoader.getString("log_prop", "mat_col")

    val matTable = configLoader.getString("log_prop", "mat_table")

    val mbLimits = configLoader.getString("log_prop", "mb_limits").toInt

    ///////////
    //載入資料//
    ///////////

    try {

      println(s"MB Limits : $mbLimits")

      val mariadbUtils = new MariadbUtils()

      //(1)測試結果表
      //1-1: 將測試結果表資料儲存進Cockroachdb
//      val testDetailDestPath = IoUtils.flatMinioFiles(spark,
//        flag,
//        testDetailPath,
//        testDetailFileLmits)

      val testDetailDestPath = IoUtils.flatMinioFiles(spark,
        testDetailPath,
        (mbLimits * 1024 * 1024),
        jobYear + jobMonth,
        jobDay,
        jobHour + jobMinute + jobSecond)

//      val testDetailDestPath = new Path("s3a://rca-ftp/Cartridge-Nesta/Data/TEST_DETAIL/2019/08/28/10/00/00/*")

      val testDetailSourceDf = IoUtils.getDfFromPath(spark, testDetailDestPath.toString, testDetailColumns, dataSeperator)

println("testDetailSourceDf from file, Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb + " MB")

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
        .persist(StorageLevel.MEMORY_AND_DISK)

testDetailTempDf.show(3, false)

      def  testDetailDateStringToTimestamp (colName:String, configkey: String, configValue: String): Column  = {
        unix_timestamp(trim(col(colName)),
          configLoader.getString(configkey, configValue)).cast(TimestampType)
      }

      //TODO: summary file
//      Summary.setMasterFilesNameList(IoUtils.getFilesNameList(spark, testDetailDestPath))

      testDetailTempDf = testDetailTempDf
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

      var testDetailCockroachDf = testDetailTempDf
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
println("testDetailCockroachDf count:" + testDetailCockroachDf.count())
      //v2調整: insert value_rank flag and select part_master
      val snList = testDetailCockroachDf.select("sn").dropDuplicates().map(_.getString(0)).collect.toList
      val snSize = snList.size
      println("sn count:" + snList.size)
      if(snSize > 0){
        val snCondition = "sn in (" + snList.map(s => "'" + s + "'").mkString(",") + ")"
        val testDetailWhereStr = "product,station_name,sn"
        val testDetailWhereColumn = testDetailWhereStr.split(",")
        val testDetailSelectColumnStr = "product,station_name,sn,value_rank,test_starttime"


        val productList = testDetailTempDf.select("product").dropDuplicates().as(Encoders.STRING).collect()
        val productCondition = "product in (" + productList.map(s => "'" + s + "'").mkString(",") + ")"


        println("-----------------> select part table (first scantime) where sn, product start_time:" + new SimpleDateFormat(
          configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

        //找出所有sn在組裝主表第一筆scantime的工單號
        val masterSql = "select " + masterFilterColumn.split(",").map(col => "t2." + col).mkString(",") + " from " + masterTable + " as t2, " +
          "(select sn, product, min(scantime) as scantime from " + masterTable + " where " + snCondition + " group by sn, product) as t1 " +
          "where t2.sn=t1.sn and t2.product = t1.product and t1.scantime=t2.scantime"
        val partMasterDf = IoUtils.getDfFromCockroachdb(spark, masterSql, numExecutors)
            .withColumnRenamed("floor", "scan_floor")
//        partMasterDf.show(false)

        println("-----------------> select part table (first scantime) where sn, product end_time:" + new SimpleDateFormat(
          configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))


        println("-----------------> select part detail table where id start_time:" + new SimpleDateFormat(
          configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

        //找出所有組裝主表id在組裝細表第一筆scantime的line線別
        val partMasterIdList = partMasterDf.select("id").dropDuplicates().map(_.getString(0)).collect.toList

        val idCondition = "id in (" + partMasterIdList.map(s => "'" + s + "'").mkString(",") + ")"

        val detailSql = "select " +  detailLineColumn + " from " + detailTable +
          " where " + idCondition

        //group by id, order by scantime asc, 取第一筆
        val wSpecPartDetailAsc = Window.partitionBy(col("id"))
          .orderBy(asc("scantime"))

        val partDetailDf = IoUtils.getDfFromCockroachdb(spark, detailSql, numExecutors)
          .withColumn("rank", rank().over(wSpecPartDetailAsc))
          .where($"rank".equalTo(1))
          .drop("rank", "scantime")

        println("-----------------> select part detail table where id end_time:" + new SimpleDateFormat(
          configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

        testDetailCockroachDf = testDetailCockroachDf.join(partMasterDf, Seq("sn"), "left")
            .join(partDetailDf, Seq("id"), "left")

        println("-----------------> save test_detail start_time:" + new SimpleDateFormat(
          configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

        println("saveToCockroachdb --> testDetailCockroachDf")
        IoUtils.saveToCockroachdb(testDetailCockroachDf,
          configLoader.getString("log_prop", "test_detail_table"),
          numExecutors)

        println("-----------------> save test_detail end_time:" + new SimpleDateFormat(
          configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))


        testDetailTempDf = testDetailTempDf
          .withColumn("temp", arrays_zip($"test_item", $"test_upper", $"test_lower", $"test_unit", $"test_value"))
          .withColumn("temp", explode($"temp"))
          .selectExpr("product", "station_name", "temp.test_item as test_item", "temp.test_upper as test_upper",
            "temp.test_lower as test_lower", "temp.test_unit as test_unit", "temp.test_value as test_value",
            "test_version", "test_starttime")
          .withColumn("test_item", regexp_replace($"test_item", ctrlDCode, ctrlDValue))
          .withColumn("test_upper", parseColumnValue(col("test_upper")))
          .withColumn("test_lower", parseColumnValue(col("test_lower")))
          .withColumn("test_unit", parseColumnValue(col("test_unit")))
          .withColumn("test_value", parseColumnValue(col("test_value")))
          .withColumn("test_item_datatype", castColumnDataType(col("test_value")))
          .drop("test_value")

        //1-2: 將測項上下界撈出來之後, 根據測試版號與時間選最新
        //insert product item spec to mysql
        val itemSpecColumnStr = "product,station_name,test_item,test_upper,test_lower,test_unit,test_version,test_starttime,test_item_datatype"
        val itemSpecColumn = itemSpecColumnStr.split(",")
        val itemSpecSql = "select " + itemSpecColumnStr + " from " + productItemSpecTable + " where " + productCondition
        println(itemSpecSql)
        val productItemSpecDf = mariadbUtils
          .getDfFromMariadbWithQuery(spark, itemSpecSql, numExecutors)
        //                .getDfFromMariadb(spark, "product_item_spec")
        //                .selectExpr(itemSpecColumn: _*)
        //                .where(col("product").isin(productList:_*))

        testDetailTempDf = testDetailTempDf.selectExpr(itemSpecColumn: _*)
        testDetailTempDf = productItemSpecDf.union(testDetailTempDf)

        val wSpec = Window.partitionBy(col("product"), col("station_name"), col("test_item"))
          .orderBy(desc("test_version"), desc("test_starttime"))

        testDetailTempDf = testDetailTempDf
          .withColumn("rank", rank().over(wSpec))
          .where($"rank".equalTo(1)).drop("rank")
          .drop("test_value")

        println("saveToMariadb --> testDetailTempDf")

        mariadbUtils.saveToMariadb(
          testDetailTempDf,
          productItemSpecTable,
          numExecutors
        )

        val productStationColumnStr = "product,station_name,flag,station_name_user"
        val productStationTable = "product_station"
        val productStationSql = "select " + productStationColumnStr + " from " + productStationTable + " where " + productCondition
        //1-3: insert product station to mysql
        //紀錄Product、工站名稱 list
        var productStationDf = mariadbUtils
          .getDfFromMariadbWithQuery(spark, productStationSql, numExecutors)
        //          .getDfFromMariadb(spark, "product_station")
        //          .select("product", "station_name", "flag", "station_name_user")
        //          .where(col("product").isin(productList:_*))

        productStationDf = productStationDf.union(
          testDetailSourceDf.select("product", "station_name")
            .dropDuplicates()
            .withColumn("flag", lit(1)) //flag: 1:extract from row data, 2:user insert from web
            .withColumn("station_name_user", lit(null)))
        productStationDf = productStationDf.dropDuplicates("product", "station_name")

        println("saveToMariadb --> productStationDf")

        mariadbUtils.saveToMariadb(
          productStationDf,
          "product_station",
          numExecutors
        )
      }

      //(2)工單
//      val woDestPath = IoUtils.flatMinioFiles(spark,
//        flag,
//        woPath,
//        woFileLmits)

      val woDestPath = IoUtils.flatMinioFiles(spark,
        woPath,
        totalRawDataSize,
        jobYear + jobMonth,
        jobDay,
        jobHour + jobMinute + jobSecond)

      var woSourceDf = IoUtils.getDfFromPath(spark, woDestPath.toString, woColumns, dataSeperator)
      woSourceDf = woSourceDf.drop("prodversion","create_date")
        .withColumn("release_date", unix_timestamp(trim($"release_date"), woDtfmt)
          .cast(TimestampType))
//      woSourceDf = woSourceDf.drop("release_date","prodversion","create_date")
        .withColumn("upsert_time", lit(jobStartTime).cast(TimestampType))
println("woSourceDf from file, Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb + " MB")

woSourceDf.show(3, false)

      //將工單資料儲存進Cockroachdb
      println("saveToCockroachdb --> woSourceDf")
      IoUtils.saveToCockroachdb(woSourceDf,
        configLoader.getString("log_prop", "wo_table"),
        numExecutors)

      //(3)關鍵物料
//      val matDestPath = IoUtils.flatMinioFiles(spark,
//        flag,
//        matPath,
//        matFileLmits)

      val matDestPath = IoUtils.flatMinioFiles(spark,
        matPath,
        totalRawDataSize,
        jobYear + jobMonth,
        jobDay,
        jobHour + jobMinute + jobSecond)

      var matSourceDf = IoUtils.getDfFromPath(spark, matDestPath.toString, matColumns, dataSeperator)
println("matSourceDf from file, Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb + " MB")

      matSourceDf = matSourceDf.withColumn("upsert_time", lit(jobStartTime).cast(TimestampType))
matSourceDf.show(3, false)
      //3.1 將關鍵物料資料儲存進Cockroachdb
      println("saveToCockroachdb --> matSourceDf")
      IoUtils.saveToCockroachdb(matSourceDf, matTable, numExecutors)
      //3.2 將關鍵物料資料儲存進mariadb
      println("saveToMariadb --> matSourceDf")
      mariadbUtils.saveToMariadb(matSourceDf.drop("upsert_time"), matTable, numExecutors)

      val jobEndTime: String = new SimpleDateFormat(
        configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime())
      println("job end time : " + jobEndTime)
      jobStatus = true

    } catch {
      case ex: FileNotFoundException => {
        // ex.printStackTrace()
        println("===> FileNotFoundException !!!")
      }
    }
  }

  def getHostName(): String = {
    var hostName = ""
    try {
      val ip = InetAddress.getLocalHost()
      hostName = ip.getHostName
    } catch {
      case ex: Exception => {
        println("===> Get Pod Hostname Exception !!!")
        ex.printStackTrace()
      }
    }
    hostName
  }

}
