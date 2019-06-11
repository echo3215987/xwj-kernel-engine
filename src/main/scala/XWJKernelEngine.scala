package com.foxconn.iisd.bd.rca

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.foxconn.iisd.bd.config.ConfigLoader
import com.foxconn.iisd.bd.rca.utils.IoUtils
import com.foxconn.iisd.bd.rca.utils.Summary
import com.foxconn.iisd.bd.rca.utils.db._
import com.foxconn.iisd.bd.rca.SparkUDF._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.Encoders
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import java.io.File

import com.mysql.cj.x.protobuf.MysqlxDatatypes.Scalar
import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.Path

import scala.collection.mutable._


object XWJKernelEngine{

    var configLoader = new ConfigLoader()
    val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

    def main(args: Array[String]): Unit = {

        val limit = 1
        var count = 0

        println("xyj-kernel-engine-v1:")

        while(count < limit) {
            println(s"count: $count")

            try {
                configLoader.setDefaultConfigPath("""conf/default.yaml""")
                if(args.length == 1) {
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
        /*val jobStartTime: String = new SimpleDateFormat(
            configLoader.getString("summary_log_path","job_fmt")).format(date.getTime())
        println("job start time : " + jobStartTime)
        Summary.setJobStartTime(jobStartTime)
*/
        println(s"flag: $flag"+": xwj")

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

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

        //"sn,build_name,build_description,unit_number,station_id,test_status,test_starttime,test_endtime,list_of_failure,list_of_failure_detail,test_phase,machine_id,factory_code,floor,line_id,test_item,test_value,test_unit,test_lower,test_upper,create_time,update_time,station_name,start_date,product,test_version"
        //CN95I870ZC06MD_||_SOR_||_SOR_||_CN95I870ZC06MD_||_L7_TLEOL_06_||_Exception_||_2019/05/18 06:36_||_2019/05/18 06:36_||_PcaVerifyFirmwareRev_||_Error_||_MP_||__||_CQ_||_D62_||_2_||_ProcPCClockSync^DResultInfo^APcaVerifyFirmwareRev^DResultInfo^APcaVerifyFirmwareRev^DExpectedVersion^APcaVerifyFirmwareRev^DReadVersion^APcaVerifyFirmwareRev^DDateTimeStarted^APcaVerifyFirmwareRev^DActualFWUpdate^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C5/18/2019 5:29:48 AM^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_2019/05/18 06:36_||_2019/05/18 06:36_||_TLEOL_||_2019/05/18 06:36_||_TaiJi Base_||_42.3.8 REV_37_Taiji25
        val testDetailColumns = configLoader.getString("log_prop", "test_detail_col")

        val dataSeperator = configLoader.getString("log_prop", "log_seperator")

        ///////////
        //載入資料//
        ///////////

        //try {
            val testDetailDestPath = IoUtils.flatMinioFiles(spark,
                flag,
                testDetailPath,
                testDetailFileLmits)

            var testDetailSourceDf = IoUtils.getDfFromPath(spark, testDetailDestPath.toString, testDetailColumns, dataSeperator)


            //val testDetailSourceDfCnt = testDetailSourceDf.count()

            testDetailSourceDf = testDetailSourceDf.distinct()
                //.withColumn("product", regexp_replace($"product", "\t", " ")
                .withColumn("test_starttime",
                    unix_timestamp(trim($"test_starttime"),
                        configLoader.getString("log_prop", "test_detail_dt_fmt")).cast(TimestampType))
                .withColumn("test_endtime",
                    unix_timestamp(trim($"test_endtime"),
                        configLoader.getString("log_prop", "test_detail_dt_fmt")).cast(TimestampType))
                .withColumn("create_time",
                    unix_timestamp(trim($"create_time"),
                        configLoader.getString("log_prop", "test_detail_dt_fmt")).cast(TimestampType))
                .withColumn("start_date",
                    unix_timestamp(trim($"start_date"),
                        configLoader.getString("log_prop", "test_detail_dt_fmt")).cast(TimestampType))
                .withColumn("update_time",
                    unix_timestamp(trim($"update_time"),
                        configLoader.getString("log_prop", "test_detail_dt_fmt")).cast(TimestampType))
                .withColumn("test_item", parseArrayToString(split(trim($"test_item"), "\001")))
                .withColumn("test_item", concat(lit("ARRAY["), $"test_item", lit("]")))
                .withColumn("test_value", parseStringToJSONString(split(trim($"test_value"), "\001")))
                .withColumn("test_upper", parseStringToJSONString(split(trim($"test_upper"), "\001")))
                .withColumn("test_lower", parseStringToJSONString(split(trim($"test_lower"), "\001")))
                .withColumn("test_unit", parseStringToJSONString(split(trim($"test_unit"), "\001")))
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

            val testDetailSourceDfDistCnt = testDetailSourceDf.count()

            testDetailSourceDf.select("test_item").show(false)
            testDetailSourceDf.printSchema()

            //TODO: summary file
            //Summary.setMasterFilesNameList(IoUtils.getFilesNameList(spark, testDetailDestPath))
            //                  testDetailSourceDf.select("test_upper").show(false)
            //                  testDetailSourceDf.select("test_lower").show(false)
            //                  testDetailSourceDf.select("test_unit").show(false)
            //            testDetailSourceDf.printSchema()

            //將資料儲存進Cockroachdb
            println("saveToCockroachdb --> testDetailSourceDf")
            IoUtils.saveToCockroachdb(testDetailSourceDf,
              configLoader.getString("log_prop", "test_detail_table"),
              numExecutors)



            /*} catch {
                case ex: FileNotFoundException => {
                    // ex.printStackTrace()
                    println("===> FileNotFoundException !!!")
                }
            }*/
    }

}
