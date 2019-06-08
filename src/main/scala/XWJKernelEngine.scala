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

import scala.collection.mutable._


object KernelEngine{

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
                KernelEngine.start()
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
        }*/

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

        val factory = configLoader.getString("general", "factory")

        val failCondition: Int = configLoader.getString("analysis", "fail_condition").toInt

        //s3a://" + bucket + "/
        val wipPath = configLoader.getString(logPathSection, "wip_path")
        val wipPartsPath = configLoader.getString(logPathSection, "wip_parts_path")
        val bobcatPath = configLoader.getString(logPathSection, "bobcat_path")

        val wipFileLmits = configLoader.getString(logPathSection, "wip_file_limits").toInt
        val wipPartsFileLmits = configLoader.getString(logPathSection, "wip_parts_file_limits").toInt
        val bobcatFileLmits = configLoader.getString(logPathSection, "bobcat_file_limits").toInt

        //"id,sn,wo,hh_part,cust_part,assembline,scantime,na1,na2,product,floor"
        //CN8BJ7C2G1_|_CN8BJ7C2G1_|_296200140_|_V1N03B#629_|_V1N03B#629_|_Main	B_|_2018-11-19	13:39:07.467_|__|__|_TaiJi	Base_|_D626
        val wipColumns = configLoader.getString("log_prop", "wip_col")

        //"id,partsn,scantime,opid,assembly_station,part,hh_pard,cust_part,line,disassembly"
        //CN8BJ7C2G1_|_C189220M8PFAC_|_2018-11-19 13:39:07.467_|_F1034891_|_Main B_|_LK_|_F5S43-60001_|_F5S43-60001_|_D626_|_0
        val wipPartsColumns = configLoader.getString("log_prop", "wip_parts_col")

        //"sn,station,stationcode,machine,start_time,end_time,istestfail,symptom,desc,uploadtime,emp,ver1,ver2"
        //CN8BS7C334_||_TLEOL_||_TLEOL_||_LC_TLEOL_41_||_11/29/2018 8:33:10 AM_||_11/29/2018 8:38:46 AM_||_0_||__||__||_11/29/2018 8:38:46 AM_||_ _||_ _||_
        val bobcatColumns = configLoader.getString("log_prop", "bobcat_col")


        ///////////
        //載入資料//
        ///////////

        try {

        } catch {
            case ex: FileNotFoundException => {
                // ex.printStackTrace()
                println("===> FileNotFoundException !!!")
            }
        }
    }

}
