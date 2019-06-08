import java.io.FileNotFoundException
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.foxconn.iisd.bd.config.ConfigLoader
import com.foxconn.iisd.bd.rca.XWJKernelEngine.configLoader
import com.foxconn.iisd.bd.rca.utils.IoUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.Seq

object Test{

    var configLoader = new ConfigLoader()
    val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

    def main(args: Array[String]): Unit = {

        /*val spark = SparkSession.builder()
          .appName("Spark SQL basic example")
          .config("spark.master", "local")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")*/
/*
        var test = Vector("SpecRedNomCh123_Bits8.1=1", "SpecMaxChipTolFromNom_Bits8=2", "SpecMaxChipTolFromNom_Bits7=3")

        var upperValue = test.filter(testparm=> testparm.contains("SpecRedNomCh123_Bits8.1")).mkString.split("=")(1).toFloat
        var upperSub = test.filter(testparm=> testparm.contains("SpecMaxChipTolFromNom_Bits8")).mkString.split("=")(1).toFloat

        println(upperValue)
        println(upperSub)
        println(upperValue-upperSub)

        //var test = Vector("SpecRedNomCh123_Bits8.1=1", "SpecMaxChipTolFromNom_Bits8=2", "SpecMaxChipTolFromNom_Bits7=3")


        var cc = spark.read.option("header", "true").csv("C:\\Users\\foxconn\\Desktop\\cc.csv")
        cc.withColumn("test", getSpec(col("item"), col("compare")))
        def getSpec = udf {
            (item: String, compare: Seq[String]) => item  match {
                case "PcaVerifyFirmwareRev^DReadVersion" => {
                    //var upper = compare.filter(testparm => testparm.contains("ValidFWRevs")).mkString.split("=")(1)
                    var upper = getSpecBoundary(compare, "ValidFWRevs")
                    var lower = upper
                    Vector(upper, lower)
                }
            }

        }

        def getSpecBoundary(compare: Seq[String], spec: String): String = {
            compare.filter(testparm => testparm.contains(spec)).mkString.split("=")(1)
        }
*/

        var date: java.util.Date = new java.util.Date()
        val flag = date.getTime().toString
        /*val jobStartTime: String = new SimpleDateFormat(
            configLoader.getString("summary_log_path","job_fmt")).format(date.getTime())
        println("job start time : " + jobStartTime)
        Summary.setJobStartTime(jobStartTime)
*/      configLoader.setDefaultConfigPath("""conf/default.yaml""")
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

        //parse data
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
        val testDetailPath = configLoader.getString(logPathSection, "test_detail_path")
        println(testDetailPath)
        val testDetailFileLmits = configLoader.getString(logPathSection, "test_detail_file_limits").toInt

        //"sn,build_name,build_description,unit_number,station_id,test_status,test_starttime,test_endtime,list_of_failure,list_of_failure_detail,test_phase,machine_id,factory_code,floor,line_id,test_item,test_value,test_unit,test_lower,test_upper,create_time,update_time,station_name,start_date,product,test_version"
        //CN95I870ZC06MD_||_SOR_||_SOR_||_CN95I870ZC06MD_||_L7_TLEOL_06_||_Exception_||_2019/05/18 06:36_||_2019/05/18 06:36_||_PcaVerifyFirmwareRev_||_Error_||_MP_||__||_CQ_||_D62_||_2_||_ProcPCClockSync^DResultInfo^APcaVerifyFirmwareRev^DResultInfo^APcaVerifyFirmwareRev^DExpectedVersion^APcaVerifyFirmwareRev^DReadVersion^APcaVerifyFirmwareRev^DDateTimeStarted^APcaVerifyFirmwareRev^DActualFWUpdate^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C5/18/2019 5:29:48 AM^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_2019/05/18 06:36_||_2019/05/18 06:36_||_TLEOL_||_2019/05/18 06:36_||_TaiJi Base_||_42.3.8 REV_37_Taiji25
        val testDetailColumns = configLoader.getString("log_prop", "test_detail_col")

        val dataSeperator = configLoader.getString("log_prop", "log_seperator")
        ///////////
        //載入資料//
        ///////////

        try {
            val testDetailDestPath = "C:/Users/foxconn/Desktop/RCA/ask/.txt/*.txt"
            var testDetailSourceDf = IoUtils.getDfFromPath(spark, testDetailDestPath.toString, testDetailColumns, dataSeperator)

            //"sn,build_name,build_description,unit_number,station_id,test_status,test_starttime,test_endtime,list_of_failure,list_of_failure_detail,test_phase,machine_id,factory_code,
            // floor,line_id,test_item,test_value,test_unit,test_lower,test_upper,create_time,update_time,station_name,start_date,product,test_version"
            testDetailSourceDf.select("sn").show(false)
            testDetailSourceDf.select("build_name").show(false)
            testDetailSourceDf.select("build_description").show(false)
            testDetailSourceDf.select("unit_number").show(false)
            testDetailSourceDf.select("station_id").show(false)
            testDetailSourceDf.select("test_status").show(false)
            testDetailSourceDf.select("test_starttime").show(false)
            testDetailSourceDf.select("test_starttime").show(false)
            testDetailSourceDf.select("list_of_failure").show(false)
            testDetailSourceDf.select("list_of_failure_detail").show(false)
            testDetailSourceDf.select("test_phase").show(false)
            testDetailSourceDf.select("machine_id").show(false)
            testDetailSourceDf.select("factory_code").show(false)
            testDetailSourceDf.select("floor").show(false)
            testDetailSourceDf.select("line_id").show(false)

            testDetailSourceDf.select("create_time").show(false)
            testDetailSourceDf.select("update_time").show(false)
            testDetailSourceDf.select("station_name").show(false)
            testDetailSourceDf.select("start_date").show(false)
            testDetailSourceDf.select("product").show(false)
            testDetailSourceDf.select("test_version").show(false)

        }catch{
            case ex: Exception => {
                // ex.printStackTrace()
                println("===> Exception !!!")
            }
        }

    }

}
