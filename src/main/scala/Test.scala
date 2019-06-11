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

        val spark = SparkSession.builder()
          .appName("Spark SQL basic example")
          .config("spark.master", "local")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
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

        try {

            val testDetailDestPath = "C:/Users/foxconn/Desktop/test.txt"
            var df = spark.read.text(testDetailDestPath)
            df.selectExpr("regexp_extract(value,'(TestResultInfo=\")(.+)(TestDateTimeStarted=)',2)").show(false)
        }catch{
            case ex: Exception => {
                // ex.printStackTrace()
                println("===> Exception !!!")
            }
        }

    }

}
