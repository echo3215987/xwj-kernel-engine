package com.foxconn.iisd.bd.rca

import com.foxconn.iisd.bd.rca.SparkUDF.{castColumnDataType, parseArrayToString, parseColumnValue, parseStringToJSONString}
import com.foxconn.iisd.bd.rca.XWJKernelEngine.{configContext, configLoader}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Encoders}
import org.apache.spark.storage.StorageLevel

/*
 *
 *
 * @author EchoLee
 * @date 2019/11/7 下午 04:31
 * @description 處理工單資料流
 */
class WoDataProcessing(configContext: ConfigContext) extends BaseDataProcessing {

  val sparkSession = configContext.sparkSession
  import sparkSession.implicits._

  def  woCoreEngine (): Unit  = {
    var woSourceDf = configContext.fileDataSource.fetchWoDataDf()

    val upsertTime = new java.util.Date()

    woSourceDf = woSourceDf.drop("prodversion","create_date")
      .withColumn("release_date", unix_timestamp(trim($"release_date"), configContext.woDtFmt)
        .cast(TimestampType))
      .withColumn("upsert_time", lit(upsertTime).cast(TimestampType))
      .withColumn("ke_flag", lit(configContext.flag))

    woSourceDf.show(3, false)

    //將工單資料儲存進Cockroachdb
    println("saveToCockroachdb --> woSourceDf")
    configContext.cockroachDBIo.saveToCockroachdb(
      woSourceDf,
      configContext.cockroachDbWoTable,
      configContext.sparkNumExcutors)
  }

  override def saveTotalCnt2SummaryFile(dataFrame: DataFrame): Unit = {
    val woFilesTotalRows = dataFrame.count()
    SummaryFile.woFilesTotalRows = woFilesTotalRows
    SummaryFile.woTotalRowsInCockroachdb = woFilesTotalRows
  }

  override def saveDistTotalCnt2SummaryFile(dataFrame: DataFrame): Unit = {
    val woFilesTotalRowsDist = dataFrame.count()
    SummaryFile.woFilesTotalRowsDist = woFilesTotalRowsDist
    SummaryFile.woTotalRowsDistInCockroachdb = woFilesTotalRowsDist
  }

}
