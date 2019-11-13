package com.foxconn.iisd.bd.rca

import java.text.SimpleDateFormat

import com.foxconn.iisd.bd.rca.XWJKernelEngine.configContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

/*
 *
 *
 * @author EchoLee
 * @date 2019/11/7 下午 04:31
 * @description 處理關鍵物料資料流
 */
class MatDataProcessing(configContext: ConfigContext) extends BaseDataProcessing {

  val sparkSession = configContext.sparkSession
  import sparkSession.implicits._

  def  calculation (): Unit  = {
    var matSourceDf = configContext.fileDataSource.fetchMatDataDf()
    saveTotalCnt2SummaryFile(matSourceDf)

    val upsertTime = new SimpleDateFormat(configContext.jobDateFmt).format(new java.util.Date().getTime())

    matSourceDf = matSourceDf.withColumn("upsert_time", lit(upsertTime).cast(TimestampType))
      .withColumn("ke_flag", lit(configContext.flag))
      .distinct()

    saveDistTotalCnt2SummaryFile(matSourceDf)

    matSourceDf.show(3, false)

    //3.1 將關鍵物料資料儲存進Cockroachdb
    println("saveToCockroachdb --> matSourceDf")
    configContext.cockroachDBIo.saveToCockroachdb(
      matSourceDf,
      configContext.cockroachDbMatTable,
      configContext.sparkNumExcutors)

    //3.2 將關鍵物料資料儲存進mariadb
    println("saveToMariadb --> matSourceDf")
    matSourceDf = matSourceDf.drop("upsert_time")
    configContext.mysqlDBIo.saveToMariadb(matSourceDf, configContext.mysqlProductItemSpecTable, configContext.sparkNumExcutors)

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
