package com.foxconn.iisd.bd.rca

import java.text.SimpleDateFormat

import com.foxconn.iisd.bd.rca.utils.SummaryFile
import org.apache.spark.sql.functions.{lit, regexp_replace, trim, unix_timestamp}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.storage.StorageLevel

/*
 *
 *
 * @author JasonLai
 * @date 2019/10/2 下午2:29
 * @description 處理Master資料流
 */
class MasterDataProcessing(configContext: ConfigContext) extends BaseDataProcessing {

  val schema = StructType(configContext.wipColumns
    .split(",")
    .map(fieldName => StructField(fieldName, StringType, true)))

  val sparkSession = configContext.sparkSession
  import sparkSession.implicits._

  override def readData(): DataFrame = {
    val dataRdd = configContext.sparkSession.sparkContext.emptyRDD[Row]
    var masterDf = configContext.sparkSession.createDataFrame(dataRdd, schema)

    if(configContext.dataProcessingMode.equals(KEConstants.DATA_PROCESSING_NORMAL)) {
      masterDf = configContext.fileDataSource.fetchMasterDataDf()
    // daily
    } else {
      //do nothing
    }
    masterDf
  }

  override def saveTotalCnt2SummaryFile(dataFrame: DataFrame): Unit = {
    val masterFilesTotalRows = dataFrame.count()
    SummaryFile.masterFilesTotalRows = masterFilesTotalRows
    SummaryFile.masterTotalRowsInCockroachdb = masterFilesTotalRows
  }

  override def transformDataframe(dataFrame: DataFrame): DataFrame = {
    var masterProductScantimeDf = dataFrame
    if(SummaryFile.masterFilesTotalRows > 0) {
      masterProductScantimeDf = dataFrame.distinct()
        .withColumn("product", regexp_replace($"product", "\t", " "))
        .withColumn("scantime1", unix_timestamp(trim($"scantime"),
          configContext.wipDtFmt).cast(TimestampType))
        .drop("scantime")
        .withColumnRenamed("scantime1", "scantime")
        .orderBy("scantime")
        .withColumn("factory", lit(configContext.factory))
        .withColumn("ke_flag", lit(configContext.flag))
        .withColumn("createTime", lit(new SimpleDateFormat(configContext.jobDateFmt).format(configContext.job.jobStartTime.getTime)))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
    masterProductScantimeDf
  }

  override def saveDistTotalCnt2SummaryFile(dataFrame: DataFrame): Unit = {
    val masterFilesTotalRowsDist = dataFrame.count()
    SummaryFile.masterFilesTotalRowsDist = masterFilesTotalRowsDist
    SummaryFile.masterTotalRowsDistInCockroachdb = masterFilesTotalRowsDist
  }

  override def saveDB(dataFrame: DataFrame): Unit = {
    println("======> save master data into cockroach database")
    dataFrame.show(false)
    configContext.cockroachDBIo.saveToCockroachdb(
      dataFrame,
      configContext.cockroachDbMasterTable,
      configContext.sparkNumExcutors
    )
  }

  override def saveFirstRowTime2SummaryFile(dataFrame: DataFrame): Unit = {
    println("======> first row by masterProductScantimeDf")
    if(dataFrame.count() > 0) {
      val masterProductScantimeDfFirstTimeList = dataFrame
        .select("scantime")
        .withColumn("scantime2", $"scantime".cast(StringType))
        .select("scantime2")
        .orderBy($"scantime2".asc)
        .limit(1)
        .rdd
        .map(r => r(0))
        .collect
        .toList
        .asInstanceOf[List[String]]

      println("======> masterProductScantimeDfFirstTimeList(0) : " + masterProductScantimeDfFirstTimeList(0))
      SummaryFile.masterFirstRowTimeInCockroachdb = masterProductScantimeDfFirstTimeList(0)
    } else {
      SummaryFile.masterFirstRowTimeInCockroachdb = "N/A"
    }
  }

  override def saveLastRowTime2SummaryFile(dataFrame: DataFrame): Unit = {
    println("======> last row by masterProductScantimeDf")
    if(dataFrame.count() > 0) {
      val masterProductScantimeDfLastTimeList = dataFrame
        .select("scantime")
        .withColumn("scantime2", $"scantime".cast(StringType))
        .select("scantime2")
        .orderBy($"scantime2".desc)
        .limit(1)
        .rdd
        .map(r => r(0))
        .collect
        .toList
        .asInstanceOf[List[String]]

      println("======> masterProductScantimeDfLastTimeList(0) : " + masterProductScantimeDfLastTimeList(0))
      SummaryFile.masterLastRowTimeInCockroachdb = masterProductScantimeDfLastTimeList(0)
    } else {
      SummaryFile.masterLastRowTimeInCockroachdb = "N/A"
    }
  }
}
