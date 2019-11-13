package com.foxconn.iisd.bd.rca

import java.text.SimpleDateFormat
import java.util.Date

import com.foxconn.iisd.bd.rca.SparkUDF.{castColumnDataType, parseArrayToString, parseColumnValue, parseStringToJSONString}
import com.foxconn.iisd.bd.rca.XWJKernelEngine.{configContext, configLoader}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Encoders, Row}
import org.apache.spark.storage.StorageLevel

/*
 *
 *
 * @author EchoLee
 * @date 2019/11/7 下午 04:31
 * @description 處理測試細表資料流
 */
class TestDetailDataProcessing(configContext: ConfigContext) extends BaseDataProcessing {

  val sparkSession = configContext.sparkSession
  import sparkSession.implicits._

  def  testDetailCoreEngine (): Unit  = {
    //1: 將測試結果表資料儲存進Cockroachdb
    val testDetailSourceDf = configContext.fileDataSource.fetchTestDetailDataDf()

    saveTotalCnt2SummaryFile(testDetailSourceDf)

    var testDetailTempDf = testDetailSourceDf.distinct()
      .withColumn("test_item", split(trim($"test_item"), XWJKEConstants.ctrlACode))
      .withColumn("test_value", split(trim($"test_value"), XWJKEConstants.ctrlACode))
      .withColumn("test_upper", split(trim($"test_upper"), XWJKEConstants.ctrlACode))
      .withColumn("test_lower", split(trim($"test_lower"), XWJKEConstants.ctrlACode))
      .withColumn("test_unit", split(trim($"test_unit"), XWJKEConstants.ctrlACode))
      //        新增兩個欄位test_item_result, test_item_result_detail
      .withColumn("test_item_result", split(trim($"test_item_result"), XWJKEConstants.ctrlACode))
      .withColumn("test_item_result_detail", split(trim($"test_item_result_detail"), XWJKEConstants.ctrlACode))

      .withColumn("list_of_failure", regexp_replace($"list_of_failure", XWJKEConstants.ctrlACode, XWJKEConstants.ctrlAValue))
      .withColumn("list_of_failure_detail", regexp_replace($"list_of_failure_detail", XWJKEConstants.ctrlACode, XWJKEConstants.ctrlAValue))
      .persist(StorageLevel.MEMORY_AND_DISK)

    saveDistTotalCnt2SummaryFile(testDetailTempDf)

    testDetailTempDf.show(3, false)

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

    val upsertTime = new java.util.Date()

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
      .withColumn("upsert_time", lit(upsertTime).cast(TimestampType))
      .withColumn("ke_flag", lit(configContext.flag))

    println("testDetailCockroachDf count:" + testDetailCockroachDf.count())

    //調整: insert value_rank flag and select part_master
    val snList = testDetailCockroachDf.select("sn").dropDuplicates().map(_.getString(0)).collect.toList
    val snSize = snList.size
    println("sn count:" + snSize)
    if (snSize > 0) {
      val snCondition = "sn in (" + snList.map(s => "'" + s + "'").mkString(",") + ")"
      val testDetailWhereStr = "product,station_name,sn"
      val testDetailWhereColumn = testDetailWhereStr.split(",")
      val testDetailSelectColumnStr = "product,station_name,sn,value_rank,test_starttime"

      val productList = testDetailTempDf.select("product").dropDuplicates().as(Encoders.STRING).collect()
      val productCondition = "product in (" + productList.map(s => "'" + s + "'").mkString(",") + ")"

      //找出所有sn在組裝主表第一筆scantime的工單號
      val masterSql = "select " + configContext.cockroachDbMasterColumns.split(",").map(col => "t2." + col).mkString(",") +
        " from " + configContext.cockroachDbMasterTable + " as t2, " +
        "(select sn, product, min(scantime) as scantime from " + configContext.cockroachDbMasterTable +
        " where " + snCondition + " group by sn, product) as t1 " +
        "where t2.sn=t1.sn and t2.product = t1.product and t1.scantime=t2.scantime"

      val partMasterDf = configContext.cockroachDBIo.getDfFromCockroachdb(configContext.sparkSession, masterSql, configContext.sparkNumExcutors)
        .withColumnRenamed("floor", "scan_floor")

      //找出所有組裝主表id在組裝細表第一筆scantime的line線別
      val partMasterIdList = partMasterDf.select("id").dropDuplicates().map(_.getString(0)).collect.toList

      val idCondition = "id in (" + partMasterIdList.map(s => "'" + s + "'").mkString(",") + ")"

      val detailSql = "select " + configContext.cockroachDbDetailColumns + " from " + configContext.cockroachDbDetailTable +
        " where " + idCondition

      //group by id, order by scantime asc, 取第一筆
      val wSpecPartDetailAsc = Window.partitionBy(col("id"))
        .orderBy(asc("scantime"))

      val partDetailDf = configContext.cockroachDBIo.getDfFromCockroachdb(configContext.sparkSession, detailSql, configContext.sparkNumExcutors)
        .withColumn("rank", rank().over(wSpecPartDetailAsc))
        .where($"rank".equalTo(1))
        .drop("rank", "scantime")

      testDetailCockroachDf = testDetailCockroachDf.join(partMasterDf, Seq("sn"), "left")
        .join(partDetailDf, Seq("id"), "left")

      println("saveToCockroachdb --> testDetailCockroachDf")
      configContext.cockroachDBIo.saveToCockroachdb(
        testDetailCockroachDf,
        configContext.cockroachDbTestDetailTable,
        configContext.sparkNumExcutors
      )


      //2: 將測項上下界撈出來之後, 根據測試版號與時間選最新
      //insert product item spec to mysql
      testDetailTempDf = testDetailTempDf
        .withColumn("temp", arrays_zip($"test_item", $"test_upper", $"test_lower", $"test_unit", $"test_value"))
        .withColumn("temp", explode($"temp"))
        .selectExpr("product", "station_name", "temp.test_item as test_item", "temp.test_upper as test_upper",
          "temp.test_lower as test_lower", "temp.test_unit as test_unit", "temp.test_value as test_value",
          "test_version", "test_starttime")
        .withColumn("test_item", regexp_replace($"test_item", XWJKEConstants.ctrlDCode, XWJKEConstants.ctrlDValue))
        .withColumn("test_upper", parseColumnValue(col("test_upper")))
        .withColumn("test_lower", parseColumnValue(col("test_lower")))
        .withColumn("test_unit", parseColumnValue(col("test_unit")))
        .withColumn("test_value", parseColumnValue(col("test_value")))
        .withColumn("test_item_datatype", castColumnDataType(col("test_value")))
        .drop("test_value")

      val itemSpecColumnStr = "product,station_name,test_item,test_upper,test_lower,test_unit,test_version,test_starttime,test_item_datatype"
      val itemSpecColumn = itemSpecColumnStr.split(",")
      val itemSpecSql = "select " + itemSpecColumnStr + " from " + configContext.mysqlProductItemSpecTable + " where " + productCondition

      val productItemSpecDf = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession,
        itemSpecSql, configContext.sparkNumExcutors)

      testDetailTempDf = testDetailTempDf.selectExpr(itemSpecColumn: _*)
      testDetailTempDf = productItemSpecDf.union(testDetailTempDf)

      val wSpec = Window.partitionBy(col("product"), col("station_name"), col("test_item"))
        .orderBy(desc("test_version"), desc("test_starttime"))

      testDetailTempDf = testDetailTempDf
        .withColumn("rank", rank().over(wSpec))
        .where($"rank".equalTo(1)).drop("rank")
        .drop("test_value")

      println("saveToMariadb --> testDetailTempDf")
      configContext.mysqlDBIo.saveToMariadb(testDetailTempDf, configContext.mysqlProductItemSpecTable, configContext.sparkNumExcutors)

      //3: insert product station to mysql
      //紀錄Product、工站名稱 list
      val productStationColumnStr = "product,station_name,flag,station_name_user"
      val productStationTable = "product_station"
      val productStationSql = "select " + productStationColumnStr + " from " + productStationTable + " where " + productCondition

      var productStationDf = configContext.cockroachDBIo.getDfFromCockroachdb(configContext.sparkSession,
        productStationSql, configContext.sparkNumExcutors)

      productStationDf = productStationDf.union(
        testDetailSourceDf.select("product", "station_name")
          .dropDuplicates()
          .withColumn("flag", lit(1)) //flag: 1:extract from row data, 2:user insert from web
          .withColumn("station_name_user", lit(null)))
      productStationDf = productStationDf.dropDuplicates("product", "station_name")

      println("saveToMariadb --> productStationDf")
      configContext.mysqlDBIo.saveToMariadb(productStationDf, configContext.mysqlProductStationTable, configContext.sparkNumExcutors)
    }
  }

  def  testDetailDateStringToTimestamp (colName:String, configkey: String, configValue: String): Column  = {
    unix_timestamp(trim(col(colName)),
      configLoader.getString(configkey, configValue)).cast(TimestampType)
  }

  override def saveTotalCnt2SummaryFile(dataFrame: DataFrame): Unit = {
    val testDetailFilesTotalRows = dataFrame.count()
    SummaryFile.testDetailFilesTotalRows = testDetailFilesTotalRows
    SummaryFile.testDetailTotalRowsInCockroachdb = testDetailFilesTotalRows
  }

  override def saveDistTotalCnt2SummaryFile(dataFrame: DataFrame): Unit = {
    val testDetailFilesTotalRowsDist = dataFrame.count()
    SummaryFile.testDetailFilesTotalRowsDist = testDetailFilesTotalRowsDist
    SummaryFile.testDetailTotalRowsDistInCockroachdb = testDetailFilesTotalRowsDist
  }

//  override def saveDB(dataFrame: DataFrame): Unit = {
//    println("======> save test detail data into cockroach database")
//    dataFrame.show(false)
//    configContext.cockroachDBIo.saveToCockroachdb(
//      dataFrame,
//      configContext.cockroachDbMasterTable,
//      configContext.sparkNumExcutors
//    )
//  }

//  override def saveFirstRowTime2SummaryFile(dataFrame: DataFrame): Unit = {
//    println("======> first row by masterProductScantimeDf")
//    if(dataFrame.count() > 0) {
//      val masterProductScantimeDfFirstTimeList = dataFrame
//        .select("scantime")
//        .withColumn("scantime2", $"scantime".cast(StringType))
//        .select("scantime2")
//        .orderBy($"scantime2".asc)
//        .limit(1)
//        .rdd
//        .map(r => r(0))
//        .collect
//        .toList
//        .asInstanceOf[List[String]]
//
//      println("======> masterProductScantimeDfFirstTimeList(0) : " + masterProductScantimeDfFirstTimeList(0))
//      SummaryFile.testDetailFirstRowTimeInCockroachdb = masterProductScantimeDfFirstTimeList(0)
//    } else {
//      SummaryFile.testDetailFirstRowTimeInCockroachdb = "N/A"
//    }
//  }
//
//  override def saveLastRowTime2SummaryFile(dataFrame: DataFrame): Unit = {
//    println("======> last row by masterProductScantimeDf")
//    if(dataFrame.count() > 0) {
//      val masterProductScantimeDfLastTimeList = dataFrame
//        .select("scantime")
//        .withColumn("scantime2", $"scantime".cast(StringType))
//        .select("scantime2")
//        .orderBy($"scantime2".desc)
//        .limit(1)
//        .rdd
//        .map(r => r(0))
//        .collect
//        .toList
//        .asInstanceOf[List[String]]
//
//      println("======> masterProductScantimeDfLastTimeList(0) : " + masterProductScantimeDfLastTimeList(0))
//      SummaryFile.testDetailLastRowTimeInCockroachdb = masterProductScantimeDfLastTimeList(0)
//    } else {
//      SummaryFile.testDetailLastRowTimeInCockroachdb = "N/A"
//    }
//  }
}
