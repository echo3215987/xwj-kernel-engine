package com.foxconn.iisd.bd.rca

import org.apache.spark.sql.DataFrame


abstract class BaseDataProcessing {

  /*
   *
   *
   * @author EchoLee
   * @date 2019/11/7 上午10:28
   * @param []
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 取得資料 , 可以切換資料來源file/database
   */

//  def readData(): DataFrame

  /*
   *
   *
   * @author JasonLai
   * @date 2019/10/2 下午2:21
   * @param [dataframe]
   * @return void
   * @description summary file 設定 檔案總筆數 以及 cockroachdb總筆數
   */
//  def saveTotalCnt2SummaryFile(dataframe: DataFrame)

  /*
   *
   *
   * @author JasonLai
   * @date 2019/10/2 下午2:21
   * @param [dataframe]
   * @return void
   * @description 資料轉換(產品間隔,時間格式,增加工廠,增加flag,增加寫入時間)
   */
//  def transformDataframe(dataframe: DataFrame): DataFrame

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/2 下午2:25
   * @param [dataframe]
   * @return void
   * @description summary file 設定 檔案總筆數(去重複) 以及 cockroachdb總筆數(去重複)
   */
//  def saveDistTotalCnt2SummaryFile(dataframe: DataFrame)

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/2 下午2:25
   * @param [dataframe]
   * @return void
   * @description 資料儲存至資料庫
   */
//  def saveDB(dataframe: DataFrame)

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/2 下午2:25
   * @param [dataframe]
   * @return void
   * @description summary file 設定 依照資料時間排序(正向)取得第一筆
   */
//  def saveFirstRowTime2SummaryFile(dataframe: DataFrame)

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/2 下午2:25
   * @param [dataframe]
   * @return void
   * @description summary file 設定 依照資料時間排序(反向)取得第一筆
   */
//  def saveLastRowTime2SummaryFile(dataframe: DataFrame)

 /*
  *
  *
  * @author EchoLee
  * @date 2019/11/8 下午 02:26
  * @param [dataFrame]
  * @return void
  * @description summary file 設定 檔案總筆數(去重複) 以及 cockroachdb總筆數
  */
  def saveTotalCnt2SummaryFile(dataFrame: DataFrame): Unit = {
    val testDetailFilesTotalRows = dataFrame.count()
    SummaryFile.testDetailFilesTotalRows = testDetailFilesTotalRows
    SummaryFile.testDetailTotalRowsInCockroachdb = testDetailFilesTotalRows
  }

/*
 *
 *
 * @author EchoLee
 * @date 2019/11/8 下午 02:27
 * @param [dataFrame]
 * @return void
 * @description summary file 設定 檔案總筆數(去重複) 以及 cockroachdb總筆數(去重複)
 */
  def saveDistTotalCnt2SummaryFile(dataFrame: DataFrame): Unit = {
    val testDetailFilesTotalRowsDist = dataFrame.distinct().count()
    SummaryFile.testDetailFilesTotalRowsDist = testDetailFilesTotalRowsDist
    SummaryFile.testDetailTotalRowsDistInCockroachdb = testDetailFilesTotalRowsDist
  }

  /*
   *
   *
   * @author EchoLee
   * @date 2019/11/8 下午 02:29
   * @param [dataFrame]
   * @return void
   * @description
   */
//  def saveDB(dataFrame: DataFrame): Unit = {
//    println("======> save test detail data into cockroach database")
//    dataFrame.show(false)
//    configContext.cockroachDBIo.saveToCockroachdb(
//      dataFrame,
//      configContext.cockroachDbMasterTable,
//      configContext.sparkNumExcutors
//    )
//  }

}
