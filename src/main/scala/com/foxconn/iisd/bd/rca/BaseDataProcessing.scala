package com.foxconn.iisd.bd.rca

import org.apache.spark.sql.DataFrame

/*
 *  
 * 
 * @author JasonLai
 * @date 2019/9/27 上午10:02
 * @param 
 * @return 
 * @description
 */
abstract class BaseDataProcessing {

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/9/27 上午10:28
   * @param []
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 取得資料 , 可以切換資料來源file/database
   */
  def readData(): DataFrame

  /*
   *
   *
   * @author JasonLai
   * @date 2019/10/2 下午2:21
   * @param [dataframe]
   * @return void
   * @description summary file 設定 檔案總筆數 以及 cockroachdb總筆數
   */
  def saveTotalCnt2SummaryFile(dataframe: DataFrame)

  /*
   *
   *
   * @author JasonLai
   * @date 2019/10/2 下午2:21
   * @param [dataframe]
   * @return void
   * @description 資料轉換(產品間隔,時間格式,增加工廠,增加flag,增加寫入時間)
   */
  def transformDataframe(dataframe: DataFrame): DataFrame

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/2 下午2:25
   * @param [dataframe]
   * @return void
   * @description summary file 設定 檔案總筆數(去重複) 以及 cockroachdb總筆數(去重複)
   */
  def saveDistTotalCnt2SummaryFile(dataframe: DataFrame)

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/2 下午2:25
   * @param [dataframe]
   * @return void
   * @description 資料儲存至資料庫
   */
  def saveDB(dataframe: DataFrame)

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/2 下午2:25
   * @param [dataframe]
   * @return void
   * @description summary file 設定 依照資料時間排序(正向)取得第一筆
   */
  def saveFirstRowTime2SummaryFile(dataframe: DataFrame)

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/2 下午2:25
   * @param [dataframe]
   * @return void
   * @description summary file 設定 依照資料時間排序(反向)取得第一筆
   */
  def saveLastRowTime2SummaryFile(dataframe: DataFrame)

}
