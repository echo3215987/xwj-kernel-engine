package com.foxconn.iisd.bd.rca

import org.apache.spark.sql.DataFrame

abstract class BaseDataSource() {

  // read Context object
  def init()

  // read Data
  def fetchMasterDataDf(): DataFrame
  def fetchDetailDataDf(): DataFrame
  def fetchTestDataDf(): DataFrame

}
