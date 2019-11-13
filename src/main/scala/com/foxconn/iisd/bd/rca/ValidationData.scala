package com.foxconn.iisd.bd.rca

import com.foxconn.iisd.bd.rca.utils.SummaryFile

object ValidationData {

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/1 上午10:30
   * @param []
   * @return void
   * @description Job 狀態判別 - 驗證有無資料
   */
  def validateEmptyData(): Boolean = {
    var isEmptyData = false
    if(SummaryFile.masterFilesTotalRowsDist == 0 && SummaryFile.detailFilesTotalRowsDist == 0 && SummaryFile.testFilesTotalRowsDist == 0) {
      isEmptyData = true
    }
    isEmptyData
  }

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/1 上午10:33
   * @param []
   * @return void
   * @description 驗證已經寫入cockroachdb master detail test table的資料，並將資料塞入summary file
   */
  def validateCockroachDB(configContext: ConfigContext): Unit = {

    //create cockroachDB object
    val cockroachDBIo = new CockroachDBIo(configContext)

    //驗證資料是否都有寫進Cockroach Database
    val pushdownQueryPattern = "select count(%s) from %s where ke_flag = '%s'"

    val masterCockroachDbCnt = cockroachDBIo
      .getDfFromCockroachdb(configContext.sparkSession,pushdownQueryPattern.format("sn", configContext.cockroachDbMasterTable, configContext.flag))
      .first().getLong(0)

    val detailCockroachDbCnt = cockroachDBIo
      .getDfFromCockroachdb(configContext.sparkSession,pushdownQueryPattern.format("id", configContext.cockroachDbDetailTable, configContext.flag))
      .first().getLong(0)

    val testCockroachDbCnt = cockroachDBIo
      .getDfFromCockroachdb(configContext.sparkSession,pushdownQueryPattern.format("sn", configContext.cockroachDbTestTable, configContext.flag))
      .first().getLong(0)

    //summaryfile
    SummaryFile.masterTotalRowsValidateInCockroachdb = masterCockroachDbCnt
    SummaryFile.detailTotalRowsValidateInCockroachdb = detailCockroachDbCnt
    SummaryFile.testTotalRowsValidateInCockroachdb = testCockroachDbCnt
  }
}
