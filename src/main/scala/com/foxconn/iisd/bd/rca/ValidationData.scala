package com.foxconn.iisd.bd.rca

object ValidationData {

  /*
   *  
   * 
   * @author EchoLee
   * @date 2019/11/7 上午10:30
   * @param []
   * @return void
   * @description Job 狀態判別 - 驗證有無資料
   */
  def validateEmptyData(): Boolean = {
    var isEmptyData = false
    if(SummaryFile.testDetailFilesTotalRowsDist == 0 && SummaryFile.woFilesTotalRowsDist == 0 && SummaryFile.matFilesTotalRowsDist == 0) {
      isEmptyData = true
    }
    isEmptyData
  }

  /*
   *  
   * 
   * @author EchoLee
   * @date 2019/11/7 上午10:33
   * @param []
   * @return void
   * @description 驗證已經寫入cockroachdb test_detail wo mat table的資料，並將資料塞入summary file
   */
  def validateCockroachDB(configContext: ConfigContext): Unit = {

    //create cockroachDB object
    val cockroachDBIo = new CockroachDBIo(configContext)

    //驗證資料是否都有寫進Cockroach Database
    val pushdownQueryPattern = "select count(%s) from %s where ke_flag = '%s'"

    val testDetailCockroachDbCnt = cockroachDBIo
      .getDfFromCockroachdb(configContext.sparkSession,pushdownQueryPattern.format("sn", configContext.cockroachDbTestDetailTable, configContext.flag))
      .first().getLong(0)

    val woCockroachDbCnt = cockroachDBIo
      .getDfFromCockroachdb(configContext.sparkSession,pushdownQueryPattern.format("wo", configContext.cockroachDbWoTable, configContext.flag))
      .first().getLong(0)

    val matCockroachDbCnt = cockroachDBIo
      .getDfFromCockroachdb(configContext.sparkSession,pushdownQueryPattern.format("component", configContext.cockroachDbMatTable, configContext.flag))
      .first().getLong(0)

    //summaryfile
    SummaryFile.testDetailTotalRowsValidateInCockroachdb = testDetailCockroachDbCnt
    SummaryFile.woTotalRowsValidateInCockroachdb = woCockroachDbCnt
    SummaryFile.matTotalRowsValidateInCockroachdb = matCockroachDbCnt
  }
}
