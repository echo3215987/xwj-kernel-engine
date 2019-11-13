package com.foxconn.iisd.bd.rca

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame

/*
 *
 *
 * @author JasonLai
 * @date 2019/9/19 上午9:47
 * @param
 * @return
 * @description 針對檔案形式來源的資料進行存取操作
 */
@SerialVersionUID(114L)
class FileSource(configContext: ConfigContext) extends BaseDataSource with Serializable {

  var wipPath = ""
  var wipPartsPath = ""
  var bobcatPath = ""
  var wipColumns = ""
  var wipPartsColumns = ""
  var bobcatColumns = ""
  var dataSeperator = ""
  var mbLimits = 0
  var job = new Job()
  val minioIo = new MinioIo(configContext)

  /*
   *
   *
   * @author JasonLai
   * @date 2019/9/19 上午9:39
   * @description 初始化File所需的參數
   */
  override def init(): Unit = {
    wipPath = configContext.wipPath
    wipPartsPath = configContext.wipPartsPath
    bobcatPath = configContext.bobcatPath
    wipColumns = configContext.wipColumns
    wipPartsColumns = configContext.wipPartsColumns
    bobcatColumns = configContext.bobcatColumns
    dataSeperator = configContext.dataSeperator
    mbLimits = configContext.mbLimits
    //use minio service
    setMinioS3()
    job = configContext.job
  }

  /*
   *
   *
   * @author JasonLai
   * @date 2019/9/19 上午9:40
   * @param []
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 從minio wip file取得master資料，並轉換成 spark dataframe
   */
  override def fetchMasterDataDf(): DataFrame = {
    println("======> master data from file")

    val wipDestPath = minioIo.flatMinioFiles(configContext.sparkSession,
      wipPath,
      (mbLimits * 1024 * 1024),
      getJobIdTime(0, 4).toString + getJobIdTime(4, 6).toString,
      getJobIdTime(6, 8).toString,
      getJobIdTime(8, 10).toString + getJobIdTime(10, 12).toString + getJobIdTime(12, 14).toString)

    minioIo.getDfFromPath(configContext.sparkSession, wipDestPath.toString, wipColumns, dataSeperator)
  }

  /*
   *
   *
   * @author JasonLai
   * @date 2019/9/19 上午9:42
   * @param []
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 從minio wipParts file取得detail資料，並轉換成 spark dataframe
   */
  override def fetchDetailDataDf(): DataFrame = {
    println("======> detail data from file")
    val wipPartsDestPath = minioIo.flatMinioFiles(configContext.sparkSession,
      wipPartsPath,
      configContext.residualCapacityDataSize,
      getJobIdTime(0, 4).toString + getJobIdTime(4, 6).toString,
      getJobIdTime(6, 8).toString,
      getJobIdTime(8, 10).toString + getJobIdTime(10, 12).toString + getJobIdTime(12, 14).toString)
    minioIo.getDfFromPath(configContext.sparkSession, wipPartsDestPath.toString, wipPartsColumns, dataSeperator)
  }

  /*
   *
   *
   * @author JasonLai
   * @date 2019/9/19 上午9:43
   * @param []
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 從minio bobcat file取得test資料，並轉換成 spark dataframe
   */
  override def fetchTestDataDf(): DataFrame = {
    println("======> test data from file")
    val bobcatDestPaths = minioIo.flatMinioFiles(configContext.sparkSession,
      bobcatPath,
      configContext.residualCapacityDataSize,
      getJobIdTime(0, 4).toString + getJobIdTime(4, 6).toString,
      getJobIdTime(6, 8).toString,
      getJobIdTime(8, 10).toString + getJobIdTime(10, 12).toString + getJobIdTime(12, 14).toString)
    println(s"======> bobcatDestPaths.toString : ${bobcatDestPaths.toString}")
    minioIo.getDfFromPath(configContext.sparkSession, bobcatDestPaths.toString, bobcatColumns, dataSeperator)
  }

  /*
   *
   *
   * @author JasonLai
   * @date 2019/9/19 上午9:46
   * @param []
   * @return void
   * @description sparkSession 設定minio s3參數
   */
  def setMinioS3(): Unit = {
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", configContext.minioEndpoint)
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", configContext.minioConnectionSslEnabled.toString)
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", configContext.minioAccessKey)
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", configContext.minioSecretKey)
  }

  def getJobIdTime(startIndex: Int, endIndex: Int): String = {
    this.job.jobId.split("-uuid-")(1).split("-")(0).slice(startIndex, endIndex)
  }
}
