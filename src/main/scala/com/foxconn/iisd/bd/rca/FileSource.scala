package com.foxconn.iisd.bd.rca

import org.apache.spark.sql.DataFrame

/*
 *
 *
 * @author EchoLee
 * @date 2019/11/7 上午9:47
 * @param
 * @return
 * @description 針對檔案形式來源的資料進行存取操作
 */
@SerialVersionUID(114L)
class FileSource(configContext: ConfigContext) extends BaseDataSource with Serializable {

  var testDetailPath = ""
  var woPath = ""
  var matPath = ""
  var testDetailColumns = ""
  var woColumns = ""
  var matColumns = ""
  var dataSeperator = ""
  var mbLimits = 0
  var job = new Job()
  val minioIo = new MinioIo(configContext)

  /*
   *
   *
   * @author EchoLee
   * @date 2019/11/7 上午9:39
   * @description 初始化File所需的參數
   */
  override def init(): Unit = {
    testDetailPath = configContext.testDetailPath
    woPath = configContext.woPath
    matPath = configContext.matPath
    testDetailColumns = configContext.testDetailColumns
    woColumns = configContext.woColumns
    matColumns = configContext.matColumns
    dataSeperator = configContext.dataSeperator
    mbLimits = configContext.mbLimits
    //use minio service
    setMinioS3()
    job = configContext.job
  }

  /*
   *
   *
   * @author EchoLee
   * @date 2019/11/7 上午9:40
   * @param []
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 從minio test detail file取得master資料，並轉換成 spark dataframe
   */
  override def fetchTestDetailDataDf(): DataFrame = {
    println("======> test detail data from file")

    val testDetailDestPath = minioIo.flatMinioFiles(configContext.sparkSession,
      testDetailPath,
      mbLimits * 1024 * 1024,
      getJobIdTime(0, 4).toString + getJobIdTime(4, 6).toString,
      getJobIdTime(6, 8).toString,
      getJobIdTime(8, 10).toString + getJobIdTime(10, 12).toString + getJobIdTime(12, 14).toString)

    minioIo.getDfFromPath(configContext.sparkSession, testDetailDestPath.toString, testDetailColumns, dataSeperator)
  }

  /*
   *
   *
   * @author EchoLee
   * @date 2019/11/7 上午9:42
   * @param []
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 從minio wo file取得detail資料，並轉換成 spark dataframe
   */
  override def fetchWoDataDf(): DataFrame = {
    println("======> wo data from file")
    val woDestPath = minioIo.flatMinioFiles(configContext.sparkSession,
      woPath,
      configContext.mbLimits * 1024 * 1024,
      getJobIdTime(0, 4).toString + getJobIdTime(4, 6).toString,
      getJobIdTime(6, 8).toString,
      getJobIdTime(8, 10).toString + getJobIdTime(10, 12).toString + getJobIdTime(12, 14).toString)
    minioIo.getDfFromPath(configContext.sparkSession, woDestPath.toString, woColumns, dataSeperator)
  }

  /*
   *
   *
   * @author JasonLai
   * @date 2019/9/19 上午9:43
   * @param []
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 從minio mat file取得test資料，並轉換成 spark dataframe
   */
  override def fetchMatDataDf(): DataFrame = {
    println("======> mat data from file")
    val matDestPaths = minioIo.flatMinioFiles(configContext.sparkSession,
      matPath,
      configContext.mbLimits * 1024 * 1024,
      getJobIdTime(0, 4).toString + getJobIdTime(4, 6).toString,
      getJobIdTime(6, 8).toString,
      getJobIdTime(8, 10).toString + getJobIdTime(10, 12).toString + getJobIdTime(12, 14).toString)
    println(s"======> matDestPaths.toString : ${matDestPaths.toString}")
    minioIo.getDfFromPath(configContext.sparkSession, matDestPaths.toString, matColumns, dataSeperator)
  }

  /*
   *
   *
   * @author EchoLee
   * @date 2019/11/7 上午9:46
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
