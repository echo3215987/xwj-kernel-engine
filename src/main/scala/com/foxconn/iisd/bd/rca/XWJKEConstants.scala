package com.foxconn.iisd.bd.rca

/*
 *
 *
 * @author JasonLai
 * @date 2019/9/19 上午9:52
 * @param
 * @return
 * @description Kernel Engine 程式中所有定值
 */
object KEConstants {

  //job
  val JOB_SUCCEEDED = "@Job_Result_200"
  val JOB_FAILED = "@Job_Result_300"
  val JOB_EMPTY_DATA = "@Job_Result_301"

  //kernel
  val NEED_TO_RECALCULATE_TAG = "!!!NOMASTER!!!"
  val NO_PRODUCT = "!!!NOPRODUCT!!!"

  //bu
  val IPBD_LX_MONITOR = "rca-ipbd-lx-monitor"

  //data processing
  val DATA_PROCESSING_NORMAL = "normal"
  val DATA_PROCESSING_DAILY = "daily"

  //MINIO FILE PATH
  val MINIO_BOBCAT_PATH = "Bobcat"
  val MINIO_WIP_PARTS_PATH = "R_WIP_PARTS"
  val MINIO_WIP_PATH = "R_WIP"

  //env
  val ENV_LOCAL = "local"
  val ENV_DEV = "dev"
  val ENV_PROD = "prod"

  //key
  val KEY_TEST_STATION_LIST_TYPE = "1"
  val KEY_PART_LIST_TYPE = "2"
  val KEY_VENDOR_DATE_CODE_LIST_TYPE = "3"

  //summaryfile
  val SUMMARYFILE_SUCCEEDED = "SUCCEEDED"
  val SUMMARYFILE_WARRING = "WARRING"
  val SUMMARYFILE_FAILED = "FAILED"

}
