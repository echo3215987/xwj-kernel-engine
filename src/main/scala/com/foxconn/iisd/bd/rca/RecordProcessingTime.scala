package com.foxconn.iisd.bd.rca

import java.util.Date
import java.util.concurrent.TimeUnit

object RecordProcessingTime {

  /*
   *
   *
   * @author JasonLai
   * @date 2019/10/18 上午11:31
   * @param [configContext]
   * @return void
   * @description
   */
  def setStartPoint(configContext: ConfigContext): Unit = {
    configContext.processingStartTime = new Date().getTime()
  }

  /*
   *
   *
   * @author JasonLai
   * @date 2019/10/18 上午11:31
   * @param [configContext]
   * @return void
   * @description
   */
  def showRuntime(configContext: ConfigContext): Unit = {
    configContext.processingEndTime = new Date().getTime()

    val sec = TimeUnit.SECONDS.convert(configContext.processingEndTime - configContext.processingStartTime, TimeUnit.MILLISECONDS)
    val s = sec % 60
    val m = (sec/60) % 60
    val h = (sec/60/60) % 24

    println(s"======> processing runtime :  ${"%02d:%02d:%02d".format(h, m, s)}")
  }

}
