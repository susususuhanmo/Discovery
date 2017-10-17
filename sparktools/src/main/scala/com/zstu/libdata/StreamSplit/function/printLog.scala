package com.zstu.libdata.StreamSplit.function

import java.io.PrintWriter

import org.apache.spark.sql.Row
import org.joda.time.DateTime

/**
 * Created by Administrator on 2017/4/7.
 */
object printLog {
  val logger = new PrintWriter("./DiscoveryInfo.txt")
  def logUtil(info: String): Unit = {
    if (logger != null) {
      logger.println(info +  DateTime.now +  "\r\n")
      logger.flush()
    }
  }
  def logUtil(info: Row): Unit = {
    if (logger != null) {
      logger.println(info + "\r\n")
      logger.flush()
    }
  }
  def logUtil(info: Any): Unit = {
    if (logger != null) {
      logger.println(info + "\r\n")
      logger.flush()
    }
  }
}
