package com.zstu.libdata.StreamSplit

import java.io.PrintWriter

import com.zstu.libdata.StreamSplit.function.CommonTools._
import com.zstu.libdata.StreamSplit.function.ReadData
import com.zstu.libdata.StreamSplit.mainAll.latestTaskComplete
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

/**
  * Created by SuHanmo on 2017/7/1.
  * AppName: mainAll
  * Function:
  * Input Table:
  * Output Table:
  */
object mainAll {

  val logger = new PrintWriter("./allDiscovery.txt")
  var day = 1
  var finishedCNKI = false
  var finishedVIP = false
  var finishedWF = false
  var dayCount = 0
  var todayRun = Array("VIP", "WF", "CNKI")


  def main(args: Array[String]) {
    val hiveContext = initSpark("mainALL")
            run("CNKI",hiveContext)
    //    run("VIP",hiveContext)
    //    run("WF",hiveContext)


//    while (true) {
//
//
//      val runSource = refreshDate(hiveContext)
//      if (runSource != null) {
//        while (DateTime.now().hourOfDay().get() != 15) {
//          Thread.sleep(1000 * 60 * 59)
//        }
//        if(latestTaskComplete(hiveContext)){
//          run(runSource, hiveContext)
//        }else{
//          //如果前一天任务未完成，则倒回一天，明天继续执行
//          if(dayCount == 0) dayCount =2 else dayCount -= 1
//        }
//
//
//      }
//    }
  }

  def latestTaskComplete(hiveContext: HiveContext): Boolean = {
    val uncheckNum = ReadData.readDataLog("tmp_ExpertLog", hiveContext).filter("isCheck = 0").count()
    if (uncheckNum == 0) {
      logger.println("Latest Task has been Completed")
      logger.flush()
      true
    }
    else {
      logger.println("Latest Task has not been Completed")
      logger.flush()
      false
    }
  }

  def refreshDate(hiveContext: HiveContext): String = {
    val today = DateTime.now().dayOfWeek().get()
    if (today != day) {

        logger.println("DATE CHANGED!")
        logger.flush()
        //若到日期发生改变 改变count（0,1,2）循环
        if (dayCount == 2) dayCount = 0 else dayCount += 1
        //更新日期为今日
        day = today
        //返回应运行的数据源
        todayRun(dayCount)


    }
    //若日期没发生改变，则返回null不运行程序
    else {
      Thread.sleep(1000 * 60 * 60)
      null
    }
  }


  def run(source: String, hiveContext: HiveContext) = {
    logger.println("its time to run " + source + "!!!" + DateTime.now + "\r\n")
    logger.flush()
    try {
      source match {
        case "VIP" =>
          mainVIP.main(hiveContext: HiveContext)
          finishedVIP = true
        case "WF" =>
          mainWF.main(hiveContext: HiveContext)
          finishedWF = true
        case "CNKI" =>
          mainCNKI.main(hiveContext: HiveContext)
          finishedCNKI = true
        case _ =>
          logger.println("error:uncorrect source name!" + DateTime.now + "\r\n")
          logger.flush()
      }

    } catch {
      case ex: Exception =>
        logger.println("exception" + ex.getMessage + "\ntime:" + DateTime.now + "\r\n")
        logger.flush()
    }
  }

}
