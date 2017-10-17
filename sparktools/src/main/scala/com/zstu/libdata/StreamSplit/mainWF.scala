package com.zstu.libdata.StreamSplit

import com.zstu.libdata.StreamSplit.function.getData.{getForSplitRdd, getRightRddAndReportError, readSourceRdd}
import com.zstu.libdata.StreamSplit.function.newDataOps.dealNewData0623
import com.zstu.libdata.StreamSplit.function._
import com.zstu.libdata.StreamSplit.kafka.commonClean
import com.zstu.libdata.StreamSplit.splitAuthor.getCLC.addCLCName
import com.zstu.libdata.StreamSplit.function.printLog.logUtil
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/6/24 0024.
  */
object mainWF {


  def main(hiveContext: HiveContext): Unit = {

    val types = 8
    var (clcRdd, simplifiedJournalRdd)
    = readSourceRdd(hiveContext)


    logUtil("数据读取完成")
    logUtil("clc" + clcRdd.count())
    logUtil("simpified" + simplifiedJournalRdd.count())

    //    while(true)
    //      {
    try {


      val orgjournaldata = commonClean.readDataOrg("t_WF_UPDATE", hiveContext)
        .filter("status != 2 and status != 3").limit(500000).cache()
      orgjournaldata.registerTempTable("t_orgjournaldataWF")


      logUtil("全部数据" + orgjournaldata.count())

      val (simplifiedInputRdd, repeatedRdd) =
        distinctRdd.distinctInputRdd(orgjournaldata.map(f => commonClean.transformRdd_wf_simplify(f)))
      logUtil("简化后的数据" + simplifiedInputRdd.count())


      WriteData.writeErrorData(repeatedRdd, types, hiveContext)
      logUtil("重复数据写入" + repeatedRdd.count())

      val fullInputData = addCLCName(getData.getFullDataWFsql(hiveContext), clcRdd, hiveContext)


      val forSplitRdd = getForSplitRdd(fullInputData)
      logUtil("待拆分的数据" + forSplitRdd.count())


      //过滤出正常数据并将错误数据反馈
      val (rightInputRdd, errorRdd) = getRightRddAndReportError(simplifiedInputRdd, hiveContext)
      logUtil("正常数据" + rightInputRdd.count())

      WriteData.writeErrorData(errorRdd, types, hiveContext)


      //开始查重 join group
      val inputJoinJournalRdd = rightInputRdd.leftOuterJoin(simplifiedJournalRdd).map(f => (f._2._1._4, f._2))
      logUtil("join成功" + inputJoinJournalRdd.count())


      //处理新数据 得到新的journal大表 和 新作者表
      val newDataCompleted =
        try {
          dealNewData0623(fullInputData, types, inputJoinJournalRdd, hiveContext)
        }
        catch {
          case e => {
            logUtil(e.getMessage)
            false
          }
        }
      if (newDataCompleted) logUtil("新数据处理成功获得新数据")
      else logUtil("新数据处理失败")


      //处理旧数据


      val num = oldDataOps.dealOldData(fullInputData, types, inputJoinJournalRdd, hiveContext)
      logUtil("匹配成功的旧数据处理成功" + num)
      val logData = hiveContext.sql("select GUID as id," + types + " as resource from t_orgjournaldataWF")
      WriteData.writeDataWangzhihong("t_Log3", logData)
      logUtil("写入Log表" + logData.count())
      hiveContext.dropTempTable("t_orgjournaldataWF")
    } catch {
      case ex: Exception => logUtil(ex.getMessage)
    }


  }
}
