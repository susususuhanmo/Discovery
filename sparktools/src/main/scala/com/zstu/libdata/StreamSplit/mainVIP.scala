package com.zstu.libdata.StreamSplit

import com.zstu.libdata.StreamSplit.function.getData.{getForSplitRdd, getRightRddAndReportError, readSourceRdd}
import com.zstu.libdata.StreamSplit.function.newDataOps.dealNewData0623
import com.zstu.libdata.StreamSplit.function._
import org.apache.spark.sql.hive.HiveContext

//import com.zstu.libdata.StreamSplit.function.oldDataOps.dealOldData
import com.zstu.libdata.StreamSplit.kafka.commonClean
import com.zstu.libdata.StreamSplit.splitAuthor.getCLC.addCLCName
import com.zstu.libdata.StreamSplit.function.printLog.logUtil

/**
  * Created by Administrator on 2017/6/24 0024.
  */
object mainVIP {

  def main(hiveContext: HiveContext): Unit = {

    val types = 4
    var (clcRdd, simplifiedJournalRdd)
    = readSourceRdd(hiveContext)


    logUtil("数据读取完成")
    logUtil("clc" + clcRdd.count())
    logUtil("simpified" + simplifiedJournalRdd.count())

    //    while(true)
    //      {
    try {


      val orgjournaldata = commonClean.readDataOrg("t_VIP_UPDATE", hiveContext)
        .filter("status != 2 and status != 3").limit(1000000).cache()
      orgjournaldata.registerTempTable("t_orgjournaldataVIP")


      val fullInputData = addCLCName(getData.getFullDataVIPsql(hiveContext), clcRdd, hiveContext)


      val (simplifiedInputRdd, repeatedRdd) =
        distinctRdd.distinctInputRdd(orgjournaldata.map(f => commonClean.transformRdd_vip_simplify(f)))





      // val simplifiedInputRdd =getSimplifiedInputRdd(CNKIData)
      logUtil("简化后的数据" + simplifiedInputRdd.count())
      val forSplitRdd = getForSplitRdd(fullInputData)
      logUtil("待拆分的数据" + forSplitRdd.count())

      //过滤出正常数据并将错误数据反馈
      val (rightInputRdd, errorRdd) = getRightRddAndReportError(simplifiedInputRdd, hiveContext)
      logUtil("正常数据" + rightInputRdd.count())




      //开始查重 join group
      val inputJoinJournalRdd = rightInputRdd.leftOuterJoin(simplifiedJournalRdd).map(f => (f._2._1._4, f._2))
      logUtil("join成功" + inputJoinJournalRdd.count())
      val joinedGroupedRdd = inputJoinJournalRdd.groupByKey()
      logUtil("group成功" + joinedGroupedRdd.count())


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
      //      val num = dealOldData(inputJoinJournalRdd, fullInputRdd, sourceCoreRdd
      //        , journalMagSourceRdd, simplifiedJournalRdd, types)
      val num = oldDataOps.dealOldData(fullInputData, types, inputJoinJournalRdd, hiveContext)
      logUtil("匹配成功的旧数据处理成功" + num)
      val logData = hiveContext.sql("select GUID as id," + types + " as resource from t_orgjournaldataVIP")
      WriteData.writeDataWangzhihong("t_Log3", logData)
      WriteData.writeErrorData(repeatedRdd, types, hiveContext)
      WriteData.writeErrorData(errorRdd, types, hiveContext)
      logUtil("重复数据写入" + repeatedRdd.count())
      logUtil("写入Log表" + logData.count())
      hiveContext.dropTempTable("t_orgjournaldataVIP")
    } catch {
      case ex: Exception => logUtil(ex.getMessage)
    }


  }
}
