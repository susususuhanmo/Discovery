package com.zstu.libdata.StreamSplit.function

import com.zstu.libdata.StreamSplit.kafka.cnkiClean
import com.zstu.libdata.StreamSplit.splitAuthor.splitAuthorFunction.splitRdd
import AddJournalInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/6/16 0016.
  */
object newDataOps {

  case class journalCoreJudge(journalName: String, isCore: Int)

  case class noMatchData(idNoMatch: String)

  case class operateAndSource(operater: Int, source: Int)

  def dealNewData0623(fullInputData: DataFrame,
                      types: Int,
                      inputJoinJournalRdd: RDD[(String, ((String, String, String, String, String, String, String, String), Option[(String, String, String, String, String, String, String, String)]))],
                      hiveContext: HiveContext)

  = {

    printLog.logUtil("InputData" + fullInputData.count())

    //查重，获取匹配成功的id
    val matchedId = inputJoinJournalRdd.filter(f => commonOps.getDisMatchRecord(f._2)).map(value => (value._1, "")).distinct
    printLog.logUtil("matchCount" + matchedId.count())

    //获取新数据id，并转换为Dataframe
    val resultNotMatch = inputJoinJournalRdd.map(value => value._1)
      .distinct.map(value => (value, ""))
      .leftOuterJoin(matchedId).filter(value => value._2._2.orNull == null)
      .map(value => value._1)
    //    if (resultNotMatch.count() == 0) return authorRdd
    printLog.logUtil("noMatchRdd" + resultNotMatch.count())

    val resultNomatchData = hiveContext.createDataFrame(resultNotMatch.map(
      value => noMatchData(value)
    ))


    //join 输入数据获得完整的新数据
    val noMatchFullData = fullInputData.join(resultNomatchData,
      fullInputData("id") === resultNomatchData("idNoMatch"))
      .drop("idNoMatch")
    printLog.logUtil("noMatchRddfullData" + noMatchFullData.count())




    //为数据添加types和operate两列
    val operateSourceData = hiveContext.createDataFrame(Array(operateAndSource(1, types)))
    val resultData = noMatchFullData.join(operateSourceData).cache




//    WriteData.writeData50("DiscoveryV3", "t_JournalLog", resultData)
    printLog.logUtil("resultData" + resultData.count())
 true


    /** ****** 相似度小于90的处理结束 *******/
  }

}



