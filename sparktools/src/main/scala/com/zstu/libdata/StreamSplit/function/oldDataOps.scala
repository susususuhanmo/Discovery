package com.zstu.libdata.StreamSplit.function

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
/**
  * Created by Administrator on 2017/6/16 0016.
  */
object oldDataOps {
//  def dealOldData(inputJoinJournalRdd: RDD[(String, ((String, String, String, String, String, String), Option[(String, String, String, String, String, String)]))],
//                  fullInputRdd: RDD[(String, (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String))],
//                  sourceCoreRdd: RDD[(String, String)],
//                  journalMagSourceRdd: RDD[(String, String)],
//                  simplifiedJournalRdd: RDD[(String, (String, String, String, String, String, String))],
//                  types: Int): Long = {
//    /** ****** 相似度大于90的处理开始 *******/
//    val rdd_kafka_result_match = inputJoinJournalRdd
//      .filter(f => commonOps.getDisMatchRecord(f._2)).groupByKey().map(f => commonOps.getHightestRecord(f))
//    //将disMatchResult入库
//    val rdd_kafka_matchdata_source = fullInputRdd.join(rdd_kafka_result_match).distinct()
//    val  rdd_kafka_matchdata_source_insert_map = rdd_kafka_matchdata_source.map(f => (f._2._1._12, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7, f._2._1._8, f._2._1._9, f._2._1._10, f._2._1._11, f._2._1._12, f._2._1._13, f._2._1._14, f._2._1._15,f._2._1._16,f._2._1._17,f._2._1._18, f._2._2)))
//    //logUtil("------------rdd_kafka_matchdata_source_insert_map：" + rdd_kafka_matchdata_source_insert_map.first().toString() + "---------------")
//    val rdd_kafka_matchdata_source_insert_map2 =  rdd_kafka_matchdata_source_insert_map.leftOuterJoin(sourceCoreRdd)
//    //logUtil("------------rdd_kafka_matchdata_source_insert_map2：" + rdd_kafka_matchdata_source_insert_map2.first().toString() + "---------------")
//    val rdd_kafka_matchdata_source_insert_map3 =  rdd_kafka_matchdata_source_insert_map2.leftOuterJoin(journalMagSourceRdd)
//    //logUtil("------------rdd_kafka_matchdata_source_insert_map3：" + rdd_kafka_matchdata_source_insert_map3.first().toString() + "---------------")
//    val rdd_kafka_matchdata_source_insert_final =rdd_kafka_matchdata_source_insert_map3.map(f => {
//      var iscore = 0
//      var journaldatasource = ""
//
//      if (f._2._1._2 == None)
//        iscore = 0
//      else
//        iscore = 1
//      if(f._2._2 == None)
//        journaldatasource = ""
//      else
//        journaldatasource =f._2._2.get
//      (f._2._1._1._1, f._2._1._1._2, f._2._1._1._3, f._2._1._1._4, f._2._1._1._5, f._2._1._1._6, f._2._1._1._7, f._2._1._1._8, f._2._1._1._9, f._2._1._1._10, f._2._1._1._11, f._2._1._1._12, f._2._1._1._13, f._2._1._1._14, iscore, 2, f._2._1._1._19,journaldatasource, f._2._1._1._15, f._2._1._1._16, f._2._1._1._17)
//    })
//    logUtil("------------rdd_kafka_matchdata_source_insert_final：" + rdd_kafka_matchdata_source_insert_final.first().toString() + "---------------")
//    rdd_kafka_matchdata_source_insert_final.foreach(f => {
//      var conn: Connection = null
//      var stmt: PreparedStatement = null
//        conn = DriverManager.getConnection(commonClean.sqlUrl, commonClean.userName, commonClean.passWord);
//        //插入数据库 方法1
//        val insertSQL = "insert into t_JournalLog(id,title,titleAlt,creator,creatorAlt,creatorAll,keyword,keywordAlt,institute,instituteAll,year,journal,issue,url,isCore,operater,operateTime,source,isCheck,otherId,volume,page,datasource,abstract,abstractAlt,classifications)" +
//          "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,getdate(),"+types+",0,?,?,?,?,?,?,?)"
//        stmt = conn.prepareStatement(insertSQL)
//        commonOps.setData(stmt, f, types)
//        //logUtil("------插入数据（存在）---" + f.toString())
//        if(f._1  != f._17) //id  == otherid 表示该数据重复，不保存
//          stmt.execute()
//        else{
//          val conn1 = DriverManager.getConnection(commonClean.orgdata_sqlUrl, commonClean.orgdata_userName, commonClean.orgdata_passWord);
//          val errorInsertSQL = "insert into t_Error (id,message,resource) values(\'"+f._1+"\',\'otherid == id\',\'"+types+"\')"
//          stmt = conn1.prepareStatement(errorInsertSQL)
//          stmt.execute()
//          //logUtil("-------该数据的id已经存在(存在)id == otherid---" + f._1)
//          //插入t_error表
//        }
//
//    })
//    /** ****** 相似度大于90的处理结束 *******/
//    1
//  }
case class journalCoreJudge(journalName: String,isCore: Int)

  case class noMatchData (idNoMatch: String,otherId:String)
  case class operateAndSource(operater:Int,source:Int)
  def dealOldData(fullInputData: DataFrame,
                       types: Int,
                      inputJoinJournalRdd: RDD[(String, ((String, String, String, String, String, String,String,String), Option[(String, String, String, String, String, String,String,String)]))],
                      hiveContext: HiveContext
                ): Int

  = {

    val rdd_kafka_result_notmatch: RDD[(String, String)] = inputJoinJournalRdd
      .filter(f => commonOps.getDisMatchRecord(f._2)).groupByKey().map(f => commonOps.getHightestRecord(f))

    if(rdd_kafka_result_notmatch.count() == 0) return 1

    printLog.logUtil("MatchRdd" + rdd_kafka_result_notmatch.count())
    val resultNomatchData = hiveContext.createDataFrame(rdd_kafka_result_notmatch.map(
      value => noMatchData(value._1,value._2)
    ))


    val noMatchFullData = fullInputData.join(resultNomatchData,
      fullInputData("id") === resultNomatchData("idNoMatch"))
      .drop("idNoMatch")
    printLog.logUtil("MatchRddfullData" + noMatchFullData.count())



    val operateSourceData = hiveContext.createDataFrame(Array(operateAndSource(2,types)))
    val resultData = noMatchFullData.join(operateSourceData).filter("id != otherid")


    WriteData.writeData50("DiscoveryV3", "t_JournalLog", resultData)
    resultData.count().toInt


  }
}
