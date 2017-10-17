package com.zstu.libdata.StreamSplit.splitAuthor

import com.zstu.libdata.StreamSplit.function.ReadData
import com.zstu.libdata.StreamSplit.function.keywordsOps.{formatRddNew, getFormattedSubject, keywordCatNew, splitCodeNew}
import com.zstu.libdata.StreamSplit.function.CommonTools.cutStr


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/6/1 0001.
  * 函数：
  * 1、addCLCName 为输入DataFrame添加subjects。需要通过getCLCRdd获得的CLCRdd
  * 2、getCLCRdd  获得CLCRdd
  */
object getCLC {

  case class codeIdCodeName(codeId: String, code: String, subject: String)

  /**
    * 程序通过CLCCCD对应RDD，为输入inputData表增加subject一列(classifications匹配到的中文名)
    *
    * @param inputData   输入DataFrame 要求有classifications的列
    * @param CLCRdd      CLC对应RDD 格式为  (code第一位,(code，name))
    *                    可从getCLCRdd获得
    * @param hiveContext hiveContext
    * @return 在原有作者表基础上新增subject列后的Dataframe
    */
  def addCLCName(inputData: DataFrame, CLCRdd: (RDD[(String, (String, String))]), hiveContext: HiveContext): DataFrame = {

    //从原始数据读取学科代码，清洗并去重。等待匹配
    val codeRdd: RDD[String] = inputData.map(row => getFormattedSubject(row.getString(row.fieldIndex("classifications"))))
      .distinct()
//    codeRdd.foreach(logUtil)

    //将待匹配学科代码拆分成单个代码（code第一位，（code，codeId））
    //这里选用差分钱的多个代码作为唯一Id，若code为null则code为生成的newGuid
    val spitedCodeRdd: RDD[(String, (String, String))] = codeRdd.flatMap(splitCodeNew)
//    spitedCodeRdd.foreach(value =>logUtil(value.toString()))


    //根据code第一位进行Join 并计算匹配度codeLevel。 格式调整为
    // ((code, codeId), (ccdName, codeLevel))
    val joinedRddNew: RDD[((String, String), (String, Int))] = spitedCodeRdd.leftOuterJoin(CLCRdd).map(formatRddNew)
//    joinedRddNew.foreach(value =>logUtil(value.toString()))

    //根据匹配程度进行reduce得到最佳学科名称
    //（codeId，（code,name））
    val codeNameRdd = joinedRddNew
      .reduceByKey((value1, value2) => if (value1._2 > value2._2) value1 else value2)
      .map(value => (value._1._2, (value._1._1, value._2._1)))

//    codeNameRdd.foreach(value =>logUtil(value.toString()))

    //将拆分后的code和name链接回去。此时codeId和code为顺序不同的，code与name为一一对应顺序。
    //    （codeId，code,name）
    val catedNameRdd = codeNameRdd.reduceByKey(keywordCatNew)
//    catedNameRdd.foreach(value =>logUtil(value.toString()))
    //创建DataFrame
    //    codeIdCodeName(codeId: String,code: String,subjects: String)   ps: subjects 为之前的name
    val catedNameData = hiveContext.createDataFrame(catedNameRdd
      .map(value => codeIdCodeName(value._1, value._2._1, value._2._2)))
//    catedNameData.foreach(logUtil)

    //根据classifications join回作者表，为作者表增加subjects列。
    val authorDataWithCLC = inputData.join(catedNameData,
      inputData("classifications") === catedNameData("codeId"), "left")
      .drop("classifications").drop("codeId").withColumnRenamed("code", "classifications")
//    authorDataWithCLC.foreach(logUtil)
    authorDataWithCLC
  }

  /**
    *
    * @param inputRdd RDD[(String, String, String, String, Any, Int, Any)]
    *                 oldAuthor 最后一位为Join得到的subjectid等数据
    *                 newAuthor 最后一位为新生成的 id
    * @param CLCRdd
    * @return
    */
  def addCLCRddOld(inputRdd :RDD[(String, String, String, String, Any, Int, Any)],CLCRdd: (RDD[(String, (String, String))]))
  : RDD[(String, String, String, String, Any, Int, Any)] ={
    def getAny5(any: Any):(String,String,String,String,String) = {
      def myToString(a:Any):String ={
        if (a == null)  null
        else a.toString
      }
      any match {
        case (a, b, c,d,e) => (myToString(a), myToString(b), myToString(c),myToString(d),myToString(e))
        case _ => (null,null,null,null,null)
      }
    }

//    (keywordNew,keywordAltNew,subjectNew,paperId,journal)
    val formattedInput = inputRdd.map(value =>
      (getAny5(value._5)._3,
      (value._1,value._2,value._3,value._4,value._5,value._6,value._7)))
    //从原始数据读取学科代码，清洗并去重。等待匹配
//    Any 4 (id,subject,keyword,keywordAlt)
    val codeRdd: RDD[String] = formattedInput.map(value => getFormattedSubject(value._1))
      .distinct()
    //    codeRdd.foreach(logUtil)

    //将待匹配学科代码拆分成单个代码（code第一位，（code，codeId））
    //这里选用差分钱的多个代码作为唯一Id，若code为null则code为生成的newGuid
    val spitedCodeRdd: RDD[(String, (String, String))] = codeRdd.flatMap(splitCodeNew)
    //    spitedCodeRdd.foreach(value =>logUtil(value.toString()))


    //根据code第一位进行Join 并计算匹配度codeLevel。 格式调整为
    // ((code, codeId), (ccdName, codeLevel))
    val joinedRddNew: RDD[((String, String), (String, Int))] = spitedCodeRdd.leftOuterJoin(CLCRdd).map(formatRddNew)
    //    joinedRddNew.foreach(value =>logUtil(value.toString()))

    //根据匹配程度进行reduce得到最佳学科名称
    //（codeId，（code,name））
    val codeNameRdd = joinedRddNew
      .reduceByKey((value1, value2) => if (value1._2 > value2._2) value1 else value2)
      .map(value => (value._1._2, (value._1._1, value._2._1)))

    //    codeNameRdd.foreach(value =>logUtil(value.toString()))

    //将拆分后的code和name链接回去。此时codeId和code为顺序不同的，code与name为一一对应顺序。
    //    （codeId，code,name）
    val catedNameRdd = codeNameRdd.reduceByKey(keywordCatNew)
    //    catedNameRdd.foreach(value =>logUtil(value.toString()))

   val resultRdd: RDD[(String, String, String, String, Any, Int, Any)]

   = formattedInput.leftOuterJoin(catedNameRdd)
    .map(value =>
      (value._2._1._1,value._2._1._2,value._2._1._3,value._2._1._4,
        (getAny5(value._2._1._5)._1,getAny5(value._2._1._5)._2,value._2._2.getOrElse((null,null))._2,getAny5(value._2._1._5)._4,getAny5(value._2._1._5)._5),
        value._2._1._6,value._2._1._7)
    )
    resultRdd



  }



  def addCLCRddNew(inputRdd :RDD[(String, String, String, String, Any, Int, String)],CLCRdd: (RDD[(String, (String, String))])) ={
    def getAny5(any: Any):(String,String,String,String,String) = {
      def myToString(a:Any):String ={
        if (a == null)  null
        else a.toString
      }
      any match {
        case (a, b, c,d,e) => (myToString(a), myToString(b), myToString(c),myToString(d),myToString(e))
        case _ => (null,null,null,null,null)
      }
    }

    //    (keywordNew,keywordAltNew,subjectNew,paperId,journal)
    val formattedInput = inputRdd.map(value =>
      (getAny5(value._5)._3,
        (value._1,value._2,value._3,value._4,value._5,value._6,value._7)))
    //从原始数据读取学科代码，清洗并去重。等待匹配
    //    Any 4 (id,subject,keyword,keywordAlt)
    val codeRdd: RDD[String] = formattedInput.map(value => getFormattedSubject(value._1))
      .distinct()
    //    codeRdd.foreach(logUtil)

    //将待匹配学科代码拆分成单个代码（code第一位，（code，codeId））
    //这里选用差分钱的多个代码作为唯一Id，若code为null则code为生成的newGuid
    val spitedCodeRdd: RDD[(String, (String, String))] = codeRdd.flatMap(splitCodeNew)
    //    spitedCodeRdd.foreach(value =>logUtil(value.toString()))


    //根据code第一位进行Join 并计算匹配度codeLevel。 格式调整为
    // ((code, codeId), (ccdName, codeLevel))
    val joinedRddNew: RDD[((String, String), (String, Int))] = spitedCodeRdd.leftOuterJoin(CLCRdd).map(formatRddNew)
    //    joinedRddNew.foreach(value =>logUtil(value.toString()))

    //根据匹配程度进行reduce得到最佳学科名称
    //（codeId，（code,name））
    val codeNameRdd = joinedRddNew
      .reduceByKey((value1, value2) => if (value1._2 > value2._2) value1 else value2)
      .map(value => (value._1._2, (value._1._1, value._2._1)))

    //    codeNameRdd.foreach(value =>logUtil(value.toString()))

    //将拆分后的code和name链接回去。此时codeId和code为顺序不同的，code与name为一一对应顺序。
    //    （codeId，code,name）
    val catedNameRdd = codeNameRdd.reduceByKey(keywordCatNew)
    //    catedNameRdd.foreach(value =>logUtil(value.toString()))

    val resultRdd: RDD[(String, String, String, String, Any, Int, String)]

    = formattedInput.leftOuterJoin(catedNameRdd)
      .map(value =>
        (value._2._1._1,value._2._1._2,value._2._1._3,value._2._1._4,
          (getAny5(value._2._1._5)._1,getAny5(value._2._1._5)._2,value._2._2.getOrElse((null,null))._2,getAny5(value._2._1._5)._4,getAny5(value._2._1._5)._5),
          value._2._1._6,value._2._1._7)
      )
    resultRdd



  }


  /**
    * 获取CLCCCD对应RDD。可用于addCLCName函数
    *
    * @param hiveContext hiveContext
    * @return
    */
  def getCLCRdd(hiveContext: HiveContext): RDD[(String, (String, String))] = {
    //读取clc对照表
    val keywordsNameDataSource = ReadData.readDataV3("CLCCCD", hiveContext)
      .select("CLC_CODE", "CCD_NAME")
    //调整表格式为（code第一位，（code，name））
    val keywordsNameRdd: RDD[(String, (String, String))] = keywordsNameDataSource.map(row => (
      cutStr(row.getString(row.fieldIndex("CLC_CODE")), 1),
      (row.getString(row.fieldIndex("CLC_CODE")),
        row.getString(row.fieldIndex("CCD_NAME")))
    ))
    keywordsNameRdd
  }
}
