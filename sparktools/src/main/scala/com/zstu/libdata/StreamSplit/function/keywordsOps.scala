package com.zstu.libdata.StreamSplit.function

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.IndexedSeq

/**
 * Created by Administrator on 2017/4/17.
 */
object keywordsOps {

  case class keyword(paperId: String, year: String, keyword: String,
                     keywordAlt: String, subjectCode: String, subjectName: String)

  def getFormattedSubject(subject: String): String = {
    var rtn = CommonTools.convertChineseNumAndLetter(subject)
    if (CommonTools.isNull(rtn)) null
    else {
      rtn = if (rtn.last == ';') rtn.substring(0, rtn.length - 1)
      else rtn
      rtn = if(rtn(0) == '0') rtn.substring(1)
      else rtn
      if(CommonTools.isNull(rtn)) return null
      rtn
        .replace(" ", ";")
        .replace(",", ";")
        .replace("&", ";")
        .replace(":", ";")
        .replace(";;", ";")
        .replace("''", "")
        .replace(">", "")
        .replace("<", "")
        .toUpperCase

    }

  }

  def isNull(str: String) = {
    if (str == "" || str == null) true
    else false
  }


  def splitKeyword(value: (String, String, String, String, String, String)):
  IndexedSeq[(String, String, String, String, String, String)] = {
    val (id, year, keywords, keywordAlt, subectCode, subjectName) = value
    if (isNull(keywords) && isNull(keywordAlt)) {
      //中英文均为空 返回原值
      for (i <- 0 to 0) yield (subectCode, id, year, null, null, subjectName)
    }
    else
    if (isNull(keywords)) {
      //中文为空拆分英文
      val ENArray = keywordAlt.split(";")
      for (i <- ENArray.indices)
        yield (subectCode, id, year, null, ENArray(i), subjectName)
    }
    else if (isNull(keywordAlt)) {
      //英文为空拆分中文
      val CNArray = keywords.split(";")
      for (i <- CNArray.indices)
        yield (subectCode, id, year, CNArray(i), null, subjectName)
    }
    else {
      //都不为空为一一对应，全部拆分。若有不为一一对应的，返回原值
      val CNArray = keywords.split(";")
      val ENArray = keywordAlt.split(";")
      if (CNArray.length == ENArray.length)
        for (i <- CNArray.indices)
          yield (subectCode, id, year, CNArray(i), ENArray(i), subjectName)

      else {
        val CN = for (i <- CNArray.indices)
          yield (subectCode, id, year, CNArray(i), null, subjectName)
        val EN = for (i <- ENArray.indices)
          yield (subectCode, id, year, null, ENArray(i), subjectName)
        CN.union(EN)
      }

    }
  }

  def cutStr(str: String, length: Int): String = {
    if (str == null) return null
    if (str.length >= length) {
      return str.substring(0, length)
    } else str
  }

  def splitCode(value: (String, String, String, String, String)) = {
    val (id, code, year, keywords, keywordAlt) = value

    if (code == null) for (i <- 0 to 0) yield ("", (id, null, year, keywords, keywordAlt))
    else {
      val codes = code.split(";")
      for (i <- codes.indices) yield (cutStr(codes(i), 1), (id, codes(i), year, keywords, keywordAlt))
    }
  }

  def splitCodeNew(value: (String)) = {
    val code = value
    val codeId = CommonTools.newGuid()
    if (code == null) for (i <- 0 to 0) yield ("", (null, codeId))
    else {
      val codes = code.split(";")
      for (i <- codes.indices) yield (cutStr(codes(i), 1), (codes(i), code))
    }
  }

  def getBetterName(value1: (String, String, String, String, String, Int)
                    , value2: (String, String, String, String, String, Int)) = {
    if (value1._6 > value2._6) value1
    else value2
  }

  def keywordCat(value1: (String, String, String, String, String),
                 value2: (String, String, String, String, String)) = {
    val catCode = if (value1._4.indexOf(value2._4) >= 0) value1._4
    else if (value2._4.indexOf(value1._4) >= 0) value2._4
    else value1._4 + ";" + value2._4

    val catName = if (value1._5.indexOf(value2._5) >= 0) value1._5
    else if (value2._5.indexOf(value1._5) >= 0) value2._5
    else value1._5 + ";" + value2._5

    (value1._1, value1._2, value1._3, catCode, catName)
  }
def keywordCatNew(value1: (String,String),value2: (String,String)) ={
  val catCode = value1._1 + ";" + value2._1
  val catName = if(isNull(value1._2)) value2._2
    else if (isNull(value2._2)) value1._2
  else   if (value1._2.indexOf(value2._2) >= 0) value1._2
  else if (value2._2.indexOf(value1._2) >= 0) value2._2
  else value1._2 + ";" + value2._2
  (catCode,catName)
}

  def formatRdd(value: (String, ((String, String, String, String, String), Option[(String, String)]))) = {
    val (id, code, year, keywords, keywordAlt) = value._2._1
    val (clcCode, ccdName) = value._2._2.getOrElse((null, null))
    val codeLevel = if (clcCode == null) 0
    else if (code.indexOf(clcCode) < 0) 0
    else clcCode.length
    ((id, code), (year, keywords, keywordAlt, clcCode, ccdName, codeLevel))
  }

  def formatRddNew(value: (String, ((String, String), Option[(String, String)]))) = {
    val (code, codeId) = value._2._1
    val (clcCode, ccdName) = value._2._2.getOrElse((null, null))
    val codeLevel = if (clcCode == null|| isNull(ccdName)) -1
    else if (code.indexOf(clcCode) < 0) 0
    else clcCode.length
    ((code, codeId), (ccdName, codeLevel))
  }
  def formatKeyword(value:(String, ((String, String, String, String), Option[(String, String)])))
  = {

    val (code,name) = value._2._2.getOrElse((null,null))

    (value._2._1._1,value._2._1._2,value._2._1._3,value._2._1._4,code,name)
  }
    def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("notOneToOne").set("spark.speculation", "true").set("spark.default.parallelism", "200")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    hiveContext.udf.register("len", (str: String) => if (str == null) 0 else str.length)

    hiveContext.udf.register("left", (str: String, length: Int) => if (str.length < length) str.substring(0, str.length) else str.substring(0, length))


    /** 一、读取所需表 1、 distinct表 2、 clccd表 */
    val distinctData = ReadData.readDataV3("t_Distinct_Resource_Journal", hiveContext)

    val distinctRdd: RDD[(String, (String, String, String, String))] = distinctData.map(row => (
      getFormattedSubject(row.getString(row.fieldIndex("classifications"))),
      ( row.getString(row.fieldIndex("id")),
      row.getString(row.fieldIndex("year")),
      row.getString(row.fieldIndex("keywords")),
      row.getString(row.fieldIndex("keywordAlt")) )
      )).filter(value => !isNull(value._1) || !isNull(value._2._3) || !isNull(value._2._4))
    printLog.logUtil("distinctRdd读取成功" + distinctRdd.count())

    val keywordsNameDataSource = ReadData.readDataV3("CLCCCD", hiveContext)
      .select("CLC_CODE", "CCD_NAME")
    val keywordsNameRdd = keywordsNameDataSource.map(row => (
      cutStr(row.getString(row.fieldIndex("CLC_CODE")), 1),
      (row.getString(row.fieldIndex("CLC_CODE")),
        row.getString(row.fieldIndex("CCD_NAME")))
      ))
    printLog.logUtil("keywordsNameDataSource读取成功" + keywordsNameRdd.count())
    /** 二、两表Join 根据学科代码获得学科名称 */
    val codeRdd = distinctData.map(row => getFormattedSubject(row.getString(row.fieldIndex("classifications"))))
      .distinct()
    printLog.logUtil("codedistinct成功" + codeRdd.count())
    val spitedCodeRdd = codeRdd.flatMap(splitCodeNew)
    printLog.logUtil("code拆分成功" + spitedCodeRdd.count())
//    spitedCodeRdd.foreach(f =>printLog.logUtil(f._1 + f._2._1 + f._2._2))

    val joinedRdd: RDD[((String, String), (String, Int))] = spitedCodeRdd.leftOuterJoin(keywordsNameRdd).map(formatRddNew)
    // ((code, codeId), (ccdName, codeLevel))
    printLog.logUtil("join成功" + joinedRdd.count())
//    joinedRdd.foreach(f =>printLog.logUtil(f._1._1 + f._1._2 + f._2._1 + f._2._2))



    val codeNameRdd: RDD[(String, (String, String))]
    = joinedRdd.reduceByKey(
      (value1,value2) => if(value1._2 > value2._2) value1 else value2)
    .map(value=> (value._1._2,(value._1._1,value._2._1)))
    //(id,(code,name))


    printLog.logUtil("bettername成功" + codeNameRdd.count())
//    codeNameRdd.foreach(f =>printLog.logUtil(f._1 + f._2))

    val catedNameRdd = codeNameRdd.reduceByKey( keywordCatNew)
    .map(value =>( value._1,value._2))
//    (codeid,(name,code))
    printLog.logUtil("连接成功" + catedNameRdd.count())
//    catedNameRdd.foreach(f =>printLog.logUtil(f._1 + f._2))



//    (id, year, keywords, keywordAlt, subectCode, subjectName)
    val keywordRdd = distinctRdd.leftOuterJoin(catedNameRdd)
  .map(formatKeyword)
    printLog.logUtil("codeName处理完毕" + keywordRdd.count())

//    keywordRdd.foreach(value =>printLog.logUtil(value._1 + value._2 + value._3 + value._4+
//    value._5 + value._6))


//    printLog.logUtil("\n\n")

        /** 三、拆分关键字 */
        printLog.logUtil("split开始" +keywordRdd.count())
        val splitedRdd = keywordRdd.flatMap(splitKeyword)
//    splitedRdd.foreach(value =>printLog.logUtil(value._1 + value._2 + value._3 + value._4+
//         value._5 + value._6) )
        printLog.logUtil("splitwancheng" +splitedRdd.count() )

        /** 四、整理数据写入结果表 */

    //    case class keyword(paperId: String, year: String, keyword: String,
    //                       keywordAlt: String, subjectCode: String, subjectName: String)
        val resultData = hiveContext.createDataFrame(splitedRdd.map(
          value => keyword(
            cutStr(value._2,50), cutStr(value._3,50),
            cutStr(value._4,500),cutStr( value._5,500), cutStr(value._1,50), cutStr(value._6,500))
        ))
        printLog.logUtil("准备写入")
        WriteData.writeDataV3("t_Keyword", resultData)


  }

}
