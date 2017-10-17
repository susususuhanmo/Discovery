package com.zstu.libdata.StreamSplit.function


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Created by Administrator on 2017/4/19 0019.
  */
object CommonTools {
  /**
    *
    * @param str
    * @return
    */
  def getLength(str: String) : Int={
    if(isNull(str)) 0
    else str.length
  }
  def splitStr(str: String, mode: String = "cleaned", separator: String = ";"): Array[String] = {
    if (str == null) return null
    if (mode != "cleaned")
      ReplaceLastStr.ReplaceLastStr(
        str.trim.replace("；", separator)
          .replace(",", separator)
          .replace(";", separator)
          .replace("，", separator)
          .replace("|!", separator), separator)
        .split(separator)
        .map(trimStr)
    else
      str.split(separator).map(trimStr)
  }

  def trimStr(str: String): String = {
    if (str == null) str
    else str.trim
  }

  def isMeaningless(str: String): Boolean = {
    if (str == "" || str == null || str == "null" || str == "无" || str == "不详") true
    else false
  }

  def isNotNull(str: String): Boolean = {
    !isNull(str)
  }

  def isNull(str: String): Boolean = {
    if (str == "" || str == null || str == "null") true
    else false
  }
  def hasNoChinese(str: String): Boolean ={
    if(str == null) return false
    val str1 = str.trim()
    for(s <- str1){
      if(s.toInt >= 19968 && s.toInt <=19968+20901)
        return false
    }
    true
  }
  def cutStr(str: String, length: Int): String = {
    if (str == null) return null
    if (str.length >= length) {
      str.substring(0, length)
    } else str
  }

  def cutStrByChar(str: String, cut: String): String = {
    if (str == null) null
    else if (str.indexOf(cut) >= 0)
      str.substring(0, str.indexOf(cut))
    else str
  }

  def initSpark(appName: String): HiveContext = {
    val conf = new SparkConf().setAppName(appName)
      .set("spark.speculation", "true")
      .set("spark.default.parallelism", "200")
    val sc = new SparkContext(conf)
    new HiveContext(sc)
  }


  def newGuid(): String = {
    var guid = ""
    for (i <- 1 to 32) {
      val n = Math.floor(Math.random() * 16.0).toInt.toHexString
      guid += n.toUpperCase
      if (i == 8 || i == 16 || i == 12 || i == 20)
        guid += "-"
    }
    guid
  }

  def convertChineseNumAndLetter(str: String): String = {
    if (isNull(str)) null
    else {
      def convertChar(letter: Char) = (letter - 65248).toChar
      var rtn = ""
      for (c <- str) {
        if ((c >= 65296 && c <= 65305) || (c >= 65313 && c <= 65338)) rtn += convertChar(c)
        else rtn += c
      }
      rtn
    }
  }

  /**
    *
    * @param row row
    * @param rowName rowName
    * @return String
    */
  def getRowString(row:Row,rowName: String): String ={
    row.getString(row.fieldIndex(rowName))
  }

  def parseJSONRdd(rdd: RDD[String]) = {
    val inputRdd = rdd.map(str => {
      val jsonSome = JSON.parseFull(str)
      //将JSON格式字符串解读成Some
      val jsonAny = jsonSome.get
      jsonAny match {
        case jsonMap: Map[String, String] => (
          jsonMap("guid"), jsonMap("creator"), jsonMap("institute"),
          jsonMap("journal"), jsonMap("keyword"), jsonMap("subject"),
          jsonMap("title")
        )
      }
    }
    )
    inputRdd
  }

}
