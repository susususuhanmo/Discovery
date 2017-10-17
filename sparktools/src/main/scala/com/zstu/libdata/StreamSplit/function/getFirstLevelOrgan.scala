package com.zstu.libdata.StreamSplit.function

/**
 * Created by Administrator on 2017/3/1.
 */
import organMap._
object getFirstLevelOrgan {


  val endKeywords = Array("大学", "学院", "公司", "小学", "中学", "杂志社", "医院", "场", "局",
    "所", "院", "室", "中心", "厂", "队", "站", "学校", "社", "馆", "台", "中", "学会", "书店",
    "网", "村", "市", "区", "集团", "政府", "矿", "技校", "党校", "中专", "银行", "支行", "协会","研究所","研究院")
  val replaceedKeywords = Array("者单位：","现在单位","中国：","原单位："
    ,"毕业学校",".",":","：","(",")","##"," ","1.")
  val cutKeywords = Array(",","/","、")
  val peferredKeywords =Array("医院","大学","学院","重点实验室","试验室","科学院","工程试验室","研究所","研究院",
    "集团","公司", "小学", "中学", "技校", "党校", "中专", "银行","杂志社","学校", "场", "局","出版社","报社",
    "所", "院", "室", "中心", "厂", "队", "站", "联合会","委员会", "社", "馆", "台", "中", "学会", "书店",
    "网",  "矿",  "支行", "协会")
  def checkTailWithKeywords(str: String,keywords: String) ={
    val length = keywords.length
    if(str.length< length) false
    else {
      val lastStr = str.substring(str.length - length, str.length)
      if (lastStr == keywords) true
      else false
    }
  }
  def cutWithKeywords(str: String,keywords: String): String = {
    if(str.indexOf(keywords) < 0) null
    else if (keywords == "," ||keywords == "/" ||keywords == "、"  )
      str.substring(0,str.indexOf(keywords))
    else str.substring(0,str.indexOf(keywords)) + keywords
  }
  def getFirstLevelOrgan(str: String): String ={
    getFirstLevelOrganCleaned(deleteInvisibleChar.deleteInvisibleChar(str))
  }
  def getFirstLevelOrganCleaned(str: String): String ={
    //todo 去除不可见字符
    if(str == null) return null
    val mapKeywords =OrganArray.hasKeywords(str)
    val strReplaced = if( mapKeywords!= null) OrganMap.changeOrgan(mapKeywords) else str
    var str1 = strReplaced

    for(keywords <- replaceedKeywords){

      str1 = str1.replace(keywords,"")
    }

    for(keywords <- cutKeywords){
      val cutStr = cutWithKeywords(str1,keywords)
      if( cutStr != null) {
        str1 = cutStr

      }
    }
    for(keywords <- peferredKeywords){
      val cutStr = cutWithKeywords(strReplaced,keywords)
      if( cutStr != null) {
        return cutStr
      }
    }
    for(keywords <- endKeywords){
      if(checkTailWithKeywords(str1,keywords)) return str1
    }

    strReplaced
  }


  def main(args: Array[String]) {
    println(getFirstLevelOrgan("桂州社联合会"))
  }




}
