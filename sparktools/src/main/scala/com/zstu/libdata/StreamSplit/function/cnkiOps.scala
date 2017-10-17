package com.zstu.libdata.StreamSplit.function

import com.zstu.libdata.StreamSplit.function.CommonTools.{hasNoChinese, splitStr}
/**
  * Created by xiangjh on 2017/4/2.
  */
object cnkiOps {

  /**
    * 截取指定长度的字符串
    *
    * @param str
    * @param length
    * @return
    */
  def cutStr(str: String, length: Int): String = {
    if (str == null) return null
    if (str.equals("null")) return null
    if (str.length >= length) {
      str.substring(0, length)
    } else str
  }


  def getFirstCreator(author: String): String = {
    if(author == null )null
    else {author.split(";")(0)}

  }

  /**
    * 清洗作者函数
    *
    * @return
    */
  def cleanAuthor(author: String): String = {
    if(hasNoChinese(author)) return author.replace("|!",";")

    var author_tmp = deleteInvisibleChar.deleteInvisibleChar(author.toString)
      .replace("@@","")
    author_tmp=  GetReplacedStr.GetReplacedStrOld(author_tmp)
    var creators: Array[String] = author_tmp.split("\\|!")
    if (creators.length ==1){
      creators  = author_tmp.split("\\;;")
    }
    var creator = ""
    for (i <- creators.indices) {
      val eachCreator = creators(i).trim()
      //把名字后面的方挂号去掉    於宁军[1,2]
      if(eachCreator ==null){
        creator += ""
      }else{
        val tmp: Array[String] = eachCreator.toString.split("\\[")
        var creator_temp = tmp(0)
        creator_temp = DeleteCharIfIsLastNum.DeleteCharIfIsLastNum(creator_temp)
        creator += creator_temp + ""
      }
    }
    //把末尾的“；”去掉
    //creator = creator.substring(0, creator.lastIndexOf(";"))
    creator
  }

  /**
    * 清洗标题字段
    *
    * @param title
    * @return
    */
  def cleanTitle(title: String): String = {
    if(title == null) null
    else {
      var value = deleteInvisibleChar.deleteInvisibleChar(title)
      //标准化处理 统一处理成大写
      value = value.toUpperCase
      value
    }
  }

  /**
    * 清洗机构字段
    *
    * @param institute
    * @return
    */
  def cleanInstituteOld(institute: String): String = {
    val institutions: Array[String] = deleteInvisibleChar.deleteInvisibleChar(institute).split("\\|!")
    var institution = ""
    for (i <- institutions.indices) {
      val eachInstitution = institutions(i).trim()
      institution += eachInstitution + ";"
    }
    institution = institution.substring(0, institution.lastIndexOf(";"))
    institution
  }
  def cleanInstitute(institute: String): String ={
    def getStrBefore(str: String):String={
      if(str == null) null
      else {
        val rtn = str.replace("，",",").replace("|!",";")
        if(rtn.indexOf(",") >=0) rtn.substring(0,rtn.indexOf(","))
        else rtn
      }
    }
    if(institute == null) null
    else splitStr(institute).map(getStrBefore(_).trim).reduce(_+";"+_)
  }

  def getFirstInstitute(institute: String): String = {
    if(institute == null) null
    else {
      if(institute.split(";").length > 0) institute.split(";")(0)
      else null
    }
  }

  /**
    * 清洗关键字字段
    *
    * @return
    */
  def cleanKeyWord(keys: String): String = {
    val keyWords: Array[String] = deleteInvisibleChar.deleteInvisibleChar(keys).split("\\|!")
    var keyWord = ""
    for (i <- keyWords.indices) {
      val eachKeyWord = keyWords(i).trim()
      keyWord += eachKeyWord + ";"
    }
    keyWord = keyWord.substring(0, keyWord.lastIndexOf(";"))
    keyWord
  }

  /**
    * 获取中文期刊名
    *
    * @param journal
    * @return
    */
  def cleanJournal(journal: String): String = {
    var journals = journal
    if (journals == null) {
      return null
    }
      journals = deleteInvisibleChar.deleteInvisibleChar(journal)
      journals = GetReplacedStr.GetReplacedStr(journals)
      journals

  }
  /**
    * 获取中文期刊名
    *
    * @param journal
    * @return
    */
  def cleanUnJournal(journal: String): String = {
    val journals = deleteInvisibleChar.deleteInvisibleChar(journal)
    var journalstr = ""
    if(journals == null)
      journalstr =""
    else{
      val tmp :Array[String] = journals.toString.split("\\[")
      journalstr = tmp(0)

    }
    journalstr
  }


  def main(args: Array[String]): Unit = {
  //  2016年第0卷第1期 9-页,共1页
  println(cleanJournal("现代教育科学"))
  }



}