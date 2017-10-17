package com.zstu.libdata.StreamSplit.kafka

import java.io.PrintWriter

import com.zstu.libdata.StreamSplit.KafkaDataClean.ParseCleanUtil
import com.zstu.libdata.StreamSplit.function.{CheckChinese, DeleteCharIfIsLastNum, GetReplacedStr, deleteInvisibleChar}


/**
  * Created by xiangjh on 2017/4/2.
  */
object cnkiClean {

  val logger2 = new PrintWriter("./Dataclean_cnkiClean.log")

  logUtil("------------开始运行 ---------------")

  def logUtil(info: String): Unit = {
    if (logger2 != null) {
      logger2.println(info + "\r\n")
      logger2.flush()
    }
  }
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
      return str.substring(0, length)
    } else str
  }


  def getFirstCreator(author: String): String = {
    if(author == null || author =="")
      return null

    var retstr = ""
    if(author !="" && author !=null){
      val firstCreator = author.split(";")(0)
      retstr = firstCreator
    }
    retstr
  }

  def filterEnglishStr(str:String):String ={
    var retstr = ""
    if(CheckChinese.CheckWholeEnglish(str) == true){
      retstr = str
    }
    retstr
  }

  def filterChineseAbStr(str:String):String ={
    var retstr = ""
    if(CheckChinese.CheckWholeEnglish(str) == false){
      retstr = str
    }
    retstr
  }

  def getEnglishAbstract(str:String):String ={
    var retstr = ""
    if(CheckChinese.CheckWholeEnglish(str) == true){
      retstr = str
    }
    retstr
  }

  def getChineseAbstract(str:String):String ={
    var retstr = ""
    if(CheckChinese.CheckWholeEnglish(str) == false){
      retstr = str
    }
    retstr
  }

  def cleanVipAuthor(author: String,printerflag:Boolean): String = {
    var creator = ""
    var author_tmp = author.toString


    val ischinese = CheckChinese.CheckChinese(author.toString())
    if(ischinese == 2){ //如果是中文
      author_tmp = deleteInvisibleChar.deleteInvisibleChar(author_tmp)
      author_tmp=  GetReplacedStr.GetReplacedStrOld(author_tmp)
    }

    if(author_tmp !=""){
      var creators: Array[String] = author_tmp.split("\\|!")
      if (creators.length ==1){
        creators  = author_tmp.split("\\;;")
      }
      if(printerflag == true)
        logUtil("------------creators：---------------")

      for (i <- 0 to creators.length - 1) {
        val eachCreator = creators(i).trim()
        //把名字后面的方挂号去掉    於宁军[1,2]
        if(eachCreator ==null){
          creator += ""
        }else{
          val tmp: Array[String] = (eachCreator.toString).split("\\[")
          var creator_temp = tmp(0)
          creator_temp = DeleteCharIfIsLastNum.DeleteCharIfIsLastNum(creator_temp) //把所有的非法字符删掉，包括数字
          //creator_temp = DeleteCharIfIsLastNum.DeleteCharIfIsLastNum(creator_temp)
          creator += creator_temp + ";"
        }
      }
    }
    //把末尾的“；”去掉
    if(printerflag == true)
      logUtil("------------GetReplacedStr：" + creator + "---------------")
    if(creator.lastIndexOf(";") > 0)
      creator = creator.substring(0, creator.lastIndexOf(";"))
    creator
  }

  /**
    * 清洗作者函数
    *
    * @return
    */
  def cleanAuthor(author: String): String = {
    if(author == null || author =="")
      return null

    var creator = ""
    var author_tmp = author.toString

    /*var printerflag  = false
    if(author_tmp =="[南京理工大学环境与生物工程学院南京210094]"){
      printerflag = true
    }*/

    val ischinese = CheckChinese.CheckChinese(author.toString())


    if(ischinese == 2){ //如果是中文

      author_tmp = deleteInvisibleChar.deleteInvisibleChar(author_tmp)
      author_tmp=  GetReplacedStr.GetReplacedStrOld(author_tmp)
    }else{

    }


    if(author_tmp !=""){
      var creators: Array[String] = author_tmp.split("\\|!")

      if (creators.length ==1){
        creators  = author_tmp.split("\\;;")

      }

      for (i <- 0 to creators.length - 1) {
        val eachCreator = creators(i).trim()

        //把名字后面的方挂号去掉    於宁军[1,2]
        if(eachCreator ==null){
          creator += ""
        }else{
          val tmp: Array[String] = (eachCreator.toString).split("\\[")
          var creator_temp = tmp(0)

          creator_temp = DeleteCharIfIsLastNum.DeleteCharIfIsLastNum(creator_temp) //把所有的非法字符删掉，包括数字
          //creator_temp = DeleteCharIfIsLastNum.DeleteCharIfIsLastNum(creator_temp)


          if(creator_temp.length()> 0) //如果="" ,就不需要添加;了
            creator += creator_temp + ";"
        }
      }
    }
    //把末尾的“；”去掉
    if( creator.lastIndexOf(";") > 0){


      creator = creator.substring(0, creator.lastIndexOf(";"))
    }

    creator
  }

  /**
    * 清洗标题字段
    *
    * @param title
    * @return
    */
  def cleanTitle(title: String): String = {
    var value = deleteInvisibleChar.deleteInvisibleChar(title)
    //标准化处理 统一处理成大写
    value = value.toUpperCase
    value
  }

  /**
    * 清洗机构字段
    *
    * @param institute
    * @return
    */
  def cleanInstitute(institute: String): String = {
    val institutions: Array[String] = deleteInvisibleChar.deleteInvisibleChar(institute).split("\\|!")
    var institution = ""
    for (i <- 0 to institutions.length - 1) {
      val eachInstitution = institutions(i).trim()
      institution += eachInstitution + ";"
    }
    institution = institution.substring(0, institution.lastIndexOf(";"))
    institution
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
    for (i <- 0 to keyWords.length - 1) {
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
    val ischinese = 2
    if(ischinese == 2){//中文
      journals = deleteInvisibleChar.deleteInvisibleChar(journal)

      journals = GetReplacedStr.GetReplacedStr(journals)
      if (journals == null) {
        return null
      }
    }
    journals
    /*val result = CheckChinese.CheckChinese(journals)
    if (result == 1) {
      return null
    } else {
      journals
    }*/
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
      val tmp :Array[String] = (journals.toString).split("\\[")
      journalstr = tmp(0)

    }
    journalstr
  }



  def main(args: Array[String]): Unit = {
  //  2016年第0卷第1期 9-页,共1页
//    val str = "2016年第8期 8-8页,共1页"
//    val array:Array[String]=ParseCleanUtil.cleanVipIssue(str);
//    for(i <- 0 to array.length-1)
//      println(array(i))
    println(cleanJournal("浙江大学学报：工学版" ))
  }



}