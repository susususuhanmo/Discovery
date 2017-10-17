package com.zstu.libdata.StreamSplit.KafkaDataClean

import java.io.PrintWriter

import com.zstu.libdata.StreamSplit.function.{CheckChinese, deleteInvisibleChar}


/**
  * Created by xiangjh on 2017/3/15.
  * 本类主要用于清洗 vip的数据
  */
object ParseCleanUtil {

  val logger = new PrintWriter("./scala.log")

  def logUtil(info: String): Unit = {
    if (logger != null) {
      logger.println(info + "\r\n")
      logger.flush()
    }
  }

  /**
    * 其实部分清洗的步骤都是一样的，但是为了项目清晰可区分，
    * 因此没有用同一个函数，而是分成了多个函数
    */

  /**
    * 清洗作者函数
    *
    * @param key
    * @param author
    * @return
    */
  def cleanAuthor(f: (String, Any)): (String, String) = {
    //如果为空则直接返回
    val key = f._1
    if (f._2 == null)
      return (key, "")
    val author = f._2.toString()
    //    logUtil("=====开始清洗作者字段=====")
    try {
      val creators: Array[String] = deleteInvisibleChar.deleteInvisibleChar(author.toString).split("\\|!")
      var creator = ""
      for (i <- 0 to creators.length - 1) {
        val eachCreator = creators(i).trim()
        creator += eachCreator + ";"
      }
      //      logUtil("=====清理作者字段结束=====")
      (key, creator.substring(0, creator.length - 1))
    } catch {
      case ex: Exception => logUtil("清洗作者字段过程中出现错误，错误原因为:" + ex.getMessage)
        ("", "")
    }
  }

  /**
    * 清洗机构函数
    *
    * @param _1
    * @param _2
    * @return
    */
  def cleanInstitution(f: (String, Any)): (String, String) = {
    //同理，null值返回
    if (f._2 == null)
      return (f._1, "")
    //    logUtil("=====开始清洗机构字段=====")
    try {
      //去空分割
      val institutions: Array[String] = deleteInvisibleChar.deleteInvisibleChar(f._2.toString()).split("\\|!")
      var institution = ""
      for (i <- 0 to institutions.length - 1) {
        val eachInstitution = institutions(i).trim()
        institution += eachInstitution + ";"
      }
      //      logUtil("=====清理机构字段结束=====")
      (f._1, institution.substring(0, institution.length - 1))
    } catch {
      case ex: Exception => logUtil("清洗机构字段过程中出现错误，错误原因为:" + ex.getMessage)
        ("", "")
    }
  }

  /**
    * 清洗关键字字段
    *
    * @param _1
    * @param _2
    * @return
    */
  def cleanKeyWord(f: (String, Any)): (String, String) = {
    if (f._2 == null)
      return (f._1, "")
    //    logUtil("=====开始清洗关键字中文段=====")
    try {
      //去空分割
      val keyWords: Array[String] = deleteInvisibleChar.deleteInvisibleChar(f._2.toString()).split("\\|!")
      var keyWord = ""
      for (i <- 0 to keyWords.length - 1) {
        val eachKeyWord = keyWords(i).trim()
        keyWord += eachKeyWord + ";"
      }
      //      logUtil("=====清理关键字中文段结束=====")
      (f._1, keyWord.substring(0, keyWord.length - 1))
    } catch {
      case ex: Exception => logUtil("清洗关键字中文段过程中出现错误，错误原因为:" + ex.getMessage)
        ("", "")
    }
  }

  /**
    * 清晰标题字段
    *
    * @param _1
    * @param _2
    * @return
    */
  def cleanTitle(f: (String, Any)): (String, String) = {
    if (f._2 == null)
      return (f._1, "")
    try {
      //去掉不可见字符
      var value = deleteInvisibleChar.deleteInvisibleChar(f._2.toString())
      //标准化处理 统一处理成大写
      value = value.toUpperCase
      (f._1, value)
    } catch {
      case ex: Exception => logUtil("清洗关键字中文段过程中出现错误，错误原因为:" + ex.getMessage)
        ("", "")
    }
  }

  def cleanJournalName(f: (String, Any)): (String, String) = {
    if (f._2 == null)
      return (f._1, "")
    val result = CheckChinese.CheckChinese(f._2.toString())
    if (result == 1) {
      logUtil("=====第一期刊名非纯中文,舍弃=====")
      return null
    } else {
      (f._1, f._2.toString())
    }
  }

  /**
    * 用于最后的list清洗
    *
    * @param content
    * @return
    */
  def filterNullContent(content: String): Boolean = {
    if (content == null)
      return false
    try {
      ParseCleanUtil.logUtil("当前debug字串为:" + content)
      val str = content.replace("(", "").replace(")", "")
      if (str == "" || str == null)
        return false
      true
    } catch {
      case ex: Exception => logUtil("进行过滤操作出出现错误:" + ex.getMessage + "\r\n\r\n\r\n" + content + "\r\n\r\n\r\n")
        false
    }

  }

  /**
    * 获取key
    *
    * @param key
    * @return
    */
  def getKey(key: String): String = {
    if (key == null)
      return ""
    if (key.contains("(")) {
      key.substring(key.indexOf("(") + 1, key.indexOf(","))
    } else
      key.substring(0, key.indexOf(","))
  }

  /**
    * 获取value
    *
    * @param value
    * @return
    */
  def getValue(value: String): String = {
    if (value == null)
      return ""
    if (value.contains(")"))
      value.substring(value.indexOf(",") + 1, value.lastIndexOf(")"))
    else value.substring(value.indexOf(",") + 1, value.length)
  }

  /**
    * 清洗Cnki期刊号
    *
    * @param String
    * @param issue
    * @return
    */
  def cleanCnkiIssue(issue: String): Array[String] = {
    if (issue == null) return null
    //2016年第42卷第6期;28-31页,共4页
    val regex =
    """([0-9]{4})年第([0-9]+)卷第([0-9]+)期;([0-9]+)-([0-9]+)页,共([0-9]+)页""".r
    val regex(issue_year, issue_row, issue_term, issue_page_start, issue_page_end, issue_total) = issue
    val array: Array[String] = Array(issue_year, issue_row, issue_term, issue_page_start + "-" + issue_page_end, issue_total)
    array
  }


  /**
    * 清洗Wf期刊号
    *
    * @param String
    * @param issue
    * @return
    */
  def cleanWfIssue(issue: String): Array[String] = {
    var array: Array[String] = null
    if (issue == null) return null
    //2016, 26(8)
    val issues = issue.substring(issue.indexOf(" "), issue.indexOf("("))
    if (issues.length == 1) {
      val regex =
        """([0-9]{4}),.([\S]+).""".r
      val regex(issue_year, first) = issue
      array = Array(issue_year, first)
    }
    if (issues.length == 2) {
      val regex =
        """([0-9]{4}),.([\S]{1}).([\S]+).""".r
      val regex(issue_year, first, second) = issue
      array = Array(issue_year, first, second)
    }
    if (issues.length == 3) {
      val regex =
        """([0-9]{4}),.([\S]{2}).([\S]+).""".r
      val regex(issue_year, first, second) = issue
      array = Array(issue_year, first, second)
    }
    array
  }



  def cleanWfIssueNew(issue: String): Array[String] = {
    if(issue == null) null
    else {
      var year = ""
      var volum = ""
      var issues = ""
      if(issue.indexOf(',') <0 && issue.indexOf('（') <0)
        {
          year = issue
         volum =  ""
          issues = ""
        }
      else if(issue.indexOf(',') <0)
        {
          year = ""
          volum = issue.substring(0,issue.indexOf('('))
          issues = issue.substring(issue.indexOf('(') +1,issue.indexOf(')'))
        }
      else
      {
        year = issue.substring(0,issue.indexOf(','))
        volum = issue.substring(issue.indexOf(',') +1,issue.indexOf('('))
        issues = issue.substring(issue.indexOf('(') + 1 ,issue.indexOf(')'))
      }


      Array(year,volum,issues)
    }

  }


  /**
    * 清洗Vip期刊号
    *
    * @param String
    * @param issue
    * @return
    */
  def cleanVipIssue(issue: String): Array[String] = {
    if (issue == null) return null
    //2016年第42卷第6期;28-31页,共4页
    var isExists = false
    val str = issue.substring(issue.indexOf("-"), issue.indexOf("页"))
    if (str.length > 1) isExists = true
    if (issue.contains("卷")) {

        val regex =
          """([0-9]{4})年第([0-9]+)卷第([0-9A-Za-z]+)期 ([\S\s]+)页,共([\S\s]+)页""".r
        val regex(issue_year, issue_row, issue_term, issue_page_start, issue_total) = issue
        val array: Array[String] = Array(issue_year, issue_row, issue_term, issue_page_start, issue_total)
        array

    } else {

        val regex =
          """([0-9]{4})年第([0-9A-Za-z]+)期 ([\S\s]+)页,共([\S\s]+)页""".r
        val regex(issue_year, issue_term, issue_page_start, issue_total) = issue
        val array: Array[String] = Array(issue_year, issue_term, issue_page_start , issue_total).map(_.trim)
        array

    }
  }

  def otherWordClean(f: (String, Any)): (String, String) = {
    if (f._2 == null)
      return (f._1, "")
    (f._1, f._2.toString)
  }

  /**
    * 清洗wf的issue
    *
    * @param value
    * @return
    */
  def cleanWFIssue(value: String): Array[String] = {
    //2016, 37(1)
    val regex =
    """([0-9]+), ([0-9]+)(([0-9]+))""".r
    val regex(issue_year, issue_row, issue_term) = value
    val array: Array[String] = Array(issue_year, issue_row, issue_term)
    array
  }

  def main(args: Array[String]): Unit = {
    val a = cleanVipIssue("2017年第2卷第2期 160-164页,共-155页")
    a.foreach(println)
  }
}
