package com.zstu.libdata.StreamSplit.kafka

import java.io.PrintWriter
import java.sql.{PreparedStatement, Types}
import java.text.SimpleDateFormat
import java.util.Date

import com.zstu.libdata.StreamSplit.KafkaDataClean.ParseCleanUtil
import com.zstu.libdata.StreamSplit.function.{cnkiOps, deleteZero, getData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.parsing.json.JSON

/**
  * Created by Yuli_Yan on 2017/4/17.
  */
object commonClean {

  val logger1 = new PrintWriter("./DataCleanCommon.log")

  logUtil("------------开始运行 ---------------")

  def logUtil(info: String): Unit = {
    if (logger1 != null) {
      logger1.println(info + "\r\n")
      logger1.flush()
    }
  }

  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var hehe = dateFormat.format( now )
    hehe
  }




  val orgdata_sqlUrl = "jdbc:sqlserver://192.168.1.165:1433;DatabaseName=WangZhihong;"
  val orgdata_userName = "ggm"
  val orgdata_passWord = "ggm@092011"


  val sqlUrl = "jdbc:sqlserver://192.168.1.50:1433;DatabaseName=DiscoveryV2;"
  val userName = "ggm"
  val passWord = "ggm@zju"


  /**
    * 通过hive 从数据库中读取数据
    *
    * @param tableName
    * @param hiveContext
    * @return
    */
  def readData(tableName: String, hiveContext: HiveContext): DataFrame = {
    val option = Map("url" -> sqlUrl, "user" -> userName, "password" -> passWord, "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data
  }

  def readDataOrg(tableName: String, hiveContext: HiveContext): DataFrame = {
    val option = Map("url" -> orgdata_sqlUrl, "user" -> orgdata_userName, "password" -> orgdata_passWord, "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data
  }

  /**
    * 将数据insert进数据库
    *
    * @param tableName
    * @param hiveContext
    * @param writeErrorRDD
    * @return
    */
  def insertData(tableName: String, hiveContext: HiveContext, writeErrorRDD: RDD[(String, String)]) = {
    val option = Map("url" -> sqlUrl, "user" -> "ggm", "password" -> "ggm@zju", "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data.registerTempTable("error")
    val errorData = hiveContext.createDataFrame(writeErrorRDD)
    errorData.registerTempTable("insertData")
    hiveContext.sql("insert into error select * from insertData")
  }

  def insertAllData2Log(tableName: String, hiveContext: HiveContext, writeErrorRDD: RDD[(String, Int,String,String)]) = {
    val  option = Map("url" -> orgdata_sqlUrl, "user" -> orgdata_userName, "password" -> orgdata_passWord, "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data.registerTempTable("alldatatable")
    val errorData = hiveContext.createDataFrame(writeErrorRDD)
    errorData.registerTempTable("insertData")
    hiveContext.sql("insert into alldatatable select * from insertData")
  }

  def insertDataOrgErrorLog(tableName: String, hiveContext: HiveContext, writeErrorRDD: RDD[(String,String, Int,String)]) = {
    val  option = Map("url" -> orgdata_sqlUrl, "user" -> orgdata_userName, "password" -> orgdata_passWord, "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data.registerTempTable("error")
    val errorData = hiveContext.createDataFrame(writeErrorRDD)
    errorData.registerTempTable("insertData")
    hiveContext.sql("insert into error select * from insertData")
  }

  def getJournalKay(title:String,journal:String): String ={
    val subTitle = cutStr(title, 6)
    val subJournal = cutStr(journal, 4)
    val key = subTitle + subJournal
    key
  }
  /**
    * hive中的数据进行转换
    *
    * @param r
    * @return
    */
  def transformRdd(r: Row) = {
    // value 里面的数据不应该进行截取
    val title = r.getString(r.fieldIndex("title")).toUpperCase
    val journal = r.getString(r.fieldIndex("journal"))
    val year = r.getString(r.fieldIndex("year"))
    val institute = r.getString(r.fieldIndex("instituteAll"))
    var subTitle = cutStr(title, 6)
    var subJournal = cutStr(journal, 4)
    var creator = r.getString(r.fieldIndex("creatorAll"))
    val id = r.getString(r.fieldIndex("id"))
    if (subTitle == null) subTitle = ""
    if (subJournal == null) subJournal = ""

    var creator_tmp=""
    var creator_tmp2=""
    val creaotr_org = creator
    try{
      creator_tmp= cnkiClean.cleanAuthor(creator)
      if( creator!="" && creator !=null) creator_tmp2 = cnkiClean.getFirstCreator(creator_tmp)
      creator = creator_tmp2
    }catch{
      case ex: Exception => logUtil("---transformRdd, clean author error，错误原因为---" + ex.getMessage)
        logUtil("------" +id+"------creator_org:"+creaotr_org+"------creator_tmp:"+creator_tmp+"-------creator_tmp2:"+ creator_tmp2+"------")
    }

    val key = subTitle + subJournal
    (key, (title, journal, creator, id,institute,year))
  }

  def transformRdd_cnki_simplify(r:Row) ={
    val id = r.getString(r.fieldIndex("GUID"))
    var creator =r.getString(r.fieldIndex("creator"))
    var title=r.getString(r.fieldIndex("title"))
    var journal =r.getString(r.fieldIndex("journal"))
    var journal2 =r.getString(r.fieldIndex("journal2"))
    journal = getData.getChineseJournal(journal,journal2)
    journal =cnkiOps.cleanJournal(journal)
    val year=r.getString(r.fieldIndex("year"))
    var institute =r.getString(r.fieldIndex("institute"))
    var issue = deleteZero.deleteZero(r.getString(r.fieldIndex("issue")))
    var volume = null
    if( title!="" && title !=null ) title = cnkiClean.cleanTitle(title.toUpperCase)
    if( journal !="" && journal !=null) journal = cnkiClean.cleanJournal(journal)
    if(institute !="" && institute !=null){institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(institute))}
    if( creator!="" && creator !=null) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(creator))
    val key = cutStr(title, 6) + cutStr(journal, 4)

    (key, (title, journal, creator, id,institute,year,issue,volume))
  }

  def transformRdd_cnki_source(r:Row) ={

    val id = r.getString(r.fieldIndex("GUID"))
    var title =r.getString(r.fieldIndex("title"))
    val titleAlt = ""
    var creator = r.getString(r.fieldIndex("creator"))
    var creatorAlt = r.getString(r.fieldIndex("creator_all"))
    var creatorAll = r.getString(r.fieldIndex("creator"))
    var keyWord = r.getString(r.fieldIndex("keyword"))
    var keyWordAlt =""
    var instituteAll = r.getString(r.fieldIndex("institute"))
    val year = r.getString(r.fieldIndex("year"))
    var journal =r.getString(r.fieldIndex("journal"))
    var institute =r.getString(r.fieldIndex("institute"))
    val issue = r.getString(r.fieldIndex("issue"))
    val url = r.getString(r.fieldIndex("url"))
    var abstractcont =  r.getString(r.fieldIndex("abstract"))
    var abstractcont_alt = r.getString(r.fieldIndex("abstract"))
    val page = r.getString(r.fieldIndex("page"))
    val subject = r.getString(r.fieldIndex("subject"))
    var doi = r.getString(r.fieldIndex("DOI"))
    if(doi == null) doi = ""
    var issn = r.getString(r.fieldIndex("ISSN"))
    if(issn == null) issn = ""

    if( title!="" && title !=null) title = cnkiClean.cleanTitle( title)
    if( creator!="" && creator !=null) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(creator))
    if(creatorAlt !="" && creatorAlt !=null) creatorAlt = cnkiClean.cleanAuthor(creatorAlt)
    if(creatorAll !="" && creatorAll !=null) creatorAll = cnkiClean.cleanAuthor(creatorAll)
    if(keyWord !="" && keyWord !=null) keyWord = cnkiClean.cleanKeyWord(keyWord)
    if(keyWordAlt !="" && keyWordAlt !=null) keyWordAlt = cnkiClean.cleanKeyWord(keyWordAlt)
    if( instituteAll!="" && instituteAll !=null) instituteAll = cnkiClean.cleanInstitute(instituteAll)
    if( journal!="" && journal !=null) journal = cnkiClean.cleanJournal(journal)
    if( institute!="" && institute !=null) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(institute))
    if(abstractcont !="" && abstractcont !=null) abstractcont = cnkiClean.getChineseAbstract(abstractcont)
    if(abstractcont_alt !="" && abstractcont_alt !=null) abstractcont_alt = cnkiClean.getEnglishAbstract(abstractcont_alt)
    //logUtil("---abstractcont-"+abstractcont)
    //logUtil("---abstractcont_alt-"+abstractcont_alt)

    (id, (id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url,abstractcont,abstractcont_alt,page,subject,doi,issn))
  }
  def transformRdd_vip_simplify(r:Row) ={
    val id = r.getString(r.fieldIndex("GUID"))
    var creator =r.getString(r.fieldIndex("creator_2"))
    var title=r.getString(r.fieldIndex("title"))
    var journal =r.getString(r.fieldIndex("journal_name"))
    var journal2 =r.getString(r.fieldIndex("journal_2"))
    val year=r.getString(r.fieldIndex("year"))
    var institute =r.getString(r.fieldIndex("institute_2"))
    var issue = r.getString(r.fieldIndex("issue"))

    var volume = deleteZero.deleteZero(getData.getVolumeVIP(issue))
    issue = deleteZero.deleteZero(getData.getIssueVIP(issue))
    //logUtil("------" +id)
    try{
      if( creator!="" && creator !=null) creator = cnkiClean.getFirstCreator(cnkiClean.cleanVipAuthor(creator,false))
    }catch{
      case ex: Exception => logUtil("---transformRdd_vip_simplify, clean author error，错误原因为---" + ex.getMessage)
        logUtil("------" +id+"-----"+creator)
    }

    if( title!="" && title !=null ) title = cnkiClean.cleanTitle(title.toUpperCase)
    if( journal !="" && journal !=null) journal = cnkiClean.cleanJournal(journal)
    if( journal2 !="" && journal2 !=null) journal2 = cnkiClean.cleanJournal(journal2)
    if (journal == null || journal.equals(""))     journal = journal2
    journal = cnkiOps.cleanJournal(journal)
    if(institute !="" && institute !=null)      institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(institute))
    val key = cutStr(title, 6) + cutStr(journal, 4)
    (key, (title, journal, creator, id,institute,year,issue,volume))
  }

  def transformRdd_vip_source(r:Row) ={
    val id = r.getString(r.fieldIndex("GUID"))
    var title =r.getString(r.fieldIndex("title"))
    val titleAlt = r.getString(r.fieldIndex("title_alt"))
    var creator = r.getString(r.fieldIndex("creator_2"))
    var creatorAlt = r.getString(r.fieldIndex("creator_all"))
    var creatorAll = r.getString(r.fieldIndex("creator_2"))
    if(creatorAll == null || creatorAll.equals("")){
      creatorAll = creator
    }
    var keyWord = r.getString(r.fieldIndex("keyword"))
    var keyWordAlt = r.getString(r.fieldIndex("keyword_alt"))
    var instituteAll = r.getString(r.fieldIndex("institute_2"))
    val year = r.getString(r.fieldIndex("year"))
    var journal =r.getString(r.fieldIndex("journal_name"))
    var journal2 =r.getString(r.fieldIndex("journal_2"))
    var institute =r.getString(r.fieldIndex("institute_2"))
    val issue = r.getString(r.fieldIndex("issue"))
    val url = r.getString(r.fieldIndex("url"))
    var abstractcont =  r.getString(r.fieldIndex("abstract"))
    var abstractcont_alt = r.getString(r.fieldIndex("abstract"))
    val page = ""
    val subject =r.getString(r.fieldIndex("subject_2"))
    var doi = ""
    val issn = ""

//    logUtil("------" +id)
//    var printauthorflag = false
//    if(id =="FAC024F6-C622-4CF2-87BA-3AB974C14BEE") {
//      printauthorflag = true
//      logUtil("------------FAC024F6-C622-4CF2-87BA-3AB974C14BEE：  " + printauthorflag + "---------------")
//    }


    try{
      if( creator!="" && creator !=null) creator = cnkiClean.getFirstCreator(cnkiClean.cleanVipAuthor(creator,false))
      if(creatorAlt !="" && creatorAlt !=null) creatorAlt = cnkiClean.cleanVipAuthor(creatorAlt,false)
      if(creatorAll !="" && creatorAll !=null) creatorAll = cnkiClean.cleanVipAuthor(creatorAll,false)
      //如果creatorAll是英文，则放到creatorAlt中
      if(cnkiClean.filterEnglishStr(creatorAll) !=""){
        creatorAlt = cnkiClean.filterEnglishStr(creatorAll)
        creatorAll = ""
      }
    }catch{
      case ex: Exception => logUtil("---transformRdd_vip_source. clean author error，错误原因为---" + ex.getMessage)
        logUtil("------" +id+"-----"+creator)
    }
    if( title!="" && title !=null) title = cnkiClean.cleanTitle( title)
    if(keyWord !="" && keyWord !=null) keyWord = cnkiClean.filterChineseAbStr(cnkiClean.cleanKeyWord(keyWord))
    if(keyWordAlt !="" && keyWordAlt !=null) keyWordAlt = cnkiClean.filterEnglishStr(cnkiClean.cleanKeyWord(keyWordAlt))
    if( instituteAll!="" && instituteAll !=null) instituteAll = cnkiClean.cleanInstitute(instituteAll)
    if( journal !="" && journal !=null) journal = cnkiClean.cleanJournal(journal)
    if( journal2 !="" && journal2 !=null) journal2 = cnkiClean.cleanJournal(journal2)
    if (journal == null || journal.equals(""))     journal = journal2
    journal = cnkiOps.cleanJournal(journal)
    if( institute!="" && institute !=null) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(institute))
    if(abstractcont !="" && abstractcont !=null) abstractcont = cnkiClean.getChineseAbstract(abstractcont)
    if(abstractcont_alt !="" && abstractcont_alt !=null) abstractcont_alt = cnkiClean.getEnglishAbstract(abstractcont_alt)

    (id, (id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url,abstractcont,abstractcont_alt,page,subject,doi,issn))
  }
  def transformRdd_wf_simplify(r:Row) ={
    val id = r.getString(r.fieldIndex("GUID"))
    var creator =r.getString(r.fieldIndex("creator"))
    var title=r.getString(r.fieldIndex("title"))
    var journal =r.getString(r.fieldIndex("journal_name"))
    var journal2 =r.getString(r.fieldIndex("journal_alt"))
    val year=r.getString(r.fieldIndex("year"))
    var institute =r.getString(r.fieldIndex("institute"))
    var issue = r.getString(r.fieldIndex("issue"))
    var volume = deleteZero.deleteZero(getData.getVolumeWF(issue))
    issue = deleteZero.deleteZero(getData.getIssueWF(issue))
    try{
      if( creator!="" && creator !=null) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(creator))
    }catch {
      case ex: Exception => logUtil("---transformRdd_wf_simplify. clean author error，错误原因为---" + ex.getMessage)
        logUtil("------" +id+"-----"+creator)
    }

    if( title!="" && title !=null ) title = cnkiClean.cleanTitle(title.toUpperCase)
    if( journal !="" && journal !=null) journal = cnkiClean.cleanJournal(journal)
    if( journal2 !="" && journal2 !=null) journal2 = cnkiClean.cleanJournal(journal2)
    if (journal == null || journal.equals(""))     journal = journal2
    if(institute !="" && institute !=null)      institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(institute))
    val key = cutStr(title, 6) + cutStr(journal, 4)
    (key, (title, journal, creator, id,institute,year,issue,volume))
  }

  def transformRdd_wf_source(r:Row) ={
    val id = r.getString(r.fieldIndex("GUID"))
    var title =r.getString(r.fieldIndex("title"))
    val titleAlt = r.getString(r.fieldIndex("title_alt"))
    var creator = r.getString(r.fieldIndex("creator"))
    var creatorAlt = r.getString(r.fieldIndex("creator_all"))
    var creatorAll = r.getString(r.fieldIndex("creator"))
    var keyWord = r.getString(r.fieldIndex("keyword"))
    var keyWordAlt = r.getString(r.fieldIndex("keyword_alt"))
    val year = r.getString(r.fieldIndex("year"))
    var journal =r.getString(r.fieldIndex("journal_name"))
    var journal2 =r.getString(r.fieldIndex("journal_alt"))
    var institute =r.getString(r.fieldIndex("institute"))
    var instituteAll = r.getString(r.fieldIndex("institute"))
    val issue = r.getString(r.fieldIndex("issue"))
    val url = r.getString(r.fieldIndex("url"))
    val abstractcont =  r.getString(r.fieldIndex("abstract"))
    val abstractcont_alt = r.getString(r.fieldIndex("abstract_alt"))
    val page = ""
    val subject =r.getString(r.fieldIndex("subject"))
    var doi = r.getString(r.fieldIndex("doi"))
    if(doi == null) doi = ""
    val issn = ""

    if( title!="" && title !=null) title = cnkiClean.cleanTitle( title)
    try{
      if( creator!="" && creator !=null) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(creator))
      if(creatorAlt !="" && creatorAlt !=null) creatorAlt = cnkiClean.cleanAuthor(creatorAlt)
      if(creatorAll !="" && creatorAll !=null) creatorAll = cnkiClean.cleanAuthor(creatorAll)
      if(creatorAll == null || creatorAll.equals("")){
        creatorAll = creator
      }
    }catch {
      case ex: Exception => logUtil("---transformRdd_wf_source. clean author error，错误原因为---" + ex.getMessage)
        logUtil("------" +id+"-----"+creator)
    }
    if(keyWord !="" && keyWord !=null) keyWord = cnkiClean.cleanKeyWord(keyWord)
    if(keyWordAlt !="" && keyWordAlt !=null) keyWordAlt = cnkiClean.cleanKeyWord(keyWordAlt)
    if( institute!="" && institute !=null) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(institute))
    if( instituteAll!="" && instituteAll !=null) instituteAll = cnkiClean.cleanInstitute(instituteAll)
    if( journal!="" && journal !=null) journal = cnkiClean.cleanUnJournal(journal)
    if( journal2!="" && journal2 !=null) journal2 = cnkiClean.cleanJournal(journal2)
    if (journal == null || journal.equals("") ) {
      journal = journal2
    }

    (id, (id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url,abstractcont,abstractcont_alt,page,subject,doi,issn))
  }

  /**
    * 作者表数据匹配
    *
    * @param f
    */
  def getMatchMap(f: Row) = {
    val id = f.getLong(f.fieldIndex("id")).toString
    val name = f.getString(f.fieldIndex("name")).toString
    val partOrgan = f.getString(f.fieldIndex("partOrgan")).toString
    val key = name + partOrgan
    (key, id)
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

  def filterInnerSameGuid(f1:(String, String, String, String, String,String),f2:(String, String, String, String, String,String)):Boolean = {
    var flag = false
    if(f1._4 == f2._4)
      flag = true
    flag
  }





  def StringCompLessThan(A:String,B:String):Boolean = {
    var flag = true
    if(A.compareToIgnoreCase(B) < 0)
      flag = true
    else
      flag = false
    flag
  }








  /**
    * 判断是否为核心期刊
    *
    * @param row
    * @return
    */
  def matchCoreJournal(row: (String, ((String, String, String), Option[String]))): Boolean = {
    if (row._2._2 == None) {
      //没有匹配到说明不是核心期刊,直接进行insert操作
      return false
    } else {
      //不为空，说明有匹配到的数据，是核心期刊
      true
    }
  }

  def sourceCoreRecord(row: (String, ((String, String, String), Option[String]))): Boolean = {
    if (row._2._2 == None) {
      //没有匹配到说明不是核心期刊,直接进行insert操作
      return true
    } else {
      //不为空，说明有匹配到的数据，是核心期刊
      false
    }
  }






  /**
    * 解析kafka的json数据
    *
    * @param jsonMap
    * @return
    */
  def parseVipJson(jsonMap: Map[String, Any]): (String, (String, String, String, String, String,String)) = {
    var creator = ""
    var title = ""
    var journal = ""
    var id = ""
    var institute = ""
    var journal2 = ""
    var year =""
    if (isUseful(jsonMap, "guid")) id = jsonMap("guid").toString
    if (isUseful(jsonMap, "creator_2")) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(jsonMap("creator_2").toString))
    if (isUseful(jsonMap, "title")) title = cnkiClean.cleanTitle(jsonMap("title").toString)
    if (isUseful(jsonMap, "journal_2")) journal = cnkiClean.cleanJournal(jsonMap("journal_2").toString)
    if (isUseful(jsonMap, "journal_name")) journal2 = cnkiClean.cleanJournal(jsonMap("journal_name").toString)
    if (isUseful(jsonMap, "year")) year =jsonMap("year").toString
    if (journal == null || journal.equals("")) {
      journal = journal2
    }
    if (isUseful(jsonMap, "institute_2")) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(jsonMap("institute_2").toString))
    val key = cutStr(title, 6) + cutStr(journal, 4)
    (key, (title, journal, creator, id, institute,year))
  }

  /**
    * 将原数据解析
    *
    * @param jsonMap
    */
  def parseVipSourceJson(jsonMap: Map[String, Any]) = {
    var creator = ""
    var creatorAll = ""
    var title = ""
    var journal = ""
    var id = ""
    var institute = ""
    var abstractcont = ""
    var abstractcont_alt  = ""
    var titleAlt = ""
    var creatorAlt = ""
    var keyWord = ""
    var keyWordAlt = ""
    var instituteAll = ""
    var year = ""
    var issue = ""
    var url = ""
    if (isUseful(jsonMap, "guid")) id = jsonMap("guid").toString
    if (isUseful(jsonMap, "title")) title = cnkiClean.cleanTitle(jsonMap("title").toString)
    if (isUseful(jsonMap, "titleAlt")) titleAlt = jsonMap("titleAlt").toString
    if (isUseful(jsonMap, "creator")) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(jsonMap("creator").toString))
    if (isUseful(jsonMap, "creator_all")) creatorAlt = cnkiClean.cleanAuthor(jsonMap("creator_all").toString)
    if (isUseful(jsonMap, "creator")) creatorAll = cnkiClean.cleanAuthor(jsonMap("creator").toString)
    if(creatorAll == null || creatorAll.equals("")){
      creatorAll = creator
    }
    if (isUseful(jsonMap, "keyword")) keyWord = cnkiClean.cleanKeyWord(jsonMap("keyword").toString)
    if (isUseful(jsonMap, "keyword_alt")) keyWordAlt = cnkiClean.cleanKeyWord(jsonMap("keyword_alt").toString)
    if (isUseful(jsonMap, "institute_2")) instituteAll = cnkiClean.cleanInstitute(jsonMap("institute_2").toString)
    if (isUseful(jsonMap, "year")) year = jsonMap("year").toString
    if (isUseful(jsonMap, "journal_2")) journal = cnkiClean.cleanUnJournal(jsonMap("journal_2").toString)
    if (isUseful(jsonMap, "institute_2")) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(jsonMap("institute_2").toString))
    if (isUseful(jsonMap, "issue")) issue = jsonMap("issue").toString
    if (isUseful(jsonMap, "url")) url = jsonMap("url").toString
    if (isUseful(jsonMap, "abstract")) abstractcont = jsonMap("abstract").toString
    if (isUseful(jsonMap, "abstract_alt")) abstractcont_alt = jsonMap("abstract_alt").toString
    (id, (id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url,abstractcont,abstractcont_alt))
  }

  /**
    * 解析kafka的json数据
    *
    * @param jsonMap
    * @return
    */
  def parseWfJson(jsonMap: Map[String, Any]): (String, (String, String, String, String, String,String)) = {
    var creator = ""
    var title = ""
    var journal = ""
    var id = ""
    var institute = ""
    var journal2 = ""
    var year =""
    if (isUseful(jsonMap, "guid")) id = jsonMap("guid").toString
    if (isUseful(jsonMap, "creator")) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(jsonMap("creator").toString))
    if (isUseful(jsonMap, "title")) title = cnkiClean.cleanTitle(jsonMap("title").toString)
    if (isUseful(jsonMap, "journal_name")) journal = cnkiClean.cleanJournal(jsonMap("journal_name").toString)
    if (isUseful(jsonMap, "journal_alt")) journal2 = cnkiClean.cleanJournal(jsonMap("journal_alt").toString)
    if (isUseful(jsonMap, "year")) year =jsonMap("year").toString
    if (journal == null || journal.equals("") ) {
      journal = journal2
    }
    if (isUseful(jsonMap, "institute")) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(jsonMap("institute").toString))
    val key = cutStr(title, 6) + cutStr(journal, 4)
    (key, (title, journal, creator, id, institute,year))
  }

  /**
    * 将原数据解析
    *
    * @param jsonMap
    */
  def parseWfSourceJson(jsonMap: Map[String, Any]) = {
    var creator = ""
    var creatorAll = ""
    var title = ""
    var journal = ""
    var id = ""
    var institute = ""
    var abstractcont = ""
    var abstractcont_alt = ""
    var titleAlt = ""
    var creatorAlt = ""
    var keyWord = ""
    var keyWordAlt = ""
    var instituteAll = ""
    var year = ""
    var issue = ""
    var url = ""
    var journal2 = ""
    if (isUseful(jsonMap, "guid")) id = jsonMap("guid").toString
    if (isUseful(jsonMap, "title")) title = cnkiClean.cleanTitle(jsonMap("title").toString)
    if (isUseful(jsonMap, "title_alt")) titleAlt = jsonMap("title_alt").toString
    if (isUseful(jsonMap, "creator")) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(jsonMap("creator").toString))
    if (isUseful(jsonMap, "creator_all")) creatorAlt = cnkiClean.cleanAuthor(jsonMap("creator_all").toString)
    if (isUseful(jsonMap, "creator")) creatorAll = cnkiClean.cleanAuthor(jsonMap("creator").toString)
    if(creatorAll == null || creatorAll.equals("")){
      creatorAll = creator
    }
    if (isUseful(jsonMap, "keyword")) keyWord = cnkiClean.cleanKeyWord(jsonMap("keyword").toString)
    if (isUseful(jsonMap, "keyword_alt")) keyWordAlt = cnkiClean.cleanKeyWord(jsonMap("keyword_alt").toString)
    if (isUseful(jsonMap, "year")) year = jsonMap("year").toString
    if (isUseful(jsonMap, "journal_name")) journal = cnkiClean.cleanUnJournal(jsonMap("journal_name").toString)
    if (isUseful(jsonMap, "journal_alt")) journal2 = cnkiClean.cleanJournal(jsonMap("journal_alt").toString)
    if (journal == null || journal.equals("") ) {
      journal = journal2
    }
    if (isUseful(jsonMap, "institute")) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(jsonMap("institute").toString))
    if (isUseful(jsonMap, "institute")) instituteAll = cnkiClean.cleanInstitute(jsonMap("institute").toString)
    if (isUseful(jsonMap, "issue")) issue = jsonMap("issue").toString
    if (isUseful(jsonMap, "url")) url = jsonMap("url").toString
    if (isUseful(jsonMap, "abstract")) abstractcont = jsonMap("abstract").toString
    if (isUseful(jsonMap, "abstract_alt")) abstractcont_alt = jsonMap("abstract_alt").toString
    (id, (id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url,abstractcont,abstractcont_alt))
  }

  /**
    * 清理 新作者数据
    *
    * @param f
    */
  def dealDate(f: (String, (String, Option[String]))): Boolean = {
    if (f._2._2 == None)
      true
    else false
  }

  /**
    * 清理 旧作者数据
    *
    * @param f
    */
  def dealOldData(f: (String, (String, Option[String]))): Boolean = {
    if (f._2._2 == None)
      false
    else true
  }

  /**
    * 判断集合中是否包含key字段字段，同时判断该字段是否为null
    *
    * @param jsonMap
    * @param key
    * @return
    */
  def isUseful(jsonMap: Map[String, Any], key: String): Boolean = {
    if (jsonMap.contains(key) && jsonMap(key) != null)
      true
    else false
  }

  /**
    * 将错误的记录过滤出来
    *
    * @return  true是错误数据  false 是正确数据
    */
  def filterErrorRecord(value: (String, String, String, String, String,String)): Boolean = {
    if (value == None) {
      //logUtil("------------filterErrorRecord1 --"+value+"-------------")
      return true
    }
    if (value._1 == null || value._1.equals("")) {
      //logUtil("------------filterErrorRecord2 --"+value+"-------------")
      return true
    }
    if (value._2 == null || value._2.equals("")){
      //logUtil("------------filterErrorRecord3 --"+value+"-------------")
      return true
    }
    false
  }

  /**
    * 获取格式正确的记录
    *
    * @param _2
    * @return
    */
  def filterTargetRecord(_2: (String, String, String, String, String,String)): Boolean = {
    if (_2 == None) {
      //logUtil("------------filterTargetRecord --"+value+"-------------")
      return false
    }
    if (_2._1 == null || _2._1.equals("")) return false
    if (_2._2 == null || _2._2.equals("")) return false
    true
  }
  def cleanIssue(issue: String,issuetype:Int): Array[String] = {
    var retarray:Array[String] = new Array[String](3)
    if (issuetype == 4) {
      //vip的issue
      val array: Array[String] = ParseCleanUtil.cleanVipIssue(issue)
      retarray= Array(array(2),array(1),array(3))
    }
    if (issuetype == 2) {
      //cnki的issue
      val array: Array[String] = ParseCleanUtil.cleanCnkiIssue(issue)
      retarray= Array(array(2),array(1),array(3))
    }
    if (issuetype == 8) {
      //wf的issue
      val array: Array[String] = ParseCleanUtil.cleanWfIssueNew(issue)
      retarray= Array( array(0),array(1),array(2))
    }
    retarray
  }
  /**
   * 设置数据库数据
   *
   * @param stmt
   * @param f
   */
  def setData4newdata(stmt: PreparedStatement, f: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int, Int, String,String,String,String,String,(String,String,String)), source: Int) = {
    //DataCleanForAll.logUtil("---当前的类型为:---"+source)
    if (insertJudge(f._1))
      stmt.setString(1, f._1)
    else stmt.setNull(1, Types.VARCHAR)
    if (insertJudge(f._2))
      stmt.setString(2, f._2)
    else stmt.setNull(2, Types.NVARCHAR)
    if (insertJudge(f._3))
      stmt.setString(3, f._3)
    else stmt.setNull(3, Types.NVARCHAR)
    if (insertJudge(f._4))
      stmt.setString(4, f._4)
    else stmt.setNull(4, Types.NVARCHAR)
    if (insertJudge(f._5))
      stmt.setString(5, f._5)
    else stmt.setNull(5, Types.NVARCHAR)
    if (insertJudge(f._6))
      stmt.setString(6, f._6)
    else stmt.setNull(6, Types.NVARCHAR)
    if (insertJudge(f._7))
      stmt.setString(7, f._7)
    else stmt.setNull(7, Types.NVARCHAR)
    if (insertJudge(f._8))
      stmt.setString(8, f._8)
    else stmt.setNull(8, Types.NVARCHAR)
    if (insertJudge(f._9))
      stmt.setString(9, f._9)
    else stmt.setNull(9, Types.NVARCHAR)
    if (insertJudge(f._10))
      stmt.setString(10, f._10)
    else stmt.setNull(10, Types.NVARCHAR)
    if (insertJudge(f._11))
      stmt.setString(11, f._11)
    else stmt.setNull(11, Types.VARCHAR)
    if (insertJudge(f._12))
      stmt.setString(12, f._12)
    else stmt.setNull(12, Types.NVARCHAR)
    if (insertJudge(f._13)) {
      if (source == 4) {
        //vip的issue
        val array: Array[String] = ParseCleanUtil.cleanVipIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(18, array(1))
        stmt.setString(19, array(3))
      }
      if (source == 2) {
        //cnki的issue
        /*val array: Array[String] = ParseCleanUtil.cleanCnkiIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(18, array(1))
        stmt.setString(19, array(3))*/
        stmt.setString(13, f._13)
        stmt.setNull(18, Types.NVARCHAR)
        if(insertJudge(f._21)){
          stmt.setString(19,f._21)
        }else{
          stmt.setNull(19, Types.NVARCHAR)
        }
      }
      if (source == 8) {
        //wf的issue
        val array: Array[String] = ParseCleanUtil.cleanWfIssue(f._13)
        stmt.setString(13, array(array.length - 1))
        stmt.setString(18, array(1))
        stmt.setNull(19, Types.VARCHAR)
      }
    }
    else stmt.setNull(13, Types.VARCHAR)
    if (insertJudge(f._14))
      stmt.setString(14, f._14)
    else stmt.setNull(14, Types.NVARCHAR)
    stmt.setInt(15, f._15)
    stmt.setInt(16, f._16)
    if (insertJudge(f._17)) //otherId
      stmt.setString(17, f._17)
    else
      stmt.setNull(17, Types.VARCHAR)
    if (insertJudge(f._18))  //datasource
      stmt.setString(20, f._18)
    else stmt.setNull(20, Types.VARCHAR)
    if (insertJudge(f._19))  //abstract
      stmt.setString(21, f._19)
    else stmt.setNull(21, Types.VARCHAR)
    if (insertJudge(f._20))  //abstract_alt
      stmt.setString(22, f._20)
    else stmt.setNull(22, Types.VARCHAR)

    if (insertJudge(f._22._1))  //subject
      stmt.setString(23,f._22._1)
    else stmt.setNull(23, Types.VARCHAR)
    if (insertJudge(f._22._2))  //doi
      stmt.setString(24,f._22._2)
    else stmt.setNull(24, Types.VARCHAR)
    if (insertJudge(f._22._3))  //issn
      stmt.setString(25,f._22._3)
    else stmt.setNull(25, Types.VARCHAR)
  }


  def setData4newdata2Journal2016(stmt: PreparedStatement, f: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int, Int, String,String,String,String), source: Int): Unit = {
    //DataCleanForAll.logUtil("---当前的类型为:---"+source)
    if (insertJudge(f._1))
      stmt.setString(1, f._1)
    else stmt.setNull(1, Types.VARCHAR)
    if (insertJudge(f._2))
      stmt.setString(2, f._2)
    else stmt.setNull(2, Types.NVARCHAR)
    if (insertJudge(f._3))
      stmt.setString(3, f._3)
    else stmt.setNull(3, Types.NVARCHAR)
    if (insertJudge(f._4))
      stmt.setString(4, f._4)
    else stmt.setNull(4, Types.NVARCHAR)
    if (insertJudge(f._5))
      stmt.setString(5, f._5)
    else stmt.setNull(5, Types.NVARCHAR)
    if (insertJudge(f._6))
      stmt.setString(6, f._6)
    else stmt.setNull(6, Types.NVARCHAR)
    if (insertJudge(f._7))
      stmt.setString(7, f._7)
    else stmt.setNull(7, Types.NVARCHAR)
    if (insertJudge(f._8))
      stmt.setString(8, f._8)
    else stmt.setNull(8, Types.NVARCHAR)
    if (insertJudge(f._9))
      stmt.setString(9, f._9)
    else stmt.setNull(9, Types.NVARCHAR)
    if (insertJudge(f._10))
      stmt.setString(10, f._10)
    else stmt.setNull(10, Types.NVARCHAR)
    if (insertJudge(f._11))
      stmt.setString(11, f._11)
    else stmt.setNull(11, Types.VARCHAR)
    if (insertJudge(f._12))
      stmt.setString(12, f._12)
    else stmt.setNull(12, Types.NVARCHAR)
    if (insertJudge(f._13)) {
      if (source == 4) {
        //vip的issue
        val array: Array[String] = ParseCleanUtil.cleanVipIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(16, array(1))
        stmt.setString(17, array(3))
      }
      if (source == 2) {
        //cnki的issue
        val array: Array[String] = ParseCleanUtil.cleanCnkiIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(16, array(1))
        stmt.setString(17, array(3))
      }
      if (source == 8) {
        //wf的issue
        val array: Array[String] = ParseCleanUtil.cleanWfIssue(f._13)
        stmt.setString(13, array(array.length - 1))
        stmt.setString(16, array(1))
        stmt.setNull(17, Types.VARCHAR)
      }
    }
    else stmt.setNull(13, Types.VARCHAR)
    if (insertJudge(f._14))
      stmt.setString(14, f._14)
    else stmt.setNull(14, Types.NVARCHAR)
    stmt.setInt(15, f._15)

    if (insertJudge(f._19))  //abstract
      stmt.setString(21, f._19)
    else stmt.setNull(21, Types.VARCHAR)
    if (insertJudge(f._20))  //abstract_alt
      stmt.setString(22, f._20)
    else stmt.setNull(22, Types.VARCHAR)
  }

  /**
    * 设置数据库数据
    *
    * @param stmt
    * @param f
    */
  def setData(stmt: PreparedStatement, f: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int, Int, String,String,String,String,String,(String,String,String)), source: Int) = {
    //DataCleanForAll.logUtil("---当前的类型为:---"+source)
    if (insertJudge(f._1))
      stmt.setString(1, f._1)
    else stmt.setNull(1, Types.VARCHAR)
    if (insertJudge(f._2))
      stmt.setString(2, f._2)
    else stmt.setNull(2, Types.NVARCHAR)
    if (insertJudge(f._3))
      stmt.setString(3, f._3)
    else stmt.setNull(3, Types.NVARCHAR)
    if (insertJudge(f._4))
      stmt.setString(4, f._4)
    else stmt.setNull(4, Types.NVARCHAR)
    if (insertJudge(f._5))
      stmt.setString(5, f._5)
    else stmt.setNull(5, Types.NVARCHAR)
    if (insertJudge(f._6))
      stmt.setString(6, f._6)
    else stmt.setNull(6, Types.NVARCHAR)
    if (insertJudge(f._7))
      stmt.setString(7, f._7)
    else stmt.setNull(7, Types.NVARCHAR)
    if (insertJudge(f._8))
      stmt.setString(8, f._8)
    else stmt.setNull(8, Types.NVARCHAR)
    if (insertJudge(f._9))
      stmt.setString(9, f._9)
    else stmt.setNull(9, Types.NVARCHAR)
    if (insertJudge(f._10))
      stmt.setString(10, f._10)
    else stmt.setNull(10, Types.NVARCHAR)
    if (insertJudge(f._11))
      stmt.setString(11, f._11)
    else stmt.setNull(11, Types.VARCHAR)
    if (insertJudge(f._12))
      stmt.setString(12, f._12)
    else stmt.setNull(12, Types.NVARCHAR)
    if (insertJudge(f._13)) {
      if (source == 4) {
        //vip的issue
        val array: Array[String] = ParseCleanUtil.cleanVipIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(18, array(1))
        stmt.setString(19, array(3))
      }
      if (source == 2) {
        //cnki的issue
        //cnki的issue
        /*val array: Array[String] = ParseCleanUtil.cleanCnkiIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(18, array(1))
        stmt.setString(19, array(3))*/
        stmt.setString(13, f._13)
        stmt.setNull(18, Types.NVARCHAR)
        if(insertJudge(f._21)){
          stmt.setString(19,f._21)
        }else{
          stmt.setNull(19, Types.NVARCHAR)
        }
      }
      if (source == 8) {
        //wf的issue
        val array: Array[String] = ParseCleanUtil.cleanWfIssue(f._13)
        stmt.setString(13, array(array.length - 1))
        stmt.setString(18, array(1))
        stmt.setNull(19, Types.VARCHAR)
      }
    }
    else stmt.setNull(13, Types.VARCHAR)
    if (insertJudge(f._14))
      stmt.setString(14, f._14)
    else stmt.setNull(14, Types.NVARCHAR)
    stmt.setInt(15, f._15)
    stmt.setInt(16, f._16)
    if (insertJudge(f._17)) //otherId
      stmt.setString(17, f._17)
    else{
      stmt.setNull(17, Types.VARCHAR)
    }
    if (insertJudge(f._18))  //datasource
      stmt.setString(20, f._18)
    else stmt.setNull(20, Types.VARCHAR)
    if (insertJudge(f._19))  //abstract
      //stmt.setString(21, f._19)
      stmt.setString(21, "")
    else stmt.setNull(21, Types.VARCHAR)
    if (insertJudge(f._20))  //abstract_alt
      //stmt.setString(22, f._20)
      stmt.setString(22, "")
    else stmt.setNull(22, Types.VARCHAR)


    if (insertJudge(f._22._1))  //subject
      stmt.setString(23,f._22._1)
    else stmt.setNull(23, Types.VARCHAR)
    if (insertJudge(f._22._2))  //doi
      stmt.setString(24,f._22._2)
    else stmt.setNull(24, Types.VARCHAR)
    if (insertJudge(f._22._3))  //issn
      stmt.setString(25,f._22._3)
    else stmt.setNull(25, Types.VARCHAR)
  }

  def setmatchData(stmt: PreparedStatement, f: (String, String, String, String, String),types:Int) = {
    if (insertJudge(f._1))
      stmt.setString(1, f._1)
    else stmt.setNull(1, Types.VARCHAR)
    //title, journal, creator, id, institute
    if (insertJudge(f._2))
      stmt.setString(2, f._2)
    else stmt.setNull(2, Types.VARCHAR)
    if (insertJudge(f._3))
      stmt.setString(3, f._3)
    else stmt.setNull(3, Types.VARCHAR)
    if (insertJudge(f._4))
      stmt.setString(4, f._4)
    else stmt.setNull(4, Types.VARCHAR)
    if (insertJudge(f._5))
      stmt.setString(5, f._5)
    else stmt.setNull(5, Types.VARCHAR)
    stmt.setInt(6,types)
  }


  def insertJudge(col: String): Boolean = {
    if (col == null || col.equals(""))
      return false
    else true
  }


  /**
    * 解析kafka的json数据
    *
    * @param jsonMap
    * @return
    */
  def parseCnkiJson(jsonMap: Map[String, Any]): (String, (String, String, String, String, String,String)) = {
    var creator = ""
    var title = ""
    var journal = ""
    var id = ""
    var institute = ""
    var journal2 = ""
    var year =""
    if (isUseful(jsonMap, "guid")) id = jsonMap("guid").toString
    if (isUseful(jsonMap, "creator")) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(jsonMap("creator").toString))
    if (isUseful(jsonMap, "title")) title = cnkiClean.cleanTitle(jsonMap("title").toString)
    if (isUseful(jsonMap, "journal")) journal = cnkiClean.cleanJournal(jsonMap("journal").toString)
    if (isUseful(jsonMap, "journal2")) journal2 = cnkiClean.cleanJournal(jsonMap("journal2").toString)
    if (isUseful(jsonMap, "year")) year =jsonMap("year").toString
    if (journal == null || journal.equals("")) {
      journal = journal2
    }
    if (isUseful(jsonMap, "institute")) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(jsonMap("institute").toString))
    val key = cutStr(title, 6) + cutStr(journal, 4)
    (key, (title, journal, creator, id, institute,year))
  }

  /**
    * 将原数据解析
    *
    * @param jsonMap
    */
  def parseCnkiSourceJson(jsonMap: Map[String, Any]) = {
    var creator = ""
    var creatorAll = ""
    var title = ""
    var journal = ""
    var id = ""
    var institute = ""
    var abstractcont = ""
    var abstractcont_alt = ""
    var titleAlt = ""
    var creatorAlt = ""
    var keyWord = ""
    var keyWordAlt = ""
    var instituteAll = ""
    var year = ""
    var issue = ""
    var url = ""
    if (isUseful(jsonMap, "guid")) id = jsonMap("guid").toString
    if (isUseful(jsonMap, "title")) title = cnkiClean.cleanTitle(jsonMap("title").toString)
    if (isUseful(jsonMap, "titleAlt")) titleAlt = jsonMap("titleAlt").toString
    if (isUseful(jsonMap, "creator")) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(jsonMap("creator").toString))
    if (isUseful(jsonMap, "creator_all")) creatorAlt = cnkiClean.cleanAuthor(jsonMap("creator_all").toString)
    if (isUseful(jsonMap, "creator")) creatorAll = cnkiClean.cleanAuthor(jsonMap("creator").toString)
    if (isUseful(jsonMap, "keyword")) keyWord = cnkiClean.cleanKeyWord(jsonMap("keyword").toString)
    //if (isUseful(jsonMap, "keyword")) keyWordAlt = cnkiClean.cleanKeyWord(jsonMap("keyword").toString)
    if (isUseful(jsonMap, "instituteAll")) instituteAll = cnkiClean.cleanInstitute(jsonMap("institute").toString)
    if (isUseful(jsonMap, "year")) year = jsonMap("year").toString
    if (isUseful(jsonMap, "journal")) journal = cnkiClean.cleanJournal(jsonMap("journal").toString)
    if (isUseful(jsonMap, "institute")) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(jsonMap("institute").toString))
    if (isUseful(jsonMap, "issue")) issue = jsonMap("issue").toString
    if (isUseful(jsonMap, "url")) url = jsonMap("url").toString
    if (isUseful(jsonMap, "abstract")) abstractcont = jsonMap("abstract").toString
    if (isUseful(jsonMap, "abstract_alt")) abstractcont_alt = jsonMap("abstract_alt").toString
    (id, (id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url,abstractcont,abstractcont_alt))
  }


  def main(args: Array[String]): Unit = {
    val str = "{\"year\":\"2016\",\"code\":\"670348458\",\"url\":\"http:\\/\\/lib.cqvip.com\\/qk\\/71697X\\/201611\\/670348458.html\",\"catalog\":\"\\u4e2d\\u6587\\u79d1\\u6280\\u671f\\u520a\\u6570\\u636e\\u5e93==>\\u671f\\u520a\\u5bfc\\u822a==>\\u521b\\u65b0\\u4f5c\\u6587\\uff1a\\u5c0f\\u5b663-4\\u5e74\\u7ea7==>2016\\u5e7411\\u671f\",\"title\":\"\\u74f6\\u5b50\\u91cc\\u7684\\u738b\\u5b50\",\"title_alt\":null,\"creator\":\"\\u6881\\u5434\",\"creator_all\":null,\"institute\":\"\\u5e7f\\u897f\\u5357\\u5b81\\u5e02\\u51e4\\u7fd4\\u8def\\u5c0f\\u5b66\\u56db(6)\\u73ed\",\"journal\":\"\\u521b\\u65b0\\u4f5c\\u6587\\uff1a\\u5c0f\\u5b663-4\\u5e74\\u7ea7=>2016\\u5e74\\u7b2c0\\u5377\\u7b2c11\\u671f 34-34\\u9875,\\u51711\\u9875\",\"journal_alt\":null,\"fund\":null,\"abstract\":\"\\u5357\\u65b9\\u8bd7\\u8001\\u5e08\\u7684\\u8bdd\\uff1a\\u5728\\u770b\\u7ae5\\u8bdd\\u300a\\u62c7\\u6307\\u59d1\\u5a18\\u300b\\u7684\\u65f6\\u5019,\\u6211\\u4eec\\u7684\\u8111\\u5b50\\u91cc\\u5c31\\u6709\\u4e86\\u8fd9\\u4e48\\u4e00\\u4e2a\\u5c0f\\u5c0f\\u7684\\u5750\\u5728\\u82b1\\u74e3\\u4e0a\\u7684\\u4eba\\u513f.\\u54a6,\\u8fd9\\u4e2a\\u4e16\\u754c\\u4e0a\\u4f1a\\u4e0d\\u4f1a\\u4e5f\\u6709\\u8fd9\\u4e48\\u4e00\\u4e2a\\u5c0f\\u4eba\\u513f\\u5750\\u5728\\u900f\\u660e\\u7684\\u73bb\\u7483\\u74f6\\u91cc,\\u6f02\\u5728\\u5927\\u6d77\\u4e0a\\u5bfb\\u627e\\u4ed6\\u60f3\\u8981\\u7684\\u5e78\\u798f\\u5462\\uff1f\\u8fd9\\u4e48\\u4e00\\u60f3,\\u5c31\\u6709\\u4e86\\u4e0b\\u9762\\u8fd9\\u4e24\\u4e2a\\u7ae5\\u8bdd\\u6545\\u4e8b.\",\"abstract_alt\":null,\"keyword\":\"\\u738b\\u5b50|!\\u73bb\\u7483\\u74f6|!\\u7ae5\\u8bdd\",\"keyword_alt\":null,\"subject\":\"\\u5206 \\u7c7b \\u53f7\\uff1a TQ171.68 [\\u5de5\\u4e1a\\u6280\\u672f > \\u5316\\u5b66\\u5de5\\u4e1a > \\u7845\\u9178\\u76d0\\u5de5\\u4e1a > \\u73bb\\u7483\\u5de5\\u4e1a > \\u751f\\u4ea7\\u8fc7\\u7a0b\\u4e0e\\u8bbe\\u5907 > \\u5236\\u54c1\\u52a0\\u5de5\\u5de5\\u827a\\uff08\\u518d\\u6210\\u578b\\uff09\\u53ca\\u8bbe\\u5907]\",\"aboutdate\":null,\"creator_intro\":null,\"reference\":\"\",\"similarliterature\":\"\\u74f6\\u5b50\\u91cc\\u7684\\u738b\\u5b50=>\\/qk\\/71697X\\/201611\\/670348458.html;;;\\u4e8c\\u6c27\\u5316\\u949b\\u8584\\u819c\\u7535\\u6781\\u7684\\u5236\\u5907\\u53ca\\u5206\\u6790=>\\/qk\\/96274A\\/201630\\/670260753.html;;;\\u672c\\u520a\\u5f81\\u7a3f\\u542f\\u4e8b=>\\/qk\\/90499X\\/201610\\/670459386.html;;;PLC\\u5728\\u7535\\u6c14\\u81ea\\u52a8\\u63a7\\u5236\\u4e2d\\u7684\\u5e94\\u7528=>\\/qk\\/80675A\\/201621\\/670409427.html;;;Highly Efficient Power Conversion from Salinity Gradients with Ion-Selective Polymeric Nanopores=>\\/qk\\/84212X\\/201609\\/670182018.html;;;\\u8499\\u7802\\u73bb\\u7483\\u7684\\u7814\\u5236\\u4e0e\\u5438\\u5149\\u6548\\u5e94\\u7684\\u8868\\u5f81=>\\/qk\\/95166X\\/201604\\/669477109.html;;;\\u9ad8\\u5f3a\\u5ea6\\u5316\\u5b66\\u94a2\\u5316\\u94a0\\u9499\\u73bb\\u7483\\u201cARMOREX\\uff08R\\uff09\\u201d=>\\/qk\\/91373X\\/201604\\/670200953.html;;;\\u8d85\\u58f0\\u632f\\u52a8\\u94e3\\u524a\\u5149\\u5b66\\u73bb\\u7483\\u6750\\u6599\\u8868\\u9762\\u8d28\\u91cf\\u7814\\u7a76=>\\/qk\\/70459X\\/201617\\/83687174504849544955484953.html;;;High haze textured surface B-doped ZnO-TCO films on wet-chemically etched glass substrates for thin film solar cells=>\\/qk\\/94689X\\/201608\\/669996383.html;;;\\u73bb\\u7483\\u6df1\\u52a0\\u5de5\\u5de5\\u5382\\u7684\\u5de5\\u4e1a4\\uff0e0=>\\/qk\\/97223A\\/201608\\/670219816.html\",\"updatetime\":\"2017-04-24 18:02:48.087\",\"id\":\"236860\",\"journal_name\":\"\\u521b\\u65b0\\u4f5c\\u6587\\uff1a\\u5c0f\\u5b663-4\\u5e74\\u7ea7\",\"creator_2\":\"\\u6881\\u5434\",\"institute_2\":\"\\u5e7f\\u897f\\u5357\\u5b81\\u5e02\\u51e4\\u7fd4\\u8def\\u5c0f\\u5b66\\u56db(6)\\u73ed\",\"journal_2\":\"\\u521b\\u65b0\\u4f5c\\u6587\\uff1a\\u5c0f\\u5b66\\u5e74\\u7ea7\",\"issue\":\"2016\\u5e74\\u7b2c0\\u5377\\u7b2c11\\u671f 34-34\\u9875,\\u51711\\u9875\",\"subject_2\":\"TQ171.68\",\"guid\":\"6C424426-D528-E711-AECF-0050569B7A51\",\"status\":\"1\",\"from\":\"vip\"}"
    val jsonStr = JSON.parseFull(str)
    jsonStr match {
      case Some(jsonMap: Map[String, Any]) => {
        var creator = ""
        var title = ""
        var journal = ""
        var id = ""
        var institute = ""
        var journal2 = ""
        if (isUseful(jsonMap, "guid")) id = jsonMap("guid").toString
        if (isUseful(jsonMap, "creator_2")) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(jsonMap("creator_2").toString))
        if (isUseful(jsonMap, "title")) title = cnkiClean.cleanTitle(jsonMap("title").toString)
        if (isUseful(jsonMap, "journal_2")) journal = cnkiClean.cleanJournal(jsonMap("journal_2").toString)
        if (isUseful(jsonMap, "journal_name")) journal2 = cnkiClean.cleanJournal(jsonMap("journal_name").toString)
        println(journal)
        if (journal == null || journal.equals("")) {
          journal = journal2
        }
        if (isUseful(jsonMap, "institute_2")) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(jsonMap("institute_2").toString))
        val key = cutStr(title, 6) + cutStr(journal, 4)
        println(key + "---" + journal + "---" + creator + "---" + id + "---" + institute)
      }
    }
  }
}
