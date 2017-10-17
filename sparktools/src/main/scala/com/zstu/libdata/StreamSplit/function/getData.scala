package com.zstu.libdata.StreamSplit.function

import com.zstu.libdata.StreamSplit.function.CommonTools._
import com.zstu.libdata.StreamSplit.function.ReadData.readDataLog
import com.zstu.libdata.StreamSplit.function.commonOps._
import com.zstu.libdata.StreamSplit.kafka.{cnkiClean, commonClean}
import com.zstu.libdata.StreamSplit.splitAuthor.getCLC.getCLCRdd
import com.zstu.libdata.StreamSplit.test.AddJournalInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}


/**
  * Created by Administrator on 2017/6/4 0004.
  */
object getData {
  def getChineseJournal(journal1: String, journal2: String): String = {
    if (journal1 == null || journal1 == "" || CommonTools.hasNoChinese(journal1)) {
      return journal2
    }
    else return journal1
  }

  def deleteBracket(str: String): String = {
    if (str == null) return null
    val pattern = "\\[.*?\\]".r
    return pattern.replaceAllIn(str, "").replace("[", "").replace("]", "")
  }

  case class fullData(id: String, title: String, titleAlt: String, creator: String, creatorAlt: String, creatorAll: String, keyWord: String, keyWordAlt: String, institute: String, instituteAll: String, year: String, journal: String, issue: String, url
  : String, abstractcont: String, abstract_alt: String, page: String, classifications: String, doi: String, issn: String)

  def myClean(str: String): String = {
    if (str == null) null
    else
      deleteInvisibleChar.deleteInvisibleChar(str.trim.replace("#", "").replace("@@", ""))
  }

  def getFullDataCNKIsql(hiveContext: HiveContext) = {
    def cleanSplitChar(str: String) = {
      if (str == null) null
      else str.replace("|!", ";")
    }


    hiveContext.udf.register("cutStr", (str: String, len: Int) => if (str != "" && str != null) cutStr(str, len) else null)
    hiveContext.udf.register("getChineseJournal", (str1: String, str2: String) => getChineseJournal(str1, str2))

    hiveContext.udf.register("cleanSplitChar", (str: String) => if (str != "" && str != null) cleanSplitChar(str) else null)
    hiveContext.udf.register("deleteZero", (str: String) => if (str != "" && str != null) deleteZero.deleteZero(str) else null)
    hiveContext.udf.register("myClean", (str: String) => if (str != "" && str != null) myClean(str) else null)

    hiveContext.udf.register("cleanTitle", (str: String) => if (str != "" && str != null) cnkiOps.cleanTitle(str) else null)
    hiveContext.udf.register("getFirstCreator", (str: String) => if (str != "" && str != null) cnkiOps.getFirstCreator(str) else null)
    //    hiveContext.udf.register("cleanAuthor", (str: String) => if (str != "" && str != null) deleteBracket(myClean(str)) else null)
    hiveContext.udf.register("cleanAuthor", (str: String) => if (str != "" && str != null) cnkiOps.cleanAuthor(str) else null)
    hiveContext.udf.register("cleanKeyWord", (str: String) => if (str != "" && str != null) cnkiOps.cleanKeyWord(str) else null)
    hiveContext.udf.register("cleanInstitute", (str: String) => if (str != "" && str != null) cnkiOps.cleanInstitute(str) else null)
    hiveContext.udf.register("cleanJournal", (str: String) => if (str != "" && str != null) cnkiOps.cleanJournal(str) else null)
    hiveContext.udf.register("getFirstInstitute", (str: String) => if (str != "" && str != null) cnkiOps.getFirstInstitute(str) else null)
    hiveContext.udf.register("getChineseAbstract", (str: String) => if (str != "" && str != null) cnkiClean.getChineseAbstract(str) else null)
    hiveContext.udf.register("cleanTitle", (str: String) => if (str != "" && str != null) cnkiOps.cleanTitle(str) else null)
    hiveContext.udf.register("getEnglishAbstract", (str: String) => if (str != "" && str != null) cnkiClean.getEnglishAbstract(str) else null)

    val originData = hiveContext.sql("select " +
      "GUID as id,year,deleteZero(issue) as issue,url,deleteZero(page) as page,subject as classifications,cutStr(DOI,500) as DOI," +
      "cutStr(cleanTitle(title),4000) as title," +
      "cutStr(getFirstCreator(cleanAuthor(creator)),2000) as creator," +
      "cutStr(cleanAuthor(cleanSplitChar(creator_all)),4000) as creatorAlt," +
      "cutStr(cleanAuthor(creator),2000) as creatorAll," +
      "cleanKeyWord(keyword) as keyWord," +
      "cleanKeyWord('') as keyWordAlt," +
      "cutStr(cleanInstitute(institute),4000) as instituteAll," +
      "cutStr(getFirstInstitute(cleanInstitute(institute)),4000) as institute," +
      "cutStr(getChineseJournal(journal,journal2),2000) as journal," +

      "cutStr(getChineseAbstract(abstract),4000) as abstract," +
      "cutStr(getEnglishAbstract(abstract),4000) as abstractAlt " +
      "from t_orgjournaldataCNKI " +
      //      " where status = 0 and year = \'2017\'" +
      "")
    val coredData = ReadData.readData50("DiscoveryV3", "t_JournalCore", hiveContext)
  AddJournalInfo.addCoreIssnDatasource(coredData, originData, hiveContext,"cnki")


  }

  def chooseNotNull(str1: String, str2: String): String = {
    if (str1 == null || str1 == "") str2
    else str1
  }


  def getPageVIP(issue: String) = if (issue == null) null else deleteZero.deleteZero(commonClean.cleanIssue(issue, 4)(2))

  def getVolumeVIP(issue: String) = if (issue == null) null else deleteZero.deleteZero(commonClean.cleanIssue(issue, 4)(1))

  def getIssueVIP(issue: String) = if (issue == null) null else deleteZero.deleteZero(commonClean.cleanIssue(issue, 4)(0))

  def getFullDataVIPsql(hiveContext: HiveContext) = {

    def cleanSplitChar(str: String) = {
      if (str == null) null
      else str.replace("|!", ";")
    }


    hiveContext.udf.register("cleanSplitChar", (str: String) => if (str != "" && str != null) cleanSplitChar(str) else null)

    hiveContext.udf.register("chooseNotNull", (str1: String, str2: String) => chooseNotNull(str1, str2))


    hiveContext.udf.register("getPage", (str: String) => if (str != "" && str != null) getPageVIP(str) else null)
    hiveContext.udf.register("getIssue", (str: String) => if (str != "" && str != null) getIssueVIP(str) else null)
    hiveContext.udf.register("getVolume", (str: String) => if (str != "" && str != null) getVolumeVIP(str) else null)
    hiveContext.udf.register("cleanTitle", (str: String) => if (str != "" && str != null) cnkiOps.cleanTitle(myClean(str)) else null)
    hiveContext.udf.register("getFirstCreator", (str: String) => if (str != "" && str != null) cnkiOps.getFirstCreator(str) else null)
    hiveContext.udf.register("myClean", (str: String) => if (str != "" && str != null) myClean(str) else null)

    //    hiveContext.udf.register("cleanAuthor", (str: String) => if (str != "" && str != null) deleteBracket(myClean(str)) else null)
    hiveContext.udf.register("cleanAuthor", (str: String) => if (str != "" && str != null) cnkiOps.cleanAuthor(str) else null)
    hiveContext.udf.register("cleanKeyWord", (str: String) => if (str != "" && str != null) cnkiOps.cleanKeyWord(myClean(str)) else null)
    hiveContext.udf.register("cleanInstitute", (str: String) => if (str != "" && str != null) cnkiOps.cleanInstitute(str) else null)
    hiveContext.udf.register("cleanJournal", (str: String) => if (str != "" && str != null) cnkiOps.cleanJournal(str) else null)
    hiveContext.udf.register("getFirstInstitute", (str: String) => if (str != "" && str != null) cnkiOps.getFirstInstitute(str) else null)
    hiveContext.udf.register("getChineseAbstract", (str: String) => if (str != "" && str != null) cnkiClean.getChineseAbstract(str) else null)
    hiveContext.udf.register("cleanTitle", (str: String) => if (str != "" && str != null) cnkiOps.cleanTitle(str) else null)
    hiveContext.udf.register("getEnglishAbstract", (str: String) => if (str != "" && str != null) cnkiClean.getEnglishAbstract(str) else null)
    val originData = hiveContext.sql("select " +
      "GUID as id,year,url,subject_2 as classifications," +
      "getPage(issue) as page," +
      "getIssue(issue) as issue," +
      "getVolume(issue) as volume," +
      "cleanTitle(title) as title," +
      "myClean(title_alt) as titleAlt," +
      "getFirstCreator(cleanAuthor(creator_2)) as creator," +
      "cleanSplitChar(cleanAuthor(creator_all)) as creatorAlt," +
      "cleanAuthor(creator_2) as creatorAll," +
      "cleanKeyWord(keyword) as keyWord," +
      "cleanKeyWord('') as keyWordAlt," +
      "cleanInstitute(institute_2) as instituteAll," +
      "getFirstInstitute(cleanInstitute(institute_2)) as institute," +
      "chooseNotNull(journal_name,journal_2) as journal," +
      "getChineseAbstract(abstract) as abstract," +
      "getEnglishAbstract(abstract_alt) as abstractAlt " +
      "from t_orgjournaldataVIP " +
      //      " where status = 0 and year = \'2017\'" +
      "")
    val coredData = ReadData.readData50("DiscoveryV3", "t_JournalCore", hiveContext)
    AddJournalInfo.addCoreIssnDatasource(coredData, originData, hiveContext,"vip")
  }


  def getVolumeWF(issue: String) = if (issue == null) null else deleteZero.deleteZero(commonClean.cleanIssue(issue, 8)(1))

  def getIssueWF(issue: String) = if (issue == null) null else deleteZero.deleteZero(commonClean.cleanIssue(issue, 8)(2))

  def getFullDataWFsql(hiveContext: HiveContext) = {
    //    def getPage(issue: String) = commonClean.cleanIssue(issue,8)(2)

    def cleanSplitChar(str: String) = {
      if (str == null) null
      else str.replace("|!", ";")
    }

    hiveContext.udf.register("myClean", (str: String) => if (str != "" && str != null) myClean(str) else null)

    hiveContext.udf.register("cleanSplitChar", (str: String) => if (str != "" && str != null) cleanSplitChar(str) else null)
    hiveContext.udf.register("chooseNotNull", (str1: String, str2: String) => chooseNotNull(str1, str2))

    //    hiveContext.udf.register("getPage", (str: String) =>if(str !="" && str !=null) getPage(str) else null)
    hiveContext.udf.register("getIssue", (str: String) => if (str != "" && str != null) getIssueWF(str) else null)
    hiveContext.udf.register("getVolume", (str: String) => if (str != "" && str != null) getVolumeWF(str) else null)

    hiveContext.udf.register("cleanTitle", (str: String) => if (str != "" && str != null) cnkiOps.cleanTitle(myClean(str)) else null)
    hiveContext.udf.register("getFirstCreator", (str: String) => if (str != "" && str != null) cnkiOps.getFirstCreator(str) else null)
    //    hiveContext.udf.register("cleanAuthor", (str: String) => if (str != "" && str != null) deleteBracket(myClean(str)) else null)

    hiveContext.udf.register("cleanAuthor", (str: String) => if (str != "" && str != null) cnkiOps.cleanAuthor(str) else null)
    hiveContext.udf.register("cleanKeyWord", (str: String) => if (str != "" && str != null) cnkiOps.cleanKeyWord(myClean(str)) else null)
    hiveContext.udf.register("cleanInstitute", (str: String) => if (str != "" && str != null) cnkiOps.cleanInstitute(str) else null)
    hiveContext.udf.register("cleanJournal", (str: String) => if (str != "" && str != null) cnkiOps.cleanJournal(str) else null)
    hiveContext.udf.register("getFirstInstitute", (str: String) => if (str != "" && str != null) cnkiOps.getFirstInstitute(str) else null)
    hiveContext.udf.register("getChineseAbstract", (str: String) => if (str != "" && str != null) cnkiClean.getChineseAbstract(str) else null)
    hiveContext.udf.register("cleanTitle", (str: String) => if (str != "" && str != null) cnkiOps.cleanTitle(str) else null)
    hiveContext.udf.register("getEnglishAbstract", (str: String) => if (str != "" && str != null) cnkiClean.getEnglishAbstract(str) else null)
   val originData =  hiveContext.sql("select " +
      "GUID as id,year,url,subject as classifications,doi," +
      //      "getPage(issue) as page," +
      "getIssue(issue) as issue," +
      "getVolume(issue) as volume," +
      "cleanTitle(title) as title," +
      "myClean(title_alt) as titleAlt," +
      "getFirstCreator(cleanAuthor(creator)) as creator," +
      "cleanSplitChar(cleanAuthor(creator_all)) as creatorAlt," +
      "cleanAuthor(creator) as creatorAll," +
      "cleanKeyWord(keyword) as keyWord," +
      "cleanKeyWord('') as keyWordAlt," +
      "cleanInstitute(institute) as instituteAll," +
      "getFirstInstitute(cleanInstitute(institute)) as institute," +
      "chooseNotNull(journal_name,journal_alt) as journal," +
      "getChineseAbstract(abstract) as abstract," +
      "getEnglishAbstract(abstract) as abstractAlt " +
      "from t_orgjournaldataWF " +
      //      " where status = 0 and year = \'2017\'" +
      "")
    val coredData = ReadData.readData50("DiscoveryV3", "t_JournalCore", hiveContext)
    AddJournalInfo.addCoreIssnDatasource(coredData, originData, hiveContext,"wf")
  }


  /**
    * 读取所需数据
    *
    * @param hiveContext hiveContext
    * @return (CLCRdd,authorRdd,simplifiedJournalRdd,sourceCoreRdd,journalMagSourceRdd)
    *
    */
  def readSourceRdd(hiveContext: HiveContext) = {
    val CLCRdd = getCLCRdd(hiveContext)
    val simplifiedJournalRdd = ReadData.readData50("DiscoveryV3","t_JournalKey",hiveContext)
  .map(f => transformRdd(f))
    (CLCRdd, simplifiedJournalRdd)
  }

  def getCoreData(hiveContext: HiveContext) = {
    val sourceCoreData = readDataLog("t_JournalCore", hiveContext)
    sourceCoreData.select("name", "isCore").withColumnRenamed("name", "journalName")
  }

  def getMagData(hiveContext: HiveContext) = {
    val journalMagSourceRdd = readDataLog("t_JournalMag", hiveContext)
      .select("MAGTITLE", "DATASOURCE")
    journalMagSourceRdd.withColumnRenamed("MAGTITLE", "journalName")
      .withColumnRenamed("DATASOURCE", "datasource")
  }

  /**
    *
    * @param data data
    * @return
    */
  //  def getSimplifiedInputRdd(data: DataFrame): RDD[(String, (String, String, String, String, String, String))] ={
  //    //    (key, (title, journal, creator, id, institute,year))
  //    //val key = cutStr(title, 6) + cutStr(journal, 4)
  //    def getJournal(journal1: String,journal2: String) ={
  //      if (journal1 == null || journal1 == "" ) {
  //        journal2
  //      }else journal1
  //    }
  //    distinctInputRdd(
  //      data.map(r =>
  //        (
  //          cutStr(cnkiOps.cleanTitle(getRowString(r,"title")),6) + cutStr(getRowString(r,"journal"),4),
  //          (cnkiOps.cleanTitle(getRowString(r,"title")),
  //            cnkiOps.cleanJournal(getRowString(r,"journal")),
  //            cnkiOps.getFirstCreator(getRowString(r,"creatorAll")),
  //            getRowString(r,"id"),
  //            cnkiOps.getFirstInstitute(getRowString(r,"instituteAll")),
  //            getRowString(r,"year")
  //          )
  //        ))
  //    )._1
  //  }

  def getForSplitRdd(data: DataFrame): RDD[(String, ((String, String), (String, String, String, String, String)))] = {
    def cleanKeyword(keyword: String) = {
      if (keyword == null) null
      else keyword.replace("|!", ";")
    }

    data
      .map(r => (
        getRowString(r, "id"),
        ((cleanKeyword(getRowString(r, "creatorAll")), cnkiOps.cleanInstitute(cleanKeyword(getRowString(r, "instituteAll")))),
          (cleanKeyword(getRowString(r, "keyWord")), null,
            getRowString(r, "subject"), getRowString(r, "id"),
            getRowString(r, "journal"))
        )
      ))
  }

  def getFullInputRdd(data: DataFrame): RDD[(String, (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String))] = {
    //    (id, (id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url,abstractcont,abstractcont_alt))
    //    getRowString(r,"title")
    data.map(r =>
      (
        getRowString(r, "GUID"), (
        getRowString(r, "GUID"),
        getRowString(r, "title"),
        /*getRowString(r,"titleAlt")*/ null,
        getRowString(r, "creator"),
        /* getRowString(r,"creatorAlt")*/ null,
        getRowString(r, "creator_all"),
        getRowString(r, "keyword"),
        /*getRowString(r,"keyWordAlt")*/ null,
        getRowString(r, "institute"),
        /*getRowString(r,"instituteAll")*/ null,
        getRowString(r, "year"),
        getRowString(r, "journal"),
        getRowString(r, "issue"),
        getRowString(r, "url"),
        /* getRowString(r,"abstractcont")*/ null,
        /*getRowString(r,"abstractcont_alt")*/ null
        , getRowString(r, "page")))
    )
  }


  /**
    * 将错误信息写入t_error,返回正确数据
    *
    * @param simplifiedInputRdd 简化过的输入数据
    * @param hiveContext        hiveContext
    * @return rightRdd
    */
  def getRightRddAndReportError(simplifiedInputRdd: RDD[(String, (String, String, String, String, String, String, String, String))], hiveContext: HiveContext) = {
    //errorRDD也是一个集合，存放streamingRDD中的错误数据
    val (errorRDD, rightRdd) = (
      simplifiedInputRdd.filter(f => commonOps.filterErrorRecord(f._2))
      , simplifiedInputRdd.filter(f => !commonOps.filterErrorRecord(f._2)))
    //把errorRDD集合映射成t_error表（sqlserver）中的字段


    (rightRdd, errorRDD.map(value => (value._2._4, "缺少标题，期刊或者年份")))
  }


  def transformRdd_cnki_source(r: Row) = {

    val id = r.getString(r.fieldIndex("GUID"))
    var title = r.getString(r.fieldIndex("title"))
    val titleAlt = ""
    var creator = r.getString(r.fieldIndex("creator"))
    var creatorAlt = r.getString(r.fieldIndex("creator_all"))
    var creatorAll = r.getString(r.fieldIndex("creator"))
    var keyWord = r.getString(r.fieldIndex("keyword"))
    var keyWordAlt = ""
    var instituteAll = r.getString(r.fieldIndex("institute"))
    val year = r.getString(r.fieldIndex("year"))
    var journal = r.getString(r.fieldIndex("journal"))
    var institute = r.getString(r.fieldIndex("institute"))
    val issue = r.getString(r.fieldIndex("issue"))
    val url = r.getString(r.fieldIndex("url"))
    var abstractcont = r.getString(r.fieldIndex("abstract"))
    var abstractcont_alt = r.getString(r.fieldIndex("abstract"))
    val page = r.getString(r.fieldIndex("page"))
    val subject = r.getString(r.fieldIndex("subject"))

    val doi = r.getString(r.fieldIndex("DOI"))
    val issn = r.getString(r.fieldIndex("ISSN"))

    if (title != "" && title != null) title = cnkiClean.cleanTitle(title)
    if (creator != "" && creator != null) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(creator))
    if (creatorAlt != "" && creatorAlt != null) creatorAlt = cnkiClean.cleanAuthor(creatorAlt)
    if (creatorAll != "" && creatorAll != null) creatorAll = cnkiClean.cleanAuthor(creatorAll)
    if (keyWord != "" && keyWord != null) keyWord = cnkiClean.cleanKeyWord(keyWord)
    if (keyWordAlt != "" && keyWordAlt != null) keyWordAlt = cnkiClean.cleanKeyWord(keyWordAlt)
    if (instituteAll != "" && instituteAll != null) instituteAll = cnkiClean.cleanInstitute(instituteAll)
    if (journal != "" && journal != null) journal = cnkiClean.cleanJournal(journal)
    if (institute != "" && institute != null) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(institute))
    if (abstractcont != "" && abstractcont != null) abstractcont = cnkiClean.getChineseAbstract(abstractcont)
    if (abstractcont_alt != "" && abstractcont_alt != null) abstractcont_alt = cnkiClean.getEnglishAbstract(abstractcont_alt)
    //logUtil("---abstractcont-"+abstractcont)
    //logUtil("---abstractcont_alt-"+abstractcont_alt)

    fullData(id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url, abstractcont, abstractcont_alt, page, subject, doi, issn)
  }


}
