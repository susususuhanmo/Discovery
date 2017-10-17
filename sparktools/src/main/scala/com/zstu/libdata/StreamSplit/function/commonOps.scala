package com.zstu.libdata.StreamSplit.function

import java.sql.{PreparedStatement, Types}

import com.zstu.libdata.StreamSplit.KafkaDataClean.ParseCleanUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

import scala.util.parsing.json.JSON

/**
  * Created by Yuli_Yan on 2017/4/17.
  */
object commonOps {


  //todo 查重算法改进
  //找到匹配的记录
  def getDisMatchRecord(value: ((String, String, String, String, String,String,String,String)
    , Option[(String, String, String, String,String,String,String,String)])): Boolean = {
//   (key, (title, journal, creator, id,institute,year,issue,volume))

    if (value._2.isEmpty)
      return false
    val data = value._2.get
    if (getSimilarty(value._1._1, data._1, value._1._2, data._2, value._1._3, data._3) > 0.9) {
      //虽然key匹配度>0.9 ,但是只要year不同认为不匹配
      if(value._1._6 !="" && data._6 !="" && value._1._6 != data._6)
        false
        //若issue 不同 则不匹配
      else if(value._1._7 !="" && data._7 !=""&& data._7 !=null &&value._1._7 !=null && value._1._7 != data._7)
        false
      else if(value._1._8 !="" && data._8 !=""&& data._8 !=null &&value._1._8 !=null && value._1._8 != data._8)
        false
      else
        true
    } else {
      false
    }
  }

  //在找到的5条匹配的记录中，返回相似度最高的记录
  def getHightestRecord(f: (String, Iterable[((String, String, String, String, String,String,String,String), Option[(String, String, String, String,String,String,String,String)])])): (String, String) = {
    val value = f._2
    if (value.size >= 2) {
      val firstData = value.take(1).toList.apply(0)
      var guid = firstData._2.get._4
      var simalirt = 0.0
      var high = getSimilarty(firstData._1._1, firstData._2.get._1, firstData._1._2, firstData._2.get._2, firstData._1._3, firstData._2.get._3)
      for (i <- 2 to value.size) {
        val second = value.take(i).toList.apply(0)
        simalirt = getSimilarty(second._1._1, second._2.get._1, second._1._2, second._2.get._2, second._1._3, second._2.get._3)
        if (high < simalirt) {
          high = simalirt
          guid = second._2.get._4
        }
      }
      (f._1, guid)
    } else {
      (f._1, f._2.take(1).toList.apply(0)._2.get._4)
    }
  }

  val sqlUrl = "jdbc:sqlserver://192.168.1.160:1433;DatabaseName=Log;"
  val userName = "fzj"
  val passWord = "fzj@zju"




  /**
    * 将数据insert进数据库
    *
    * @param tableName
    * @param hiveContext
    * @param writeErrorRDD
    * @return
    */
  def insertData(tableName: String, hiveContext: HiveContext, writeErrorRDD: RDD[(String, String)]) = {
    val option = Map("url" -> sqlUrl, "user" -> userName, "password" -> passWord, "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data.registerTempTable("error")
    val errorData = hiveContext.createDataFrame(writeErrorRDD)
    errorData.registerTempTable("insertData")
    hiveContext.sql("insert into error select * from insertData")
  }

  def getJournalKay(title:String,journal:String): String ={
    val subTitle = cutStrOps(title, 6)
    val subJournal = cutStrOps(journal, 4)
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

    val title = r.getString(r.fieldIndex("title"))
    val journal = r.getString(r.fieldIndex("journal"))
    val year = r.getString(r.fieldIndex("year"))
    val institute = r.getString(r.fieldIndex("instituteAll"))
    var subTitle = cutStrOps(title, 6)
    var subJournal = cutStrOps(journal, 4)
    var creator = r.getString(r.fieldIndex("creatorAll"))
    val id = r.getString(r.fieldIndex("id"))
    val issue = deleteZero.deleteZero(r.getString(r.fieldIndex("issue")))
    val volume = deleteZero.deleteZero(r.getString(r.fieldIndex("volume")))
    if (subTitle == null) subTitle = ""
    if (subJournal == null) subJournal = ""
    if (creator == null) creator = ""
    val key = subTitle + subJournal
    (key, (title, journal, creator, id,institute,year,issue,volume))
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
  def cutStrOps(str: String, length: Int): String = {
    if (str == null) return null
    if (str.equals("null")) return null
    if (str.length >= length) {
      return str.substring(0, length)
    } else str
  }

  /**
    * 查重匹配数据
    *
    * @param value :title ,journal creater ,id ,institute,year
    * @return
    */
  def filterDisMatchData(value: Iterable[((String, String, String, String, String,String), Option[(String, String, String, String,String,String)])]): Boolean = {
    var flag = true
    value.foreach(f => {
      val kafkaData = f._1
      if (f._2 == None) {
        flag = true
      } else {
        val dbData = f._2.get
        if (getSimilarty(kafkaData._1, dbData._1, kafkaData._2, dbData._2, kafkaData._3, dbData._3) > 0.9)
        {
          //虽然key 匹配 > 0.9,只要 year存在并且不同则为不匹配 ，返回true
          if(kafkaData._6 !="" && dbData._6 !="" && kafkaData._6 != dbData._6){
            flag = true
          }else
            flag = false
          return flag
        }
      }
    })
    flag
  }

  /**
    * 查询不匹配的数据
    *
    * @param value
    * @return
    */
  def disMatchResult(value: Iterable[((String, String, String, String, String), Option[(String, String, String, String)])]): Boolean = {
    var flag = false
    value.foreach(f => {
      val kafkaData = f._1
      if (f._2 == None) {
        flag = false
      } else {
        val dbData = f._2.get
        if (getSimilarty(kafkaData._1, dbData._1, kafkaData._2, dbData._2, kafkaData._3, dbData._3) > 0.9) {
          flag = true
          return flag
        }
      }
    })
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
    * 相似度计算
    *
    * @param matchTitle
    * @param dbTitle
    * @param matchJournal
    * @param dbJournal
    * @param matchCreator
    * @param dbCreator
    * @return
    */
  def getSimilartyOld(matchTitle: String, dbTitle: String, matchJournal: String, dbJournal: String, matchCreator: String, dbCreator: String): Double = {
    var same = 0.0
    same += LevenshteinDistance.score(matchTitle, dbTitle) * 0.6
    same += LevenshteinDistance.score(matchJournal, dbJournal) * 0.2
    if (matchCreator == null|| dbCreator == null ||
      matchCreator.length == 0 || dbCreator.length == 0) same += 0.2
    else same += LevenshteinDistance.score(matchCreator, dbCreator) * 0.2
    same
  }


  def getSimilarty(inputTitle: String, dbTitle: String, inputJournal: String
                      , dbJournal: String, inputCreator: String, dbCreator: String): Double = {
    //title0.7 journal 0.2 creator0.1
//    若creator没有 title0.8 journal 0.2
    val titleScore = LevenshteinDistance.score(inputTitle, dbTitle)
    val journalScore = LevenshteinDistance.score(inputJournal, dbJournal)
    if(inputCreator == null || inputCreator == "" || dbCreator == null || dbCreator == "")
      titleScore*0.8 + journalScore*0.2
    else
      titleScore*0.7 + journalScore*0.2 + LevenshteinDistance.score(inputCreator, dbCreator)*0.1
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
    * @return
    */
  def filterErrorRecord(value: (String, String, String, String, String,String,String,String)): Boolean = {
//    (key, (title, journal, creator, id,institute,year))


    if (value == None) return true
    if (value._1 == null || value._1.equals("")) return true
    if (value._2 == null || value._2.equals("")) return true
    if (value._6 == null || value._6.equals("")) return true
    else
      return false
  }



  /**
   * 设置数据库数据
   *
   * @param stmt
   * @param f
   */
  def setData4newdataPage(stmt: PreparedStatement,
                          f: (String, String, String, String, String, String
    , String, String, String, String, String, String, String
    , String, Int, Int, String, String, String, String, String
    , Option[(String, String)]), source: Int) = {

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
//        val array: Array[String] = ParseCleanUtil.cleanCnkiIssue(f._13)
//        stmt.setString(13, array(2))
//        stmt.setString(18, array(1))
//        stmt.setString(19, array(3))
        // TODO: 13改f_13
        stmt.setNull(18, Types.VARCHAR)
          stmt.setString(13, f._13)
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


    if (insertJudge(f._18))  //classification
      stmt.setString(19, f._18)
    else stmt.setNull(19, Types.VARCHAR)


    if (insertJudge(f._19))  //abstract
      stmt.setString(21, f._19)
    else stmt.setNull(21, Types.VARCHAR)
    if (insertJudge(f._20))  //abstract_alt
      stmt.setString(22, f._20)
    else stmt.setNull(22, Types.VARCHAR)
    // TODO: page处理
    if (insertJudge(f._21))  //page
      stmt.setString(20, f._21)
    else stmt.setNull(20, Types.VARCHAR)


    if (insertJudge(f._22.getOrElse((null,null))._1))  //xml
      stmt.setString(23, f._22.getOrElse((null,null))._1)
    else stmt.setNull(23, Types.VARCHAR)

    if (insertJudge(f._22.getOrElse((null,null))._2))  //suject
      stmt.setString(24, f._22.getOrElse((null,null))._2)
    else stmt.setNull(24, Types.VARCHAR)
  }




  def setData4newdata(stmt: PreparedStatement, f: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int, Int, String,String,String,String), source: Int) = {
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
        val array: Array[String] = ParseCleanUtil.cleanCnkiIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(18, array(1))
        stmt.setString(19, array(3))
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
  }

  /**
   * 设置数据库数据
   *
   * @param stmt
   * @param f (id,title,titleAlt,authorName,allAuthors,keywords,keywordAlt,orgName,allOrgans,year,journal,issue,defaultUrl,isCore,volume,abstract,abstractAlt
   * @return
    */
  def setData4newdata2Journal2016(stmt: PreparedStatement, f: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int, Int, String,String,String,String), source: Int) = {
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
  def setData(stmt: PreparedStatement, f: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int, Int, String,String,String,String,String), source: Int) = {
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
        val array: Array[String] = ParseCleanUtil.cleanCnkiIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(18, array(1))
        stmt.setString(19, array(3))
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
      if (insertJudge(f._21))
        stmt.setString(17, f._21)
      else
        stmt.setNull(17, Types.VARCHAR)
    }
    if (insertJudge(f._18))  //datasource
      stmt.setString(20, f._18)
    else stmt.setNull(20, Types.VARCHAR)
    if (insertJudge(f._19))  //abstract
      stmt.setString(21, f._19)
    else stmt.setNull(21, Types.VARCHAR)
    if (insertJudge(f._20))  //abstract_alt
      stmt.setString(22, f._20)
    else stmt.setNull(22, Types.VARCHAR)
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
      false
    else true
  }





  def main(args: Array[String]): Unit = {
    println(getSimilarty("随迁子女普通高中教育需求问题探析及对策研究","随迁子女普通高中教育需求问题探析及对策研究","现代教育科学","现代教育科学","孙华东","孙华东"))
//    val str = "{\"year\":\"2016\",\"code\":\"670348458\",\"url\":\"http:\\/\\/lib.cqvip.com\\/qk\\/71697X\\/201611\\/670348458.html\",\"catalog\":\"\\u4e2d\\u6587\\u79d1\\u6280\\u671f\\u520a\\u6570\\u636e\\u5e93==>\\u671f\\u520a\\u5bfc\\u822a==>\\u521b\\u65b0\\u4f5c\\u6587\\uff1a\\u5c0f\\u5b663-4\\u5e74\\u7ea7==>2016\\u5e7411\\u671f\",\"title\":\"\\u74f6\\u5b50\\u91cc\\u7684\\u738b\\u5b50\",\"title_alt\":null,\"creator\":\"\\u6881\\u5434\",\"creator_all\":null,\"institute\":\"\\u5e7f\\u897f\\u5357\\u5b81\\u5e02\\u51e4\\u7fd4\\u8def\\u5c0f\\u5b66\\u56db(6)\\u73ed\",\"journal\":\"\\u521b\\u65b0\\u4f5c\\u6587\\uff1a\\u5c0f\\u5b663-4\\u5e74\\u7ea7=>2016\\u5e74\\u7b2c0\\u5377\\u7b2c11\\u671f 34-34\\u9875,\\u51711\\u9875\",\"journal_alt\":null,\"fund\":null,\"abstract\":\"\\u5357\\u65b9\\u8bd7\\u8001\\u5e08\\u7684\\u8bdd\\uff1a\\u5728\\u770b\\u7ae5\\u8bdd\\u300a\\u62c7\\u6307\\u59d1\\u5a18\\u300b\\u7684\\u65f6\\u5019,\\u6211\\u4eec\\u7684\\u8111\\u5b50\\u91cc\\u5c31\\u6709\\u4e86\\u8fd9\\u4e48\\u4e00\\u4e2a\\u5c0f\\u5c0f\\u7684\\u5750\\u5728\\u82b1\\u74e3\\u4e0a\\u7684\\u4eba\\u513f.\\u54a6,\\u8fd9\\u4e2a\\u4e16\\u754c\\u4e0a\\u4f1a\\u4e0d\\u4f1a\\u4e5f\\u6709\\u8fd9\\u4e48\\u4e00\\u4e2a\\u5c0f\\u4eba\\u513f\\u5750\\u5728\\u900f\\u660e\\u7684\\u73bb\\u7483\\u74f6\\u91cc,\\u6f02\\u5728\\u5927\\u6d77\\u4e0a\\u5bfb\\u627e\\u4ed6\\u60f3\\u8981\\u7684\\u5e78\\u798f\\u5462\\uff1f\\u8fd9\\u4e48\\u4e00\\u60f3,\\u5c31\\u6709\\u4e86\\u4e0b\\u9762\\u8fd9\\u4e24\\u4e2a\\u7ae5\\u8bdd\\u6545\\u4e8b.\",\"abstract_alt\":null,\"keyword\":\"\\u738b\\u5b50|!\\u73bb\\u7483\\u74f6|!\\u7ae5\\u8bdd\",\"keyword_alt\":null,\"subject\":\"\\u5206 \\u7c7b \\u53f7\\uff1a TQ171.68 [\\u5de5\\u4e1a\\u6280\\u672f > \\u5316\\u5b66\\u5de5\\u4e1a > \\u7845\\u9178\\u76d0\\u5de5\\u4e1a > \\u73bb\\u7483\\u5de5\\u4e1a > \\u751f\\u4ea7\\u8fc7\\u7a0b\\u4e0e\\u8bbe\\u5907 > \\u5236\\u54c1\\u52a0\\u5de5\\u5de5\\u827a\\uff08\\u518d\\u6210\\u578b\\uff09\\u53ca\\u8bbe\\u5907]\",\"aboutdate\":null,\"creator_intro\":null,\"reference\":\"\",\"similarliterature\":\"\\u74f6\\u5b50\\u91cc\\u7684\\u738b\\u5b50=>\\/qk\\/71697X\\/201611\\/670348458.html;;;\\u4e8c\\u6c27\\u5316\\u949b\\u8584\\u819c\\u7535\\u6781\\u7684\\u5236\\u5907\\u53ca\\u5206\\u6790=>\\/qk\\/96274A\\/201630\\/670260753.html;;;\\u672c\\u520a\\u5f81\\u7a3f\\u542f\\u4e8b=>\\/qk\\/90499X\\/201610\\/670459386.html;;;PLC\\u5728\\u7535\\u6c14\\u81ea\\u52a8\\u63a7\\u5236\\u4e2d\\u7684\\u5e94\\u7528=>\\/qk\\/80675A\\/201621\\/670409427.html;;;Highly Efficient Power Conversion from Salinity Gradients with Ion-Selective Polymeric Nanopores=>\\/qk\\/84212X\\/201609\\/670182018.html;;;\\u8499\\u7802\\u73bb\\u7483\\u7684\\u7814\\u5236\\u4e0e\\u5438\\u5149\\u6548\\u5e94\\u7684\\u8868\\u5f81=>\\/qk\\/95166X\\/201604\\/669477109.html;;;\\u9ad8\\u5f3a\\u5ea6\\u5316\\u5b66\\u94a2\\u5316\\u94a0\\u9499\\u73bb\\u7483\\u201cARMOREX\\uff08R\\uff09\\u201d=>\\/qk\\/91373X\\/201604\\/670200953.html;;;\\u8d85\\u58f0\\u632f\\u52a8\\u94e3\\u524a\\u5149\\u5b66\\u73bb\\u7483\\u6750\\u6599\\u8868\\u9762\\u8d28\\u91cf\\u7814\\u7a76=>\\/qk\\/70459X\\/201617\\/83687174504849544955484953.html;;;High haze textured surface B-doped ZnO-TCO films on wet-chemically etched glass substrates for thin film solar cells=>\\/qk\\/94689X\\/201608\\/669996383.html;;;\\u73bb\\u7483\\u6df1\\u52a0\\u5de5\\u5de5\\u5382\\u7684\\u5de5\\u4e1a4\\uff0e0=>\\/qk\\/97223A\\/201608\\/670219816.html\",\"updatetime\":\"2017-04-24 18:02:48.087\",\"id\":\"236860\",\"journal_name\":\"\\u521b\\u65b0\\u4f5c\\u6587\\uff1a\\u5c0f\\u5b663-4\\u5e74\\u7ea7\",\"creator_2\":\"\\u6881\\u5434\",\"institute_2\":\"\\u5e7f\\u897f\\u5357\\u5b81\\u5e02\\u51e4\\u7fd4\\u8def\\u5c0f\\u5b66\\u56db(6)\\u73ed\",\"journal_2\":\"\\u521b\\u65b0\\u4f5c\\u6587\\uff1a\\u5c0f\\u5b66\\u5e74\\u7ea7\",\"issue\":\"2016\\u5e74\\u7b2c0\\u5377\\u7b2c11\\u671f 34-34\\u9875,\\u51711\\u9875\",\"subject_2\":\"TQ171.68\",\"guid\":\"6C424426-D528-E711-AECF-0050569B7A51\",\"status\":\"1\",\"from\":\"vip\"}"
//    val jsonStr = JSON.parseFull(str)
//    jsonStr match {
//      case Some(jsonMap: Map[String, Any]) =>
//        var creator = ""
//        var title = ""
//        var journal = ""
//        var id = ""
//        var institute = ""
//        var journal2 = ""
//        if (isUseful(jsonMap, "guid")) id = jsonMap("guid").toString
//        if (isUseful(jsonMap, "creator_2")) creator = cnkiOps.getFirstCreator(cnkiOps.cleanAuthor(jsonMap("creator_2").toString))
//        if (isUseful(jsonMap, "title")) title = cnkiOps.cleanTitle(jsonMap("title").toString)
//        if (isUseful(jsonMap, "journal_2")) journal = cnkiOps.cleanJournal(jsonMap("journal_2").toString)
//        if (isUseful(jsonMap, "journal_name")) journal2 = cnkiOps.cleanJournal(jsonMap("journal_name").toString)
//        println(journal)
//        if (journal == null || journal.equals("")) {
//          journal = journal2
//        }
//        if (isUseful(jsonMap, "institute_2")) institute = cnkiOps.getFirstInstitute(cnkiOps.cleanInstitute(jsonMap("institute_2").toString))
//        val key = cutStrOps(title, 6) + cutStrOps(journal, 4)
//        println(key + "---" + journal + "---" + creator + "---" + id + "---" + institute)
//    }
  }
}
