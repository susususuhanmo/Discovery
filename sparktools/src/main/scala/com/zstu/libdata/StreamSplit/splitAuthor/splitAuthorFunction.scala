package com.zstu.libdata.StreamSplit.splitAuthor

import com.github.stuxuhai.jpinyin.{PinyinFormat, PinyinHelper}
import com.zstu.libdata.StreamSplit.function.CommonTools._
import com.zstu.libdata.StreamSplit.function.Filter
import com.zstu.libdata.StreamSplit.function.WriteData.writeDataLog
import com.zstu.libdata.StreamSplit.function.getFirstLevelOrgan.getFirstLevelOrgan
import com.zstu.libdata.StreamSplit.function.keywordsOps.isNull
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.immutable.IndexedSeq

/**
  * Created by Administrator on 2017/5/5 0005
  *
  */

object splitAuthorFunction {

  case class candidateResource(id: String, kId: String, unionId: String)

  case class paperAuthor(paperId: String, name: String, organ: String, partOrgan: String, journal: String
                         , isFirst: Boolean, firstLevelOrgan: String, authorId: String)

  case class expert(operater: String, id: String, name: String, pinyin: String
                    , subject: String, updateSubject: String, keyword: String, keywordAlt: String,
                    updateKeyword: String, updateKeywordAlt: String, organization: String)

  case class splitedData(name: String, organ: String, firstLevelOrgan: String
                         , partOrgan: String, isFirst: Boolean, id: String, journal: String)

  case class keyWordData(subjectCode: String, paperid: String, year: String, keyword: String, keywordAlt: String
                         , subjectName: String)

  case class candidateReturn(idCandidate: String, candidateResources: String)

  //    (subectCode, id, year, CNArray(i), EN, subjectName)
  def distinctAndCatStr(str1: String, str2: String) = {
    if (str1 == null) str2
    else if (str2 == null) str1
    else str1.split(";")
      .union(str2.split(";"))
      .distinct
      .reduce(_ + ";" + _)
  }


  def makeXMLString(paperId: String, name: String, authorId: String): String = {
    "<resource><ID>" + paperId + "</ID><kid>" + authorId + "</kid><name>" + name + "</name></resource>"
  }


  def formatForKeyword(value: (String, String, String, String, String, String, String, String, Any)): (String, (String, String, String)) = {
    //    (paperId,name,organ,partOrgan,journal,isFirst,firstLevelOrgan,authorId,any:(keywordNew,keywordAltNew,subjectNew,paperId,journal))
    val (paperId, name, organ, partOrgan, journal, isFirst, firstLevelOrgan, authorId, anyValue) = value
    if (anyValue == null) return null
    val (keywordNew, keywordAltNew, subjectNew, paperId1, journal1) = getAny5(anyValue)
    (paperId, (keywordNew, keywordAltNew, subjectNew))
  }

  def keyWordSplitNew(value: (String, (String, String, String))) = {
    val (id, (keywordNew, keywordAltNew, subjectName)) = value
    val subectCode = null
    val year = null
    if (isNull(keywordNew) && isNull(keywordAltNew)) {
      //中英文均为空 返回原值
      for (i <- 0 to 0) yield (subectCode, id, year, null, null, subjectName)
    }
    else if (isNull(keywordNew)) {
      //中文为空拆分英文
      val ENArray = keywordAltNew.split(";")
      for (i <- ENArray.indices)
        yield (subectCode, id, year, null, ENArray(i), subjectName)
    }
    else if (isNull(keywordAltNew)) {
      //英文为空拆分中文
      val CNArray = keywordNew.split(";")
      for (i <- CNArray.indices)
        yield (subectCode, id, year, CNArray(i), null, subjectName)
    }
    else {
      //都不为空为一一对应，全部拆分。若有不为一一对应的，返回原值
      val CNArray = keywordNew.split(";")
      val ENArray = keywordAltNew.split(";")
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


  def keyWordSplit(value: (String, (String, String, String))): IndexedSeq[keyWordData] = {
    //    (paperId,name,organ,partOrgan,journal,isFirst,firstLevelOrgan,authorId,any:(keywordNew,keywordAltNew,subjectNew,paperId,journal))
    val (paperId, (keywordNew, keywordAltNew, subjectNew)) = value
    val keyWords = splitStr(keywordNew, mode = "clean")
    val keyWordAlt = splitStr(keywordAltNew, mode = "clean")
    if (keyWords == null && keyWordAlt == null) return null
    //    case class keyWordData(paperid: String,year:String,keyword: String,keywordAlt: String
    //                           ,subjectCode: String,subjectName: String)

    def unionInclueNull(arrayA: IndexedSeq[keyWordData], arrayB: IndexedSeq[keyWordData]) = {
      if (arrayA == null) arrayB
      else if (arrayB == null) arrayA
      else arrayA.union(arrayB)
    }


    val keyWordsResult = if (keyWords == null) null
    else for (i <- keyWords.indices)
      yield keyWordData(paperId, null, keyWords(i), null, null, subjectNew)

    val keyWordsAltResult = if (keyWordAlt == null) null
    else for (j <- keyWordAlt.indices)
      yield keyWordData(paperId, null, null, keyWordAlt(j), null, subjectNew)
    unionInclueNull(keyWordsResult, keyWordsAltResult)

  }


  def getAny5(any: Any): (String, String, String, String, String) = {
    //(keywordNew,keywordAltNew,subjectNew,paperId,journal)
    def myToString(a: Any): String = {
      if (a == null) null
      else a.toString
    }

    any match {
      case (a, b, c, d, e) => (myToString(a), myToString(b), myToString(c), myToString(d), myToString(e))
      case _ => (null, null, null, null, null)
    }
  }

  def getAny4(any: Any): (String, String, String, String) = {
    //(id,subject,keyword,keywordAlt)
    def myToString(a: Any): String = {
      if (a == null) null
      else a.toString
    }

    any match {
      case (a, b, c, d) => (myToString(a), myToString(b), myToString(c), myToString(d))
      case _ => (null, null, null, null)
    }
  }

  /** 拆分单条数据
    *
    * @param value 待拆分数据((allAuthors, allOrgins), otherValue)
    * @return 拆分作者机构后的多条数据IndexedSeq[(author, orgin, firstLevelOrgan, isFirst(i), otherValue，type)]
    *         type：1、值为-1的为一对一匹配
    *         2、值不为-1的为机构序号，0即为第一机构。
    * */
  def splitAutorAndOrgan(value: ((String, String), Any)):
  IndexedSeq[(String, String, String, String, Any, Int)] = {

    val ((allAuthors, allOrgins), otherValue) = ((value._1._1, value._1._2), value._2)
    //    logUtil("part1开始" + allAuthors)
    //    logUtil(allOrgins)
    if (isMeaningless(allAuthors) || allOrgins == null) {
      null
    } else {
      //将作者和机构拆分成两个数组
      val authors = splitStr(allAuthors, mode = "clean")
      val organs = splitStr(allOrgins, mode = "clean")

      def isFirst(i: Int): String = {
        if (i == 0) "1" else "0"
      }

      if (authors == null || organs == null || authors.isEmpty || organs.isEmpty) {
        null
      }
      else if (authors.length == organs.length) {
        for (i <- authors.indices)
          yield (authors(i), organs(i), getFirstLevelOrgan(organs(i)), isFirst(i), otherValue, -1)
      }
      else if (authors.nonEmpty && organs.length == 1) {
        for (i <- authors.indices)
          yield (authors(i), organs(0), getFirstLevelOrgan(organs(0)), isFirst(i), otherValue, -1)
      }
      else {
        for (i <- authors.indices; j <- organs.indices)
          yield (authors(i), organs(j), getFirstLevelOrgan(organs(j)), isFirst(i), otherValue, j)
      }
    }
  }


  /**
    *
    * @param value1 value1
    * @param value2 value2
    * @return
    */
  def reduceAuthorNew(value1: (((String, String), (String, String, String, String, Any, Int, Any)), Double)
                      , value2: (((String, String), (String, String, String, String, Any, Int, Any)), Double)): (((String, String), (String, String, String, String, Any, Int, Any)), Double) = {
    //    val (author1, orgin1, firstLevelOrgan1, isFirstuthor1, otherValue1, organNum1, authorId1) = value1._1
    //    val (author2, orgin2, firstLevelOrgan2, isFirstuthor2, otherValue2, organNum2, authorId2) = value1._1

    if (value1._2 > value2._2) value1 else value2
  }


  /**
    * 得到优先级
    *
    * @param value ： ((author, orgin, firstLevelOrgan, isFirstuthor, otherValue,organNum),authorId)
    * @return
    */
  def getLevel(value: ((String, String), ((String, String, String, String, Any, Int), Option[Any])))
  : Double = {
    val ((author, orgin, firstLevelOrgan, isFirstuthor, otherValue, organNum), authorId) = value._2
    var level = 0
    if (authorId.orNull != null) level += 8
    if (organNum == -1) level += 4
    if (isFirstuthor == "1" && organNum == 0) level += 2
    if (isFirstuthor != "1" && organNum == 0) level += 1
    val length = getLength(orgin)
    val lengthLevel = 1 - (if (length != 0) length / 100 else 1)
    level + lengthLevel
  }


  def toOldData(value: (String, String, String, String, Any, Int, Any)) = {
    def fn_GetPinyin(str: String): String = {
      PinyinHelper.convertToPinyinString(str, " ", PinyinFormat.WITHOUT_TONE)
    }

    val (author, organ, firstLevelOrgan, isFirstuthor, otherValue, organNum, joinValue) = value
    //    case class expert(operater: String,id: String,name: String,pinyin: String
    //                      ,subject: String,updateSubject: String,keyword: String,keywordAlt: String,
    //                      updateKeyword: String,updateKeywordAlt: String,organization: String)
    val (id, subject, keyword, keywordAlt) = getAny4(joinValue)
    val (keywordNew, keywordAltNew, subjectNew, paperId, journal) = getAny5(otherValue)
    //    (jsonMap("keyword"), null, jsonMap("subject"), jsonMap("id"), jsonMap("journal"))
    val updateSubject = distinctAndCatStr(subject, subjectNew)
    val updateKeyword = distinctAndCatStr(keyword, keywordNew)
    val updateKeywordAlt = distinctAndCatStr(keywordAlt, keywordAltNew)
    expert("2", id: String, author: String, fn_GetPinyin(author): String
      , subjectNew: String, updateSubject: String, keywordNew: String, keywordAltNew: String,
      updateKeyword: String, updateKeywordAlt: String, firstLevelOrgan: String)
  }

  def toNewData(value: (String, String, String, String, Any, Int, String)) = {
    def fn_GetPinyin(str: String): String = {
      PinyinHelper.convertToPinyinString(str, " ", PinyinFormat.WITHOUT_TONE)
    }

    val (author, organ, firstLevelOrgan, isFirstuthor, otherValue, organNum, joinValue) = value
    //    case class expert(operater: String,id: String,name: String,pinyin: String
    //                      ,subject: String,updateSubject: String,keyword: String,keywordAlt: String,
    //                      updateKeyword: String,updateKeywordAlt: String,organization: String)
    val id = joinValue
    val (keywordNew, keywordAltNew, subjectNew, paperId, journal) = getAny5(otherValue)
    //    (jsonMap("keyword"), null, jsonMap("subject"), jsonMap("id"), jsonMap("journal"))
    val updateSubject = subjectNew
    val updateKeyword = keywordNew
    val updateKeywordAlt = keywordAltNew
    expert("1", id: String, author: String, fn_GetPinyin(author): String
      , subjectNew: String, updateSubject: String, keywordNew: String, keywordAltNew: String,
      updateKeyword: String, updateKeywordAlt: String, firstLevelOrgan: String)
  }


  /**
    *
    * @param inputRdd
    * @param authorRdd
    * @param CLCRdd
    * @param universityData
    * @param yearData
    * @param hiveContext
    * @return
    */
  def splitRdd(inputRdd: RDD[((String, String), Any)], authorRdd: RDD[((String, String), Any)],
               CLCRdd: (RDD[(String, (String, String))]), universityData: DataFrame,
               yearData: DataFrame, hiveContext: HiveContext)
  = {

    val cleanedInputRdd = inputRdd.filter(value => !isMeaningless(value._1._1) && !isMeaningless(value._1._2))
    val splitedRdd: RDD[(String, String, String, String, Any, Int)] = cleanedInputRdd.flatMap(splitAutorAndOrgan)
      .filter(f => f != null)
      .filter(value => Filter.authorFilter(value._1))
      .map(value => (value._1, value._2, value._3, value._4, value._5, value._6))
      .filter(value => value._1.length > 1 && value._2.length > 1 && !Filter.isOverlongName(value._1) && Filter.organFilter(value._2))


    //(author, orgin, firstLevelOrgan, isFirstuthor, otherValue，organNum)
    val (oneToOneRdd, notOneToOneRdd) = (splitedRdd.filter(_._6 == -1).map(value => ((value._1, cutStr(value._3, 4)), value)), splitedRdd.filter(_._6 != -1))

    // 一、取非一对一可能为新的数据:第一作者第一机构
    val (firstAuthorMatched, otherAuthor)
    = (notOneToOneRdd.filter(value => value._6 == 0 && value._4 == "1").map(value => ((value._1, cutStr(value._3, 4)), value))
      , notOneToOneRdd.filter(value => value._4 != "1").map(value => ((value._1, cutStr(value._3, 4)), value)))

    //  剔除掉第一作者的其他机构，和作者表进行匹配。


    //(author,id),(author, orgin, firstLevelOrgan, isFirstuthor, otherValue，organNum,(id,subject,keyword,keywordAlt))
    val joinedRdd: RDD[((String, String), (((String, String), (String, String, String, String, Any, Int, Any)), Double))]
    = otherAuthor.union(firstAuthorMatched).union(oneToOneRdd)
      .leftOuterJoin(authorRdd)
      .map(value =>
        ((value._1._1, getAny5(value._2._1._5)._1), ((value._1, (value._2._1._1, value._2._1._2, value._2._1._3, value._2._1._4, value._2._1._5, value._2._1._6, value._2._2.orNull)), getLevel(value))))
      .filter(value => value._2._2 >= 1)


    // 找到新数据和旧数据
    //    (author, orgin, firstLevelOrgan, isFirstuthor, otherValue，organNum,(id,subject,keyword,keywordAlt))
    val exactAuthor: RDD[(String, String, String, String, Any, Int, Any)]
    = joinedRdd.reduceByKey(reduceAuthorNew)
      .map(value => value._2._1)
      .reduceByKey((value1, value2) => if (value1._3 > value2._3) value2 else value1)
      .map(value => value._2)


    val (oldAuthor, newAuthorWithoutId) = (exactAuthor.filter(value => value._7 != null), exactAuthor.filter(value => value._7 == null))

    val newAuthor: RDD[(String, String, String, String, Any, Int, String)] =
      newAuthorWithoutId.map(value => (value._1, value._2, value._3, value._4, value._5, value._6, newGuid())).cache()

    //(keywordNew,keywordAltNew,subjectNew,paperId,journal)


    val oldAuthorWithSubject = oldAuthor

    //      addCLCRddOld(oldAuthor,CLCRdd)

    val newAuthorWithSubject = newAuthor


    //    (author, organ, firstLevelOrgan, isFirstuthor, otherValue,organNum,joinValue)
    //    ((name ,partOrgan),(id,any))
    val authorIdRddNew = newAuthorWithSubject
      .map(
        value => (
          (value._1, cutStr(value._3, 4)), (value._7.toString, value._5)
        )
      )

    val authorIdRddOld = oldAuthorWithSubject.map(
      value => (
        (value._1, cutStr(value._3, 4)), (getAny4(value._7)._1, value._5)
      )
    )
    val authorIdRdd = authorIdRddNew.union(authorIdRddOld)


    val newDataWithoutUniversity = hiveContext
      .createDataFrame(newAuthorWithSubject.map(toNewData))

    val oldDataWithoutUniversity = hiveContext
      .createDataFrame(oldAuthorWithSubject.map(toOldData))

    val authorDataWithoutUniversity = oldDataWithoutUniversity.unionAll(newDataWithoutUniversity)


    val resultAuthorData = authorDataWithoutUniversity.join(universityData, authorDataWithoutUniversity("organization") === universityData("university"), "left")
      .drop("university")


    //(name,organ,firstLevelOrgan,isFirst,Any:(keywordNew,keywordAltNew,subjectNew,paperId,journal))


    //    ((name,partOrgan),(paperId,name,organ,partOrgan,journal,isFirst,firstLevelOrgan))
    val paperAuthorWithoutId: RDD[((String, String), (String, String, String, String, String, String, String))] = splitedRdd.map(value =>
      ((value._1, cutStr(value._3, 4)),
        (getAny5(value._5)._4, value._1, value._2, cutStr(value._3, 4), getAny5(value._5)._5, value._4, value._3)
      )
    )

    def getLength(str: String) = {
      if (str == null) 0
      else str.length
    }

    //    (paperId,name,organ,partOrgan,journal,isFirst,firstLevelOrgan,authorId,any)
    val paperAuthorRdd: RDD[(String, String, String, String, String, String, String, String, Any)]
    = paperAuthorWithoutId.join(authorIdRdd).map(value =>
      (
        (value._2._1._1, value._2._1._2),
        (value._2._1._1, value._2._1._2, value._2._1._3, value._2._1._4, value._2._1._5, value._2._1._6, value._2._1._7, value._2._2._1, value._2._2._2)
      )).reduceByKey((value1, value2) => if (getLength(value1._7) > getLength(value2._7)) value2 else value1)
      .map(value => value._2)

    //    case class paperAuthor(paperId:String,name: String,organ: String,partOrgan: String,journal: String
    //                           ,isFirst: Boolean,firstLevelOrgan:String,authorId: String)
    val paperAuthorData = hiveContext.createDataFrame(paperAuthorRdd.map(value =>
      paperAuthor(value._1, value._2, value._3, value._4, value._5,
        if (value._6 == "0") false else true,
        value._7, value._8)
    ))
    writeDataLog("t_PaperAuthorLog", paperAuthorData)
    val paperAuthorId = paperAuthorData.select("authorId")

    val finalAuthorData = resultAuthorData.join(paperAuthorId,
      resultAuthorData("id") === paperAuthorId("authorId"), "left")
      .filter("authorId is not null")
      .drop("authorId")
      .distinct()

    writeDataLog("tmp_ExpertLog", finalAuthorData)


    val candidateResourceRdd = paperAuthorRdd.map(value =>
      (newGuid(), value._8, value._1, value._9, value._8, value._2)).cache
    val candidateResourceData = hiveContext.createDataFrame(
      candidateResourceRdd.map(value => candidateResource(value._1, value._2, value._3))
    )
    writeDataLog("t_CandidateResourceLog", candidateResourceData)
    //    (paperId,name,organ,partOrgan,journal,isFirst,firstLevelOrgan
    // ,authorId,any:(keywordNew,keywordAltNew,subjectNew,paperId,journal))

    val keyWord1 = paperAuthorRdd.map(formatForKeyword)
    val keyWord2 = keyWord1.filter(f => f != null)
    val keyWord3 = keyWord2.reduceByKey((value1, value2) => value1)

    val keyWord = keyWord3.flatMap(keyWordSplitNew).filter(f => f != null)
      .map(value =>
        keyWordData(value._1, value._2, value._3, value._4, value._5, value._6))


    //    case class keyWordData(paperid: String,year:String,keyword: String
    //                           ,keywordAlt: String
    //                           ,subjectCode: String,subjectName: String)

    val keyWordResultData = hiveContext.createDataFrame(keyWord).drop("year")

    val keyWordResultDataWithYear = keyWordResultData.join(yearData,
      keyWordResultData("paperid") === yearData("id"), "left")
      .drop("id").drop("isCheck")
    writeDataLog("t_KeywordLog", keyWordResultDataWithYear)
    //    (paperId,name,organ,partOrgan,journal,isFirst,firstLevelOrgan
    // ,authorId,any:(keywordNew,keywordAltNew,subjectNew,paperId,journal))


    //    val candidateResourceRdd = paperAuthorRdd.map(value =>
    //      (newGuid(),value._8,value._1,value._9,value._8,value._2)).cache
    //    unionid,authorId,paperid,any,authorid,name


    val journalLogData = candidateResourceRdd.map(value =>
      (
        value._3, (makeXMLString(value._1, value._6, value._2), getAny5(value._4)._3)
      )
    ).reduceByKey(
      (value1, value2) => (value1._1 + value2._1, value1._2)
    )
      .map(value =>
        (
          value._1, (cutStr("<candidateResources>" + value._2._1 + "</candidateResources>", 4000), value._2._2)
        )).map(value => candidateReturn(value._1, value._2._1))
    //    case class  candidateReturn(idCandidate:String,candidateResource:String,subject:String)
    (authorRdd, journalLogData)

  }


}
