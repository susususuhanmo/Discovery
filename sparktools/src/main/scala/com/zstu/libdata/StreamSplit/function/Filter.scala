package com.zstu.libdata.StreamSplit.function



/**
 * Created by Administrator on 2017/2/19.
 */
object Filter {

  def oneToOneFilter(value : (String,String,String,String)) : Boolean={
    def trimStr(str : String) = {
      if (str == null) str
      else str.trim
    }
    val (allAuthors,allOrgins,journal,journalId) = value
    if(allAuthors == null || allOrgins == null || allAuthors == "无"|| allAuthors == "不详"){
      return false
    }
    val authors =  ReplaceLastStr.ReplaceLastStr(
      allAuthors.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";").map(trimStr)

    val orgins =  ReplaceLastStr.ReplaceLastStr(
      allOrgins.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";").map(trimStr)

    if(authors == null || orgins == null){
      false
    } else if(authors.length == orgins.length) {
      true
    } else if(authors.nonEmpty &&  orgins.length == 1){
      true
    } else false
  }
  def notOneToOneFilter(value : (String,String,String,String)) : Boolean={
    def trimStr(str : String) = {
      if (str == null) str
      else str.trim
    }
    val (allAuthors,allOrgins,journal,journalId) = value
    if(allAuthors == null || allOrgins == null || allAuthors == "无"|| allAuthors == "不详"){
      return false
    }
    val authors =  ReplaceLastStr.ReplaceLastStr(
      allAuthors.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";").map(trimStr)

    val orgins =  ReplaceLastStr.ReplaceLastStr(
      allOrgins.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";").map(trimStr)

    if(authors == null || orgins == null){
      false
    } else if(authors.length == orgins.length) {
      false
    } else if(orgins.length == 1){
      false
    }else if(authors == null || orgins == null||authors.isEmpty||orgins.isEmpty)
      false
    else true
  }

def ThreeToTowFilter(value : (String,String,String,String)) : Boolean={
  val (allAuthors,allOrgins,journal,journalId) = value
  if(allAuthors == null || allOrgins == null || allAuthors == "无"|| allAuthors == "不详"
    ||allAuthors.length == 0){
    return false
  }
  val authors = ReplaceLastStr.ReplaceLastStr(
    allAuthors.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";")
  val orgins = ReplaceLastStr.ReplaceLastStr(
    allOrgins.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";")
  if(authors == null || orgins == null){
    false
  } else if(authors.length == 3 && orgins.length == 2) {
    true
  } else false
}
  def fourToTowFilter(value : (String,String,String,String)) : Boolean={
    val (allAuthors,allOrgins,journal,journalId) = value
    if(allAuthors == null || allOrgins == null || allAuthors == "无"|| allAuthors == "不详"
      ||allAuthors.length == 0){
      return false
    }
    val authors = ReplaceLastStr.ReplaceLastStr(
      allAuthors.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";")
    val orgins = ReplaceLastStr.ReplaceLastStr(
      allOrgins.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";")
    if(authors == null || orgins == null){
      false
    } else if(authors.length >= 4 && orgins.length == 2) {
      true
    } else false
  }
  
  def fourToThreeFilter(value : (String,String,String,String)) : Boolean={
    val (allAuthors,allOrgins,journal,journalId) = value
    if(allAuthors == null || allOrgins == null || allAuthors == "无"|| allAuthors == "不详"
      ||allAuthors.length == 0){
      return false
    }
    val authors = ReplaceLastStr.ReplaceLastStr(
      allAuthors.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";")
    val orgins = ReplaceLastStr.ReplaceLastStr(
      allOrgins.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";")
    if(authors == null || orgins == null){
      false
    } else if(authors.length >= 4 && orgins.length == 3) {
      true
    } else false
  }

  def fourToFourFilter(value : (String,String,String,String)) : Boolean={
    val (allAuthors,allOrgins,journal,journalId) = value
    if(allAuthors == null || allOrgins == null || allAuthors == "无"|| allAuthors == "不详"
      ||allAuthors.length == 0){
      return false
    }
    val authors = ReplaceLastStr.ReplaceLastStr(
      allAuthors.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";")
    val orgins = ReplaceLastStr.ReplaceLastStr(
      allOrgins.trim.replace("；",";").replace(",",";").replace("，",";"),";").split(";")
    if(authors == null || orgins == null){
      false
    } else if(authors.length >= 4 && orgins.length == 4) {
      true
    } else false
  }
  def firstFilter(organ: String,isFirst:String) ={
    if(organ == null) false
    else if(isFirst == "1" && organ.indexOf("《")>=0) false
    else true
  }

  def organFilter(organ: String) = {
    def isAllNum(str: String): Boolean ={
      for(c <- str ){
        if(c.toInt<48||c.toInt>57)
          return false
      }
      true
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
    if(organ == null
    ) false
    else if (organ.indexOf("不详") >= 0
      || organ.indexOf("无") >= 0
      || organ.indexOf("本刊") >= 0
      || organ.length == 0) false
    else if (isAllNum(organ))
      false
    else if (hasNoChinese(organ)) false
    else true
  }
  def isOverlongName(name: String): Boolean ={
    //名字长度大于四，且不为新疆名字的被认为是过长的名字
    if(name == null) true
    else if(name.length >4 && (name.indexOf("·") <0))
      true
    else false
  }
  def authorFilter(author: String) ={
    def hasNoChinese(str: String): Boolean ={
      if(str == null) return false
      val str1 = str.trim()
      for(s <- str1){
        if(s.toInt >= 19968 && s.toInt <=19968+20901)
          return false
      }
      true
    }
    def moreThanThreeAndEndWithKeywords(str: String,keywords: String): Boolean ={
      if(str ==null) false
      else if(str.length <3) false
      else if(str.indexOf(keywords) == str.length - keywords.length)
        true
      else false
    }
    def hasOtherChar(str: String):Boolean = {
      if (str == null) return true
      for (c <- str) {
        if (!(c.toInt >= 19968 && c.toInt <= 19968 + 20901) )
             if(c != '·')
               return true
      }
      false
    }
    def hasEng(str: String): Boolean ={
      if(str == null) return true
      val str1 = str.trim()
      for(s <- str1){
        if((s.toInt >= 65 && s.toInt <=90)||(s.toInt >= 97 && s.toInt <=122))
          return true
      }
      false
    }

    if (author == null ) false
    else if(hasEng(author) ||
      hasNoChinese(author))
      false
      else if(isOverlongName(author))
        false
      else if(hasOtherChar(author))

        false

    else if( author.length == 1||
      author.length ==0 ||
      author.indexOf("无") >= 0
      || author.indexOf("不详") >= 0
      ||author.indexOf("课题组") >= 0
      ||author.indexOf("工作组") >= 0
      ||author.indexOf("实践队") >= 0
      ||author.indexOf("调查组") >= 0
      ||author.indexOf("调研组") >= 0
      ||author.indexOf("考察组") >= 0
      ||author.indexOf("项目组") >= 0
      ||author.indexOf("研究组") >= 0
      ||author.indexOf("编写组") >= 0
      ||author.indexOf("筹备组") >= 0
      ||author.indexOf("编辑部") >= 0
      ||author.indexOf("组委会") >= 0
      ||author.indexOf("写作") >= 0
      ||author.indexOf("编辑") >= 0
      ||author.indexOf("编委会") >= 0
      ||author.indexOf("协作组") >= 0
      ||author.indexOf("委员会") >= 0
      ||author.indexOf("整理组") >= 0
      ||author.indexOf("记者") >= 0
      ||author.indexOf("本刊") >= 0
      ||author.indexOf("杂志") >= 0
      ||author.indexOf("财政") >= 0
      ||author.indexOf("办公室") >= 0
      ||author.indexOf("考察团") >= 0
      ||author.indexOf("市委") >= 0
      ||author.indexOf("图书馆") >= 0
      ||author.indexOf("修订组") >= 0
      ||author.indexOf("设计组") >= 0
      ||author.indexOf("发展改革委") >= 0
      ||author.indexOf("发改委") >= 0
      ||author.indexOf("代表处") >= 0
      ||author.indexOf("通讯员") >= 0
      ||author.indexOf("培训团") >= 0
      ||author.indexOf("管理处") >= 0
      ||author.indexOf("撰写组") >= 0
      ||author.indexOf("调研") >= 0
      ||author.indexOf("中心") >= 0
      ||author.indexOf("研究所") >= 0
      ||author.indexOf("大学") >= 0
      ||author.indexOf("学院") >= 0
      ||author.indexOf("医院") >= 0
      ||author.indexOf("公司") >= 0
      ||author.indexOf("专家组") >= 0
      ||author.indexOf("管理所") >= 0
      ||moreThanThreeAndEndWithKeywords(author,"网")
      ||moreThanThreeAndEndWithKeywords(author,"网站")

    ) false
    else true
  }

  def main(args: Array[String]) {
    val author = "孙胜·永范德萨"
    println( Filter.isOverlongName(author))

  }
}
