package com.zstu.libdata.StreamSplit.test

/**
  * Created by Administrator on 2017/6/13 0013.
  */
object generateCode {

def generateGetCod(str: String) ={
  "\ngetRowString(r,\""+ str + "\")"
}
  def  generateReadCode(str: String) ={
    var rtn = str.replace(" ","")
    val rowNameArray: Array[(String, String)] = str.split(",")
      .map(str=> str.replace("(","").replace(")",""))
      .filter(str => str.length > 0)
    .map(str => (str,generateGetCod(str)))
val sortedrowNameArray = rowNameArray.sortWith(
  (value1,value2)=> {
    val (str1,str2) = (value1._1,value2._1)
    if(str1.indexOf(str2) >= 0) true
    else if(str1.indexOf(str2) >= 0) false
    else false
  })
//    println("-------------")
//    sortedrowNameArray.foreach(value =>println(value._1))
//    println("-------------")
//    rowNameArray.foreach(println)


    for(i<-rowNameArray.indices) {
      rtn = rtn.replace(rowNameArray(i)._1+",", rowNameArray(i)._2+",")
      rtn = rtn.replace(rowNameArray(i)._1+")", rowNameArray(i)._2+")")
    }
    rtn
  }
  def main(args: Array[String]): Unit = {

    val str = "(id, (id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url,abstractcont,abstractcont_alt))"


    println(generateReadCode(str.replace(" ","")))


  }
}
