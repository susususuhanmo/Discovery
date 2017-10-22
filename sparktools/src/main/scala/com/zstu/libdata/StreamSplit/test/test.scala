package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.CommonTools.splitStr

/**
  * Created by Administrator on 2017/6/14 0014.
  */


object test {

  def cleanInstitute(institute: String): String = {
    val rtn = institute.replace("，", ",").replace("|!", ";")

    def getStrBefore(str: String): String = {
      if (str == null) null
      else {
        if (str.indexOf(",") >= 0) str.substring(0, str.indexOf(","))
        else str
      }
    }

    if (rtn == null) null
    else {
      val rtnArray = splitStr(rtn).map(getStrBefore(_).trim).filter(s => s != "" && s != null)
      if (rtnArray.isEmpty) null
      else rtnArray.reduce(_ + ";" + _)
    }
  }

  def main(args: Array[String]): Unit = {
    println(cleanInstitute(";;;"))


  }

}
