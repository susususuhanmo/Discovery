package com.zstu.libdata.StreamSplit.function

/**
  * Created by Administrator on 2017/7/13 0013.
  */
object deleteZero {


  def deleteZero(str : String) : String ={
    if(str == null) null
    else if (str.trim.length == 0) null
    else {
      var rtn =str.trim
      while(rtn(0) == '0'){
        if(rtn == "0") return "0"
        rtn = rtn.substring(1)
      }
      rtn
    }
  }

  def main(args: Array[String]): Unit = {
    println(deleteZero(""))
  }

}
