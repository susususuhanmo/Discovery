package com.zstu.libdata.StreamSplit.function

/**
 * Created by Administrator on 2016/10/14.
 * ?????????е????????
 */
object deleteInvisibleChar {
  def deleteInvisibleChar(str : String) : String = {
    var str1 = str.trim()
    for(s <- str1){
      if(s.toInt <=31 || s.toInt==127)
        str1 = str1.replace(s.toString,"")
    }
    str1
  }
}
