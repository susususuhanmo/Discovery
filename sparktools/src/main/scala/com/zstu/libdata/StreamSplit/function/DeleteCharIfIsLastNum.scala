package com.zstu.libdata.StreamSplit.function

/**
 * Created by guguomin on 2017-05-26.
 */
//末尾如果是数字，就删除
object DeleteCharIfIsLastNum {
  def DeleteCharIfIsLastNum(str : String) : String = {
    var str1 = str
    for (s <- str) {
      if(s == '0' || s == '1' ||s == '2' ||s == '3' ||s =='4' ||s == '5' ||s == '6' ||s == '7' ||s == '8' ||s == '9' ){
        str1 = str1.replace(s.toString, "")
      }
    }
    str1
  }
}