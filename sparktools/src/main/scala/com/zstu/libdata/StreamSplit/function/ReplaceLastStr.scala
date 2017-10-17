package com.zstu.libdata.StreamSplit.function

/**
 * Created by Administrator on 2016/10/16.
 * 若Last为最后一个字符，则删除该字符。
 * 功能与DeleteCharIfIsLast相同，只不过输入参数可以以字符串的类型输入last字符
 */
object ReplaceLastStr {
    def ReplaceLastStr(str : String, last : String) : String = {
        val index = str.reverse.indexOf(last)
        if(index == 0)
          return str.substring(last.length - 1,str.length - 1)
        str
      }
  }