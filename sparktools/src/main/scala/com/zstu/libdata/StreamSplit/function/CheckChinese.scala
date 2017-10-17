package com.zstu.libdata.StreamSplit.function

/**
 * Created by Administrator on 2016/10/14.
 * 检查字符串是否全是中文字符，若含有非中文字符则返回1，若全为中文字符则返回2
 * 19968到19968+20901为汉字，183为汉字符号“·”
 */
object CheckChinese {
  def CheckChinese(str : String) : Int = {
    var str1 = str.trim()
    for(s <- str1){
      if((s.toInt < 19968 || s.toInt >19968+20901) && s.toInt != 183)
        return 1
    }
    2
  }

  //判断是否全英文字符串
  def CheckWholeEnglish(str:String):Boolean = {
    var retflag = true
    var str1 = str.trim()
    for(s <- str1){
      if((s.toInt >= 19968 && s.toInt <= 19968+20901) || s.toInt == 183)
        retflag = false
    }
    retflag
  }
//  def main(args:Array[String]) {
//    var s = "范德萨"
//    println(CheckChinese(s))
//  }
}
