package com.zstu.libdata.StreamSplit.function

/**
 * Created by guguomin on 2017-05-26.
 */
//末尾如果是数字，就删除
object DeleteAllNumInStr {

  def DeleteAllNumInStr(str : String) : String = {
    val lastchar = str(str.length -1)
    if(lastchar == '0' || lastchar == '1' ||lastchar == '2' ||lastchar == '3' ||lastchar =='4' ||lastchar == '5' ||lastchar == '6' ||lastchar == '7' ||lastchar == '8' ||lastchar == '9' ){
      str.substring(0,str.length - 1)
    }else{
      str
    }
  }
}
