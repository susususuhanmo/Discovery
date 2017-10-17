package com.zstu.libdata.StreamSplit.function

import com.github.stuxuhai.jpinyin.{PinyinFormat, PinyinHelper}

/**
 * Created by Administrator on 2016/10/16.
 */
object convertToPinyin {
    def getPinyin(str : String) : String = {
        PinyinHelper.convertToPinyinString(str, " ", PinyinFormat.WITHOUT_TONE)
    }
//    def main(args:Array[String]) {
//        var str = "你好世界"
//        println(fn_GetPinyin(str))
//    }
}
