//package com.zstu.libdata.StreamSplit.function
//
//import com.zstu.libdata.StreamSplit.function.ReadData.readDataLog
//
///**
// * Created by SuHanmo on 2017/6/28.
// * AppName: isCore
// * Function:
// * Input Table:
// * Output Table:
// */
//object isCore {
//
//
//def isCore(journal: String) : Int ={
//  if(journal == null) 0
//
//  else {
//    for(coreJournal <- coreJournals){
//      if(coreJournal.indexOf(journal) >=0 ||
//        journal.indexOf(coreJournal) >=0
//      ) return 1
//    }
//    0
//  }
//}
//
//  def main(args: Array[String]) {
//    println(isCore("actageologicasinic"))
//  }
//}
