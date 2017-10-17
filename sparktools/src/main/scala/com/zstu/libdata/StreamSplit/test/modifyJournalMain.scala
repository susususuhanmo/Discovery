//package com.zstu.libdata.StreamSplit.test
//
//import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData, WriteData}
//
///**
//  * Created by Administrator on 2017/9/29 0029.
//  */
//object modifyJournalMain {
//  def main(args: Array[String]): Unit = {
//    val hiveContext  = CommonTools.initSpark("modifyJournalMain")
//    val standardData = ReadData.readData50("Discovery","t_journal_standard",hiveContext)
//    val coredData    = ReadData.readData50("DiscoveryV3","t_JournalCore",hiveContext)
//    val resultData   = AddJournalInfo.addCoreIssnDatasource(coredData,standardData,hiveContext)
//    WriteData.writeData50("DiscoveryV3","t_JournalKey",resultData)
//  }
//}
