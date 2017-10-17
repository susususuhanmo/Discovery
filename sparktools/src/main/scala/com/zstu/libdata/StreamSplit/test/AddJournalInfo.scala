package com.zstu.libdata.StreamSplit.test


import java.io.PrintWriter

import com.zstu.libdata.StreamSplit.function.deleteZero
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

/**
  * Created by Administrator on 2017/9/29 0029.
  */
object AddJournalInfo {
  val logger = new PrintWriter("./DiscoveryModify.txt")

  /**
    *
    * @param allColumCoreData  核心数据 要求有"CNKIKEY", "VIPKEY", "WFKEY", "ID", "ISSN", "DATAS这OURCE" 五个字段
    * @param originJournalData
    *                          drop("issn").drop("datasource").drop("isCore") 原始数据需要没有待添加的三个字段
    *                          且有defaultUrl字段
    * @param hiveContext
    * @return
    */

  def addCoreIssnDatasource(allColumCoreData: DataFrame, originJournalData: DataFrame, hiveContext: HiveContext,resource:String): DataFrame = {
    originJournalData.registerTempTable("originJournal")

    val coreData = allColumCoreData.select(resource.toUpperCase +"KEY", "ID", "ISSN", "DATASOURCE")
      .withColumnRenamed("ID", "journalCoreId")

    logger.println("coreData" + coreData.count() + DateTime.now + "\r\n")
    logger.flush()
    hiveContext.udf.register("deleteZero", (str: String) => if (str != "" && str != null) deleteZero.deleteZero(str) else null)

    hiveContext.udf.register("getKey", (url: String, resource: String) => if (url != "" && url != null) ParseUrl.GetKey(url, resource) else null)
    hiveContext.udf.register("parseResource", (url: String) => ParseUrl.ParseResource(url))
    val parsedJournalData = hiveContext.sql("select *," +
      "getKey(url,'"+ resource +"') as key," +
      "1 as isCore " +
      "from originJournal")
    matchData(parsedJournalData,coreData,resource,hiveContext)
  }

  def matchData(originData: DataFrame, allCoreData: DataFrame, resource: String, hiveContext: HiveContext): DataFrame = {

    val filteredData = originData
    logger.println(resource + "Data" + filteredData.count() + DateTime.now + "\r\n")
    logger.flush()
    val coreData =
      resource match {
        case "wf" =>   allCoreData.filter("WFKEY is not null")
        case "vip" =>  allCoreData.filter("VIPKEY is not null")
        case "cnki" => allCoreData.filter("CNKIKEY is not null")
      }
    resource match {
      case "wf" => filteredData.join(coreData, filteredData("key") === coreData("WFKEY")).drop("WFKEY").drop("key")
      case "vip" => filteredData.join(coreData, filteredData("key") === coreData("VIPKEY")).drop("VIPKEY").drop("key")
      case "cnki" => filteredData.join(coreData, filteredData("key") === coreData("CNKIKEY")).drop("CNKIKEY").drop("key")
    }
  }

}
