package com.zstu.libdata.StreamSplit.function

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by Administrator on 2017/1/19.
 */
object ReadData {
  def readFzjData(tableName: String, hiveContext: HiveContext): DataFrame = {
    hiveContext.udf.register("left", (str: String, length: Int) => if (str != null)
      if (str.length < length) str.substring(0, str.length) else str.substring(0, length)
    else null)
    val sqlUrl = "jdbc:sqlserver://192.168.1.200:1433;DatabaseName=FZJ;"
    val option = Map("url" -> sqlUrl, "user" -> "sa", "password" -> "sql2008r2", "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data.registerTempTable("dataTable")
    val dataSql = hiveContext.sql("select  id, " +
      "title, left(publisher,4) as shortpublisher, " +
      "creator," +
      "publisher as publisher," +
      "year," +
      "isbn " +
      " from dataTable")
    dataSql
  }
  def readData100(tableName: String, hiveContext: HiveContext): DataFrame = {

    val sqlUrl = "jdbc:sqlserver://192.168.1.100:1433;DatabaseName=FZJ;"
    val option = Map("url" -> sqlUrl, "user" -> "fzj", "password" -> "fzjfzj", "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data
  }
  def readDataV2(tableName: String, hiveContext: HiveContext): DataFrame = {

    val sqlUrl = "jdbc:sqlserver://192.168.1.160:1433;DatabaseName=FzjV2;"
    val option = Map("url" -> sqlUrl, "user" -> "fzj", "password" -> "fzj@zju", "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data
  }
  def readDataV3(tableName: String, hiveContext: HiveContext): DataFrame = {

    val sqlUrl = "jdbc:sqlserver://192.168.1.160:1433;DatabaseName=FzjV3;"
    val option = Map("url" -> sqlUrl, "user" -> "fzj", "password" -> "fzj@zju", "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data
  }
  def readDataLog(tableName: String, hiveContext: HiveContext): DataFrame = {

    val sqlUrl = "jdbc:sqlserver://192.168.1.160:1433;DatabaseName=Log;"
    val option = Map("url" -> sqlUrl, "user" -> "fzj", "password" -> "fzj@zju", "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data
  }
  def readDataCERSv4(tableName: String, hiveContext: HiveContext): DataFrame = {

    val sqlUrl = "jdbc:sqlserver://192.168.1.160:1433;DatabaseName=CERSv4;"
    val option = Map("url" -> sqlUrl, "user" -> "fzj", "password" -> "fzj@zju", "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data
  }


  def readData165(tableName: String, hiveContext: HiveContext): DataFrame = {

    val sqlUrl = "jdbc:sqlserver://192.168.1.165:1433;DatabaseName=WangZhihong;"
    val option = Map("url" -> sqlUrl, "user" -> "shm", "password" -> "shm@092011", "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data
  }

  def readData50(dataBase:String,tableName: String, hiveContext: HiveContext): DataFrame = {

    val sqlUrl = "jdbc:sqlserver://192.168.1.50:1433;DatabaseName="+dataBase+";"
    val option = Map("url" -> sqlUrl, "user" -> "shm", "password" -> "shm@zju", "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data
  }

}
