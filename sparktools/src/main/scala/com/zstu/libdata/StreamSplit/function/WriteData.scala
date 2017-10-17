package com.zstu.libdata.StreamSplit.function

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}


/**
 * Created by Administrator on 2017/1/18.
 * 将数据写入sql server函数
 * 分为匹配成功的数据，和未匹配上的数据
 * 1、匹配成功的数据将匹配信息写入信息补全的表中
 * 2、未匹配上的数据为新数据，直接写入大表中。
 * 3、接收数据类型为RDD[(String,...,String)]
 * 通过toMatchedDataFrame，toNotMatchedDataFrame将其转换为DataFrame（添加列名）
 */
object WriteData {
  def writeData100(tableName: String,dataFrame: DataFrame,hiveContext: HiveContext): Unit = {
    //设置好连接属性用于写数据
    val connectProperties = new Properties()
    connectProperties.put("user", "fzj")
    connectProperties.put("password", "fzjfzj")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.100:1433;DatabaseName=FZJ;"
    dataFrame.write.mode(SaveMode.Append).jdbc(sqlUrl, tableName, connectProperties)

  }

  def writeData50(dataBase : String,tableName: String,dataFrame: DataFrame): Unit = {
    //设置好连接属性用于写数据
    val connectProperties = new Properties()
    connectProperties.put("user", "shm")
    connectProperties.put("password", "shm@zju")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.50:1433;DatabaseName="+dataBase+";"
    dataFrame.write.mode(SaveMode.Append).jdbc(sqlUrl, tableName, connectProperties)

  }

  def writeDataDiscovery(tableName: String,dataFrame: DataFrame,hiveContext: HiveContext): Unit = {
    //设置好连接属性用于写数据
    val connectProperties = new Properties()
    connectProperties.put("user", "shm")
    connectProperties.put("password", "shm@092011")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.100:1433;DatabaseName=Discovery;"
    dataFrame.write.mode(SaveMode.Append).jdbc(sqlUrl, tableName, connectProperties)

  }
  def writeDataV2(tableName: String,dataFrame: DataFrame,hiveContext: HiveContext): Unit = {
    //设置好连接属性用于写数据
    val connectProperties = new Properties()
    connectProperties.put("user", "fzj")
    connectProperties.put("password", "fzj@zju")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.160:1433;DatabaseName=FzjV2;"
    dataFrame.write.mode(SaveMode.Append).jdbc(sqlUrl, tableName, connectProperties)

  }
  def writeDataLog(tableName: String, dataFrame: DataFrame): Unit = {
    //设置好连接属性用于写数据
    val connectProperties = new Properties()
    connectProperties.put("user", "fzj")
    connectProperties.put("password", "fzj@zju")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.160:1433;DatabaseName=Log;"
    dataFrame.write.mode(SaveMode.Append).jdbc(sqlUrl, tableName, connectProperties)

  }
  def writeDataLc(tableName: String, dataFrame: DataFrame): Unit = {
    //设置好连接属性用于写数据
    val connectProperties = new Properties()
    connectProperties.put("user", "fzj")
    connectProperties.put("password", "fzj@zju")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.160:1433;DatabaseName=LC;"
    dataFrame.write.mode(SaveMode.Append).jdbc(sqlUrl, tableName, connectProperties)

  }
  def writeDataWangzhihong(tableName: String, dataFrame: DataFrame): Unit = {
    //设置好连接属性用于写数据
    val connectProperties = new Properties()
    connectProperties.put("user", "shm")
    connectProperties.put("password", "shm@092011")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.165:1433;DatabaseName=WangZhihong;"
    dataFrame.write.mode(SaveMode.Append).jdbc(sqlUrl, tableName, connectProperties)

  }
  def writeDataDiscoveryV2(tableName: String,dataFrame: DataFrame): Unit = {
    //设置好连接属性用于写数据
    val connectProperties = new Properties()
    connectProperties.put("user", "fzj")
    connectProperties.put("password", "fzj@zju")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.50:1433;DatabaseName=DiscoveryV2;"
    dataFrame.write.mode(SaveMode.Append).jdbc(sqlUrl, tableName, connectProperties)

  }
  def writeDataV3(tableName: String,dataFrame: DataFrame): Unit = {
    //设置好连接属性用于写数据
    val connectProperties = new Properties()
    connectProperties.put("user", "fzj")
    connectProperties.put("password", "fzj@zju")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.160:1433;DatabaseName=FzjV3;"
    dataFrame.write.mode(SaveMode.Append).jdbc(sqlUrl, tableName, connectProperties)

  }
  def writeMatchedData(tableName: String, dataRDD: RDD[(String,String,String, String, String, String, String, String, String, String, String, String,String, String)],
                matchCategory: String, hiveContext: HiveContext): Unit = {
    //设置好连接属性用于写数据
    val connectProperties = new Properties()
    connectProperties.put("user", "sa")
    connectProperties.put("password", "sql2008r2")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.200:1433;DatabaseName=FZJ;"
    //匹配来源id

    //可能之后改表名，到时修改此match
      val category = tableName match {
        case "Beishida" => "0508"
        case "Calis" => "0501"
        case "Chaoxing" => "0511"
        case "Duxiu" => "0510"
        case "Fudan" => "0507"
        case "National" => "0502"
        case "Nju" => "0504"
        case "Pku" => "0506"
        case "Ruc" => "0509"
        case "Tsinghua" => "0505"
        case "Zju" => "0503"
        case "Zjunew" => "0512"
        case "Rucnew" => "0509"
        case _ => ""
      }
      //将RDD转换为DF进行写入
      val data = hiveContext.createDataFrame(
        dataRDD.map(toMatchedDataFrame(_, category, matchCategory)))
      data.write.mode(SaveMode.Append).jdbc(sqlUrl, "t_Cadal_OtherResource_Stream", connectProperties)


  }
  def writeNotMatchedData(dataRDD: RDD[(String, String, String, String, String, String, String, String)],
    hiveContext: HiveContext) = {
    //设置好连接属性用于写数据
    val connectProperties = new Properties()
    connectProperties.put("user", "sa")
    connectProperties.put("password", "sql2008r2")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.200:1433;DatabaseName=FZJ;"
    val data = hiveContext.createDataFrame(dataRDD.map(toNotMatchedDataFrame))
    data.write.mode(SaveMode.Append).jdbc(sqlUrl, "t_Cadal_OtherResource_Stream", connectProperties)


  }
  def toMatchedDataFrame(value: (String,String,String, String, String, String, String, String, String, String, String, String,String,String),
                 category: String, matchCategory: String): matchedDataColumnName = {
    val isbn = if (value._7 == null) value._8 else value._7
    matchedDataColumnName(value._1, value._2, value._9, isbn,
      value._10, category, matchCategory)
  }
  def toNotMatchedDataFrame(value : (String, String, String, String, String, String, String, String)) = {

    val Array(id, title, creator, publisher, year, isbn,subject,classification)
    =Array(value._1,value._2,value._3,value._4,value._5,value._6,
      value._7,value._8)
    cadalDataColumnName(id: String, title: String,creator: String, publisher: String,
      year: String, isbn: String,subject: String,classification: String)
  }

  case class errorData(id: String,message: String,resource: Int)
  def writeErrorData(errorRdd:RDD[(String,String)],types: Int,hiveContext: HiveContext) ={

    val connectProperties = new Properties()
    connectProperties.put("user", "shm")
    connectProperties.put("password", "shm@092011")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val sqlUrl = "jdbc:sqlserver://192.168.1.165:1433;DatabaseName=WangZhihong;"


    val dataFrame = hiveContext.createDataFrame(errorRdd.map(value =>
      errorData(value._1,value._2,types)
    ))

    dataFrame.write.mode(SaveMode.Append).jdbc(sqlUrl, "t_Error3", connectProperties)

  }
  case class matchedDataColumnName(cadalid: String, otherid: String, subject: String, isbn: String,
                           classification: String, category: String, matchCategory: String)
  case class cadalDataColumnName(id: String, title: String, creator: String, publisher: String,
                                  year: String, isbn: String,subject: String,classification: String)
}
