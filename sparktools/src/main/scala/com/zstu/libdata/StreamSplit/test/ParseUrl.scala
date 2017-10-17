package com.zstu.libdata.StreamSplit.test

import scala.util.matching.Regex

/**
  * Created by Administrator on 2017/9/29 0029.
  */
object ParseUrl {
  def GetKey(url: String,resource: String) : String ={
    resource match {
      case "cnki" => GetCnkiKey(url)
      case "wf" => GetWfKey(url)
      case "vip" => GetVipKey(url)
      case _ => null
    }
  }
  def ParseResource(url : String) ={
    if(url.indexOf("kns.cnki.net") >= 0) "cnki"
    else if(url.indexOf("wanfangdata") >= 0) "wf"
    else "vip"
  }
  private def GetWfKey(wfUrl: String): String = FindWithReg(wfUrl,"""(?<=Periodical_)([a-zA-Z-]*)(?=[0-9]*)""".r)
  private def GetCnkiKey(cnkiUrl: String): String = FindWithReg(cnkiUrl, """(?<=filename=)([a-zA-Z]*)(?=[0-9]*)""".r)
  private def GetVipKey(vipUrl: String): String = FindWithReg(vipUrl,"""(?<=QK/)(.*?)(?=/)""".r)
  private def FindWithReg(url : String,reg:Regex )={
    try{
      reg findFirstIn url get
    } catch{
      case _ => null
    }
  }

  def main(args: Array[String]): Unit = {
    println(GetKey("http://www.cqvip.com/QK/84031X/200105/15206660.html","vip"))
    println(GetKey("http://kns.cnki.net/KCMS/detail/detail.aspx?dbCode=CJFD&filename=SHIJ201711036&tableName=CJFDPREP&url=","cnki"))
    println(GetKey("http://d.g.wanfangdata.com.cn/Periodical_shjtdxxb-e200802007.aspx","wf"))
  }

}
