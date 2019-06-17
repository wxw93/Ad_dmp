package cn.bupt.utils

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object TagUtils {

  var someUseIdConditition =
    """
      | imei !="" or mac != "" or idfa != "" or openudid != "" or androidid != "" or
      | imeimd5 !="" or macmd5 != "" or idfamd5 != "" or openudidmd5 != "" or androididmd5 != "" or
      | imeisha1 !="" or mac != "" or idfasha1 != "" or openudidsha1 != "" or androididsha1 != ""
    """.stripMargin

  def getAllUserId(row:Row):ListBuffer[String] ={
    var AllUserId = new ListBuffer[String]()

    if(row.getAs[String]("imei").nonEmpty)  AllUserId.append("IM"+row.getAs[String]("imei").toUpperCase)
    if(row.getAs[String]("idfa").nonEmpty)  AllUserId.append("DF"+row.getAs[String]("idfa").toUpperCase)
    if(row.getAs[String]("mac").nonEmpty)  AllUserId.append("MC"+row.getAs[String]("mac").toUpperCase)
    if(row.getAs[String]("androidid").nonEmpty)  AllUserId.append("AD"+row.getAs[String]("androidid").toUpperCase)
    if(row.getAs[String]("openudid").nonEmpty)  AllUserId.append("OU"+row.getAs[String]("openudid").toUpperCase)

    if(row.getAs[String]("imeimd5").nonEmpty)  AllUserId.append("IMM"+row.getAs[String]("imeimd5").toUpperCase)
    if(row.getAs[String]("idfamd5").nonEmpty)  AllUserId.append("DFM"+row.getAs[String]("idfamd5").toUpperCase)
    if(row.getAs[String]("macmd5").nonEmpty)  AllUserId.append("MCM"+row.getAs[String]("macmd5").toUpperCase)
    if(row.getAs[String]("androididmd5").nonEmpty)  AllUserId.append("ADM"+row.getAs[String]("androididmd5").toUpperCase)
    if(row.getAs[String]("openudidmd5").nonEmpty)  AllUserId.append("OUM"+row.getAs[String]("openudidmd5").toUpperCase)

    if(row.getAs[String]("imeisha1").nonEmpty)  AllUserId.append("IMS"+row.getAs[String]("imeisha1").toUpperCase)
    if(row.getAs[String]("idfasha1").nonEmpty)  AllUserId.append("DFS"+row.getAs[String]("idfasha1").toUpperCase)
    if(row.getAs[String]("macsha1").nonEmpty)  AllUserId.append("MCS"+row.getAs[String]("macsha1").toUpperCase)
    if(row.getAs[String]("androididsha1").nonEmpty)  AllUserId.append("ADS"+row.getAs[String]("androididsha1").toUpperCase)
    if(row.getAs[String]("openudidsha1").nonEmpty)  AllUserId.append("OUS"+row.getAs[String]("openudidsha1").toUpperCase)



    AllUserId
  }
}
