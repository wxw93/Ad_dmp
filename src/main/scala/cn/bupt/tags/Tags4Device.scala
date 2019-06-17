package cn.bupt.tags

import org.apache.spark.sql.Row

object Tags4Device extends TagsData {
  override def MakeTag(arg: Any*): Map[String, Int] = {
    var deviceMap = Map[String,Int]()

    //类型转换
    val row = arg(0).asInstanceOf[Row]

    //获取操作系统  联网  运营商字段
    var os = row.getAs[Int]("client")
    var phoneType = row.getAs[String]("device")
    var netType = row.getAs[String]("networkmannername")
    var ispname = row.getAs[String]("ispname")

    //操作系统
    os match {
      case 1 => deviceMap += "Android D00010001" -> 1
      case 2 => deviceMap += "IOS D00010002" -> 1
      case 3 => deviceMap += "WinPhone D00010003" -> 1
      case _ => deviceMap += " 其他 D00010004" -> 1
    }

    //联网方式
    netType.toUpperCase match {
      case "WIFI" => deviceMap += "WIFI D00020001" -> 1
      case "4G" => deviceMap += "4G D00020001" -> 1
      case "3G" => deviceMap += "3G D00020001" -> 1
      case "2G" => deviceMap += "2G D00020001" -> 1
      case _ => deviceMap += "其他 D00020001" -> 1
    }

    //运营商方式
    ispname match {
      case "移动" => deviceMap += " D00030001" -> 1
      case "联通" => deviceMap += " D00030002" -> 1
      case "电信" => deviceMap += " D00030003" -> 1
      case _ => deviceMap += " D00030004" -> 1
    }

    //地域标签
    var pName = row.getAs[String]("provincename")
    var cName = row.getAs[String]("cityname")

    if(!pName.isEmpty) deviceMap += "ZP" + pName -> 1

    if(!cName.isEmpty) deviceMap += "ZC" + cName -> 1

    deviceMap
  }
}
