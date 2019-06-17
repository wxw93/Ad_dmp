package cn.bupt.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object Tags4Ad extends TagsData {
  override def MakeTag(arg: Any*): Map[String, Int] = {
    var adMap = Map[String,Int]()

    //类型转换
    val row = arg(0).asInstanceOf[Row]

    //获取广告类型和名称
    val adType= row.getAs[Int]("adspacetype")
    var adName= row.getAs[String]("adspacetypename")

    //给广告类型和名称 打标签
    if(adType>9)  adMap += "LC" + adType -> 1
    else if(adType>0) adMap += "LC0" + adType -> 1

    if(StringUtils.isNotEmpty(adName))adMap += "LN" + adName ->1

    adMap

  }
}
