package cn.bupt.tags

import org.apache.spark.sql.Row

object Tags4Channel extends TagsData {
  override def MakeTag(arg: Any*): Map[String, Int] = {
    var channelMap = Map[String,Int]()

    //类型转换
    val row = arg(0).asInstanceOf[Row]

    var channelId = row.getAs[String]("channelid")

    if(!channelId.isEmpty){
      channelMap += "CH" + channelId -> 1
    }

    channelMap
  }
}
