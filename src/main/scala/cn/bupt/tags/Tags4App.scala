package cn.bupt.tags

import org.apache.spark.sql.Row

object Tags4App extends TagsData {
  override def MakeTag(arg: Any*): Map[String, Int] = {
    var appMap = Map[String,Int]()

    //类型转换
    val row = arg(0).asInstanceOf[Row]
    val appDict = arg(1).asInstanceOf[Map[String,String]]

    //APP名称 打标签
    var appId = row.getAs[String]("appid")
    var appName = row.getAs[String]("appname")
    if(appName.isEmpty){
      appName = row.getAs[String](appId)
    }
    appMap += "APP" + appName -> 1
    appMap
  }
}
