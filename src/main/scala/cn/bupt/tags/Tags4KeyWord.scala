package cn.bupt.tags

import org.apache.spark.sql.Row

object Tags4KeyWord extends TagsData {
  override def MakeTag(arg: Any*): Map[String, Int] = {
    var keyWordMap = Map[String,Int]()

    //类型转换
    val row = arg(0).asInstanceOf[Row]
    val stopWords = arg(1).asInstanceOf[Map[String,Int]]

    // 关键词打标签
    val kws = row.getAs[String]("keywords")

    val keywords = kws.split("\\|").filter(kw=>kw.trim.length>2 && kw.trim.length<8 && !stopWords.contains(kws.trim))
    keywords.foreach(ky=>keyWordMap += "K"+ky -> 1)

    keyWordMap
  }
}
