package cn.bupt.tags

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object Tags4Buss extends TagsData {
  override def MakeTag(arg: Any*): Map[String, Int] = {
    var bussMap = Map[String,Int]()

    //类型转换
    val row = arg(0).asInstanceOf[Row]
    val jedis = arg(1).asInstanceOf[Jedis]

    val lat = row.getAs[String]("lat").toDouble
    val longs = row.getAs[String]("long").toDouble


    if((lat>3 && lat<54) && (longs>73 && longs <136)){
      val base32code = GeoHash.withCharacterPrecision(lat.toDouble,longs.toDouble,8).toBase32
      System.out.println(base32code)
      val buss = jedis.get(base32code)
      System.out.println(buss)
      if(StringUtils.isNotEmpty(buss)){
        buss.foreach(bs=>bussMap += "BS" + bs -> 1)
      }
    }
    bussMap
  }
}
