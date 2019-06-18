package cn.bupt.tools

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object GetLatandLong {
  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if (args.length != 3) {
      println(
        """
          |请正确传入参数
          |InputPath：输入数据路径
          |compressionCode <snappy, gzip, lzo>：数据压缩格式
          |OutPutPath：输出数据路径
        """.stripMargin)
      sys.exit()
    }
    // 1 接受程序参数
    val InputPath = args(0)
    // 2 创建sparkconf->sparkContext
    val sparkconf = new SparkConf()
    sparkconf.set("spark.testing.memory", "2147480000")//后面的值大于512m即可
    sparkconf.setAppName(s"${this.getClass.getSimpleName}").setMaster("local[*]")  //设置运行进程名
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkconf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkconf)
    val sQLContext = new SQLContext(sc)

//    sQLContext.read.parquet(InputPath)
//      //选择经纬度列数据
//      .select("lat","long")
//      //筛选中国境内的经纬度，排除空值
//      .where("lat>3 and lat<54 and long>73 and long <136")
//      //去重
//      .distinct()
//      .map()

  }
}
