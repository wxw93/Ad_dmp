package cn.bupt.report

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 地域报表，将数据写入到mysql
  * 采用技术：spark  SQL
  */
object LogAreaAnaly {
  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if (args.length != 1) {
      println(
        """
          |cn.bupt.report.LogAreaAnaly
          |参数：
          | logInputPath
        """.stripMargin)
      sys.exit()
    }

    // 1 接受程序参数
    val Array(logInputPath) = args

    // 2 创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.set("spark.testing.memory", "2147480000")
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    //业务逻辑
    val sQLContext = new SQLContext(sc)
    //读取数据
    val frameData = sQLContext.read.parquet(logInputPath)
    //将数据注册一张表
    frameData.registerTempTable("AreaTable")
    //使用SQL语句查询
    val result = sQLContext
      .sql("""
             |select
             |provincename, cityname,
             |sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) 有效请求,
             |sum(case when requestmode=1 and processnode =3 then 1 else 0 end) 广告请求,
             |sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 then 1 else 0 end) 参与竞价数,
             |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) 竞价成功数,
             |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) 展示数,
             |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) 点击数,
             |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*adpayment/1000 else 0 end) 广告成本,
             |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*winprice/1000 else 0 end) 广告消费
             |from AreaTable
             |group by provincename, cityname
             |order by provincename, cityname
           """.stripMargin)


    // 加载配置文件  application.conf -> application.json --> application.properties
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    // 将结果写入到mysql中
    result.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"), load.getString("jdbc.area.table"), props)
    sc.stop()
  }
}
