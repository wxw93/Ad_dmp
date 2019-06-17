package cn.bupt.report

import java.util.Properties

import cn.bupt.beans.Log
import cn.bupt.utils.{NBF, RptUtils, SchemaUtils}
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

/*
* 根据终端设备统计，将统计结果存储到MySQL
* */
object Terminal {
  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if (args.length != 2) {
      println(
        """
          |cn.bupt.report.Terminal
          |参数：
          | 日志输入路径：logInputPath
          | 字典文件路径：dictFilePath
        """.stripMargin)
      sys.exit()
    }

    // 1 接受程序参数
    val Array(logInputPath, dictFilePath) = args

    // 2 创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.set("spark.testing.memory", "2147480000")
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    //读取字典文件，并广播
    val dictMap = sc.textFile(dictFilePath).map(line => {
      val fields = line.split("\t", -1)
      (fields(4), fields(1))
    }) //collect做缓存，元组转map
      .collect().toMap
    val broadcast = sc.broadcast(dictMap)

    // 读取数据 --> parquet文件
    val sqlc = new SQLContext(sc)
    var dealData:RDD[Row]=
    sc.textFile(logInputPath)
      .map(_.split(",", -1))
      .filter(_.length >= 85)
      .map(Log(_))
      .filter(log => !log.appid.isEmpty || !log.appname.isEmpty)
      .map(log => {
        var newAppname = log.appname
        if (!StringUtils.isNotEmpty(newAppname)) {
          newAppname = broadcast.value.getOrElse(log.appid, "未知")
        }
        val req = RptUtils.caculateReq(log.requestmode, log.processnode)
        val rtb = RptUtils.caculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
        val showClick = RptUtils.caculateShowClick(log.requestmode, log.iseffective)

        (newAppname, req ++ rtb ++ showClick)
      }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => {
      t._1 + "," + t._2.mkString(",")
    })
      //.saveAsTextFile(resultOutputPath)
    .map(_.split(",",-1))
    .filter(_.length==10)
    .map(arr =>{
      NBF.LogAppRow(arr)
    })

    val resultDF = sqlc.createDataFrame(dealData,SchemaUtils.logAppType)
//    resultDF.registerTempTable("tmp")
//    val result = sqlc.sql(
//      """
//        |select * from tmp
//      """.stripMargin)
    // 加载配置文件  application.conf -> application.json --> application.properties
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    // 将结果写入到mysql的 rpt_pc_count 表中   mode写入数据库模式，Append：追加 Overwrite：覆盖
    resultDF.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), load.getString("jdbc.app.table"), props)

    sc.stop()
  }

}
