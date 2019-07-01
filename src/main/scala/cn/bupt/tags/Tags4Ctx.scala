package cn.bupt.tags

import cn.bupt.utils.{ConnectRedis, TagUtils}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Tags4Ctx {
  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if (args.length != 4) {
      println(
        """
          |cn.bupt.tags.Tags4Ctx
          |参数：
          | 日志输入路径：logInputPath
          | 字典文件路径：dictFilePath
          | 停用词路径：stopKeyWordsPath
          | 日志输出路径：resultOutputPath
        """.stripMargin)
      sys.exit()
    }
    // 1 接受程序参数
    val Array(logInputPath,dictFilePath,stopKeyWordsPath,resultOutputPath) = args

    // 2 创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.set("spark.testing.memory", "2147480000")
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    //读取字典文件，并广播出去
    val DictMap = sc.textFile(dictFilePath).map(line => {
      var fields = line.split("\t", -1)
      (fields(4), fields(1))
    }).collect().toMap

    val stopKeyWord = sc.textFile(stopKeyWordsPath).map((_,0)).collect().toMap

    //广播
    val dictMapBroat = sc.broadcast(DictMap)
    val keyWordBroat = sc.broadcast(stopKeyWord)

    //读取 parquet 日志文件
    val sqlc = new SQLContext(sc)
    val dataFrame = sqlc.read.parquet(logInputPath)
     //测试在testUseIdConditition添加下没有数据
    dataFrame.where(TagUtils.testUseIdConditition).select("lat","long").show()

    //过滤数据
    dataFrame.where(TagUtils.someUseIdConditition).mapPartitions(pars => {
      val listBuffer = new collection.mutable.ListBuffer[(String,List[(String,Int)])]()
      val jedis = ConnectRedis.getJedis()
      pars.foreach(row=>{
        val adMap = Tags4Ad.MakeTag(row)
        val appMap = Tags4App.MakeTag(row,dictMapBroat.value)
        val deviceMap = Tags4Device.MakeTag(row)
        val keywordMap = Tags4KeyWord.MakeTag(row,keyWordBroat.value)
        val channelMap = Tags4Channel.MakeTag(row)

        //商业标签
        val bussMap = Tags4Buss.MakeTag(row,jedis)

        val UserIds = TagUtils.getAllUserId(row)
        //其他userId
        val otherUserId = UserIds.slice(1,UserIds.length).map(usId=>(usId,0)).toMap

        //设置返回
        listBuffer.append((UserIds(0),(adMap ++ appMap ++ deviceMap ++ keywordMap ++ channelMap ++ bussMap ++ otherUserId).toList))
        listBuffer
      })
      jedis.close()
      listBuffer.iterator
    }).reduceByKey((a,b) =>{
      (a ++ b).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
    }).saveAsTextFile(resultOutputPath)

    sc.stop()
  }
}
