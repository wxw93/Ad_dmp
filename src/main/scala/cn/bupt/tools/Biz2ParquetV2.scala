package cn.bupt.tools

import cn.bupt.beans.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 日志转成parquet文件格式
  *
  * 使用自定义类的方式构建schema信息
  */
object Biz2ParquetV2 {

    def main(args: Array[String]): Unit = {

        // 0 校验参数个数
        if (args.length != 3) {
            println(
                """
                  |cn.dmp.tools.Bzip2Parquet
                  |参数：
                  | logInputPath
                  | compressionCode <snappy, gzip, lzo>
                  | resultOutputPath
                """.stripMargin)
            sys.exit()
        }

        // 1 接受程序参数
        val logInputPath = args(0)
        val resultOutputPath = args(1)
        val compressionCode = args(2)

        // 2 创建sparkconf->sparkContext
        val sparkConf = new SparkConf()
        sparkConf.set("spark.testing.memory", "2147480000")//后面的值大于512m即可
        sparkConf.setAppName(s"${this.getClass.getSimpleName}")
        //设置本地运行
        sparkConf.setMaster("local[*]")
        // RDD 序列化到磁盘 worker与worker之间的数据传输
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


        // 注册自定义类的序列化方式
        sparkConf.registerKryoClasses(Array(classOf[Log]))


        val sc = new SparkContext(sparkConf)

        val sQLContext = new SQLContext(sc)
        //设置压缩格式
        sQLContext.setConf("spark.sql.parquet.compression.codec", compressionCode)

        // 读取日志文件
        val dataLog: RDD[Log] = sc.textFile(logInputPath)
          .map(line => line.split(",", -1))
          .filter(_.length >= 85).map(arr => Log(arr))

        val dataFrame = sQLContext.createDataFrame(dataLog)

        // 按照省份名称及地市名称对数据进行分区
        dataFrame.write.partitionBy("provincename", "cityname").parquet(resultOutputPath)

        sc.stop()

    }


}
