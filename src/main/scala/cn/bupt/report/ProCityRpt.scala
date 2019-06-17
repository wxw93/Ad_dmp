package cn.bupt.report

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计日志文件中省市的数据量分布情况
  *
  *  本次统计是基于parquet文件
  *
  * 需求1：
  *     将统计出来的结果存储成json文件格式
  *
  * 需求2：
  *     将统计出来的结果存储到mysql中
  */
object ProCityRpt {

    def main(args: Array[String]): Unit = {

        // 0 校验参数个数
        if (args.length != 2) {
            println(
                """
                  |cn.dmp.report.ProCityRpt
                  |参数：
                  | logInputPath
                  | resultOutputPath
                """.stripMargin)
            sys.exit()
        }

        // 1 接受程序参数
        val Array(logInputPath, resultOutputPath) = args

        // 2 创建sparkconf->sparkContext
        val sparkConf = new SparkConf()
        sparkConf.set("spark.testing.memory", "2147480000")//后面的值大于512m即可
        sparkConf.setAppName(s"${this.getClass.getSimpleName}")
        sparkConf.setMaster("local[*]")
        // RDD 序列化到磁盘 worker与worker之间的数据传输
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)

        // 读取数据 --》parquet文件
        val sqlc = new SQLContext(sc)
        val df: DataFrame = sqlc.read.parquet(logInputPath)

        // 将dataframe注册成一张临时表
        df.registerTempTable("log")


        // 按照省市进行分组聚合---》统计分组后的各省市的日志记录条数
        val result: DataFrame = sqlc.sql("select provincename, cityname, count(*) ct from log group by provincename, cityname order by provincename, cityname")


        // 判断结果存储路径是否存在，如果存在则删除
        val hadoopConfiguration = sc.hadoopConfiguration
        val fs = FileSystem.get(hadoopConfiguration)

        val resultPath = new Path(resultOutputPath)
        if(fs.exists(resultPath)) {
            fs.delete(resultPath, true)
        }


        // 将结果存储的成json文件   coalesce合并分区，参数表示分区数
        result.coalesce(1).write.json(resultOutputPath)
        sc.stop()
    }
}
