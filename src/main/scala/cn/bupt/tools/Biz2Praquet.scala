package cn.bupt.tools

import cn.bupt.utils.{NBF, SchemaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将原始日志文件转换成parquet文件格式
  * 采用snappy压缩格式
  */
object Biz2Praquet {
  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if (args.length!=3){
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
    val OutPutPath = args(1)
    val compressionCode = args(2)


    // 2 创建sparkconf->sparkContext
    val sparkconf = new SparkConf()
    sparkconf.set("spark.testing.memory", "2147480000")//后面的值大于512m即可
    sparkconf.setAppName(s"${this.getClass.getSimpleName}").setMaster("local[*]")  //设置运行进程名
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkconf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkconf)

    val sQLContext = new SQLContext(sc)
    //设置压缩格式
    sQLContext.setConf("spark.sql.parquet.compression.codec", compressionCode)
    // 3 读取日志数据
    val rowLog = sc.textFile(InputPath)

    // 4 根据业务需求对数据进行ETL  xxxx,x,x,x,x,,,,,
    val rowData:RDD[Row] = rowLog
      .map(line => line.split(",",line.length))
      .filter(_.length >=85)
      .map(arr =>{
        NBF.dataRow(arr)
      })

    // 5 将结果存储到本地磁盘
    val dataFrame = sQLContext.createDataFrame(rowData,SchemaUtils.logStructType)
    dataFrame.write.parquet(OutPutPath)

    // 6 关闭sc
    sc.stop()
  }
}
