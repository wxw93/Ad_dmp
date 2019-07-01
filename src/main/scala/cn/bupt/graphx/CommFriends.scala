package cn.bupt.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CommFriends {
  def main(args: Array[String]): Unit = {
    // 2 创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.set("spark.testing.memory", "2147480000")
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    //创造数据   点
    val uv:RDD[(VertexId,(String,Int))] = sc.parallelize(Seq(
        (1, ("张三", 20)),
        (2, ("李四", 25)),
        (6, ("王五", 30)),
        (9, ("王二", 22)),
        (133, ("麻子", 32)),

        (16, ("张四", 21)),
        (21, ("李武", 23)),
        (44, ("王三", 26)),
        (138, ("麻小", 18)),

        (5, ("张武", 19)),
        (7, ("王六", 20)),
        (158, ("马晓", 28))
    ))
    //边
    val ue:RDD[Edge[Int]] = sc.parallelize(Seq(
      Edge(1, 133, 0),
      Edge(2, 133, 0),
      Edge(9, 133, 0),
      Edge(6, 133, 0),

      Edge(6, 138, 0),
      Edge(16, 138, 0),
      Edge(14, 138, 0),
      Edge(21, 138, 0),

      Edge(5, 158, 0),
      Edge(7, 158, 0)
    ))

    val graph = Graph(uv,ue)
    //连通图
    val commonV = graph.connectedComponents().vertices
//    commonV.map(t=> (t._1,List(t._2))).reduceByKey(_ ++ _)
    uv.join(commonV).map{
      case(userId,((name,age),cmId)) => (cmId,List((userId,name,age)))
    }.reduceByKey(_ ++ _).foreach(println)

    sc.stop()
  }
}
