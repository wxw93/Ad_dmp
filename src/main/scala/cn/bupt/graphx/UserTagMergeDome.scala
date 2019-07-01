package cn.bupt.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UserTagMergeDome {
  def main(args: Array[String]): Unit = {
    // 2 创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.set("spark.testing.memory", "2147480000")
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)

    val data:RDD[Array[String]] = sc.textFile(args(0)).map(_.split("\t"))

    //创建点集合
    val uv:RDD[(VertexId,(String,List[(String ,Int)]))] = data.flatMap(arr => {
      //区分名和标签
      val userNames = arr.filter(_.indexOf(":") == -1)
      val tagsAndNum = arr.filter(_.indexOf(":") != -1).map(tags => {
        val tagsNum = tags.split(":")
        (tagsNum(0), tagsNum(1).toInt)
      }).toList

      val nameAndags = userNames.map(name => {
        if (name.equals(userNames(0))) (name.hashCode.toLong, (name, tagsAndNum))
        else (name.hashCode.toLong, (name, List.empty[(String, Int)]))
      })
      nameAndags
    })
    //创建边集合
    val ue = data.flatMap(arr => {
      val userNames = arr.filter(_.indexOf(":") == -1)
      //结果(id,共同最小的顶点ID)
      userNames.map(name => Edge(userNames(0).hashCode.toLong, name.hashCode.toLong, 0))
    })

    //创建图
    val graph = Graph(uv,ue)
    val vertices = graph.connectedComponents().vertices

    //聚合数据，获取结果
    vertices.join(uv).map{
      case(id,(cmId,(name,tags))) => (cmId,(name,tags))
    }.reduceByKey{
      case(t1,t2)=> {
        val k = t1._1 ++ "-" ++ t2._1
        val v = (t1._2 ++ t2._2).groupBy (_._1).mapValues (_.foldLeft(0)(_ + _._2)).toList
        (k,v)
      }
    }.map(t=>(t._2._1,t._2._2)).foreach(println)
    sc.stop()
  }
}
