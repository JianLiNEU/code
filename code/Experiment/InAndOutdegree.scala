package Experiment

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

object InAndOutdegree {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kcore_RPDP").setMaster("local[5]")
    val sc = new SparkContext(conf)
    //图的读取
    val graph=GraphLoader.edgeListFile(sc, "D:\\文档\\数据\\图数据\\soc-pokec-relationships.txt")
    val edgeNumber=graph.vertices.values.collect().size.toDouble
    val indegreeNumber=graph.inDegrees.map(_._2).reduce(_+_).toDouble
    //val outdegreeNumber=graph.outDegrees.map(_._2).reduce(_+_).toDouble
    val degrreeNumber=graph.degrees.map(_._2).reduce(_+_).toDouble
    println("平均度"+degrreeNumber/edgeNumber)
    println("平均入度："+indegreeNumber/edgeNumber)
    //println("平均出度："+outdegreeNumber/edgeNumber)

  }


}
