package KCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.storage.StorageLevel

object Kcore01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kcore").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // load the graph
    val friendsGraph = GraphLoader.edgeListFile(sc, "D:\\文档\\数据\\社区发现\\karateDemo01.txt", false, 512, StorageLevel.MEMORY_ONLY, StorageLevel.DISK_ONLY)

    var degreeGraph = friendsGraph.outerJoinVertices(friendsGraph.degrees) {
      (vid, vd, degree) => degree.getOrElse(0)
    }.cache()

    val kNum = 200
    var lastVerticeNum: Long = degreeGraph.numVertices
    var thisVerticeNum: Long = -1
    var isConverged = false
    val maxIter = 10
    var i = 1
    while (!isConverged && i <= maxIter) {
      val subGraph = degreeGraph.subgraph(
        vpred = (vid, degree) => degree >= kNum
      ).cache()

      degreeGraph = subGraph.outerJoinVertices(subGraph.degrees) {
        (vid, vd, degree) => degree.getOrElse(0)
      }.cache()

      thisVerticeNum = degreeGraph.numVertices
      if (lastVerticeNum == thisVerticeNum) {
        isConverged = true
        println("vertice num is " + thisVerticeNum + ", iteration is " + i)
      } else {
        println("lastVerticeNum is " + lastVerticeNum + ", thisVerticeNum is " + thisVerticeNum + ", iteration is " + i + ", not converge")
        lastVerticeNum = thisVerticeNum
      }
      i += 1
    }

  }


}



