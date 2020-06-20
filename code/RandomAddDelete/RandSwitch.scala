package RandomAddDelete

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Map
import scala.util.Random

object RandSwitch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("R-SW").setMaster("local[5]")
    val sc = new SparkContext(conf)
    //val originGraph = sc.textFile("D:\\文档\\数据\\社区发现\\karateDemo01.txt")
    val originGraph = sc.textFile("D:\\文档\\研究生课题20170424\\论文\\第三篇\\IEEE ACCESS\\实验\\萤火虫实验数据集\\polblog.txt")
    //val originGraph=sc.textFile(args(0))
    val p = 30
    val splitVertice = originGraph.map(a => {
      val barray = a.split(" ")
      (barray(0), barray(1))
    })
    val candidateArray = splitVertice.collect().toBuffer
    val random = new Random()



 /*   val result = splitVertice.map(edge => {

      if (candidateArray.contains(edge)) {
        //判断候选边是否扰动
        if (random.nextInt(100) > p) {
          candidateArray.remove(candidateArray.indexOf(edge))
          Array((edge._1, edge._2, 0.toString))
        }
        else {
          val candidateNode = candidateArray.filter(a => {
            a._1 != edge._1 & a._1 != a._2 & a._2 != edge._1 & a._2 != edge._2
          })
          if (candidateNode.size > 0) {
            val candidateEdge = candidateNode(random.nextInt(candidateNode.size))
            candidateArray.remove(candidateArray.indexOf(candidateEdge))
            candidateArray.remove(candidateArray.indexOf(edge))
            Array((edge._1, edge._2, 0.toString), (candidateEdge._1, candidateEdge._2, 0.toString))
          }
          else {
            Array((edge._1, edge._2, 0.toString))
          }
        }
      } else {
        Array((0.toString, 0.toString, 0.toString))
      }
    })
    result.groupBy(_(0)).foreach(a=>{

      println(a._1)
      a._2.flatten.foreach(print(_))
    })



*/

   val result=splitVertice.collect().map(edge=>{
     if (candidateArray.contains(edge)) {
       //判断候选边是否扰动
       if (random.nextInt(100) > p) {
         candidateArray.remove(candidateArray.indexOf(edge))
          edge._1+" "+edge._2
       }
       else {
         val candidateNode = candidateArray.filter(a => {
           a._1 != edge._1 & a._1 != a._2 & a._2 != edge._1 & a._2 != edge._2
         })
         if (candidateNode.size > 0) {
           val candidateEdge = candidateNode(random.nextInt(candidateNode.size))
           candidateArray.remove(candidateArray.indexOf(candidateEdge))
           candidateArray.remove(candidateArray.indexOf(edge))
           edge._1+" "+candidateEdge._1+"  "+edge._2+" "+candidateEdge._2
         }
         else {
          edge._1+" "+edge._2
         }
       }
     } else {
       ""
     }
    })
    sc.makeRDD(result).coalesce(1,true).saveAsTextFile("D:\\文档\\研究生课题20170424\\论文\\第三篇\\IEEE ACCESS\\实验\\ADSW\\polblog\\Switch\\polblog301")







  }
}