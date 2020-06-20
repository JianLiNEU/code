package DGPA

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

import scala.collection.mutable.ArrayBuffer
//DNR随机邻居节点扰动
object DNR {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RPDP")
    val sc = new SparkContext(conf)

    //图的读取
   // val originGraph=GraphLoader.edgeListFile(sc, "D:\\文档\\数据\\社区发现\\karateDemo01.txt")
    val originGraph=GraphLoader.edgeListFile(sc, args(0))
    val r=2
    val p=49
    //原始图需要查找可达节点
    val initGraph=originGraph.mapVertices((id,attr)=>{
      val state=new VertexState()
      state.RNTE+=(id.toInt->Array(id.toInt,id.toInt,0,0))
      state
    })
    //DNR查找可达节点
 /*   val searchResult=DRNS.SearchResult(initGraph.reverse,r)
    //DNR节点匿名
    val anonymityDNR=Anonymity.anonyDNR(searchResult,p)
    anonymityDNR.values.coalesce(1,true)saveAsTextFile(args(1))*/

    //RPDP算法需要探测，寻找邻居节点。

    val searchResult=DRNS.NerSearchResult(initGraph.reverse,r)
    val anonymityDRNS=Anonymity.anonyRPDP(searchResult,p)
   /* val result=anonymityDRNS.vertices.mapValues(_.RNTE).foreach(attr=>{
      attr._2.keys.foreach(key=>{
        attr._2(key).foreach(print(_))
        println()
      })
    })*/


    val result=anonymityDRNS.vertices.mapValues(_.RNTE.values.filter(_(3)>p)).map(attr=>{
      var cand=""
      attr._2.foreach(a=>{cand+=a(0)+" "+a(1)+"   "})
      cand
    })
    result.coalesce(1,true).saveAsTextFile(args(1))




    // anonymityDRNS.triplets.map(_.attr).foreach(println(_))
    // anonymityDRNS.triplets.map(_.attr).coalesce(1,true).saveAsTextFile(args(1))



  }
}
