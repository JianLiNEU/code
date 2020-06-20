package KCore

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

import scala.collection.immutable.IntMap

object KcoreToGref {
  val initialMsg="-10"
  def mergeMsg(msg1: String, msg2:String): String = msg1+":"+msg2

  def vprog(vertexId: VertexId, value: (Int, Int,IntMap[Int],Int), message: String): (Int, Int,IntMap[Int],Int) = {
    if (message == initialMsg){
      return (value._1,value._2,value._3,value._4)
    }
    else{
      val msg=message.split(":")
      val elems=msg //newWeights.values
      var counts:Array[Int]=new Array[Int](value._1+1)
      for (m <-elems){
        val im=m.toInt
        if(im<=value._1)
        {
          counts(im)=counts(im)+1
        }
        else{
          counts(value._1)=counts(value._1)+1
        }
      }
      var curWeight =  0 //value._4-newWeights.size
      for(i<-value._1 to 1 by -1){
        curWeight=curWeight+counts(i)
        if(i<=curWeight){
          return (i, value._1,value._3,value._4)
        }
      }
      return (0, value._1,value._3,value._4)
    }
  }

  def sendMsg(triplet: EdgeTriplet[(Int, Int,IntMap[Int],Int), Int]): Iterator[(VertexId, String)] = {
    val sourceVertex = triplet.srcAttr
    val destVertex=triplet.dstAttr
    return Iterator((triplet.dstId,sourceVertex._1.toString),(triplet.srcId,destVertex._1.toString))

  }

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()
    //setting up spark environment
    val conf: SparkConf = new SparkConf()
      .setAppName("KCore").setMaster("local[10]")
    val sc = new SparkContext(conf)
    //val operation="Kclique"
    val maxIter=10
    val ygraph=GraphLoader.edgeListFile(sc, "D:\\文档\\研究生课题20170424\\论文\\第一篇\\实验结果\\34_2_0.5.txt")

    val deg=ygraph.degrees

    val mgraph=ygraph.outerJoinVertices(deg)((id, oldattr, newattr) =>newattr.getOrElse(0)).mapVertices((id, attr) =>(attr,-1,IntMap[Int](),attr))
    ygraph.unpersist()
    val minGraph = mgraph.pregel(initialMsg,maxIter,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)

    val kmax=minGraph.vertices.map(_._2._1).max
    val pruned=minGraph.subgraph(vpred=(id, attr) => attr._1==kmax)
    minGraph.vertices.foreach(u=>println(u._1+" "+u._2))
    println(kmax)
    println(minGraph.vertices.filter(v=>{v._2._1==kmax}).count())
    println(pruned.vertices.count())
    println(pruned.edges.count())
    //  pruned.edges.map(e=>{e.srcId+" "+e.dstId}).repartition(1).saveAsTextFile("D:\\文档\\数据\\社区发现\\ResultKcore.txt")
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("Total Execution Time : "+durationSeconds.toString() + "s")

    val KcoreGraph=minGraph.mapVertices((id,attr)=>{
      if(attr._1<=5) "#eaff56"//樱草色
      else if(attr._1<=5) "#fff143"//鹅黄
      else if(attr._1<=20) "#fa8c35"//橙色
      else if(attr._1<=40) "#bce672"//松花色
      else if(attr._1<=60) "#bddd22"//嫩绿
      else if(attr._1<=80) "#00bc12"//油绿
      else  "#ff461f"//火红

    })



    def toGexf[VD,ED](g:Graph[VD,ED])=
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+
        "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n"+
        "  <graph mode=\"static\" defaultedgetype=\"directed\">\n"+
        " <nodes>\n"+
        g.vertices.map(v=>" <node id=\""+v._1+"\" Label=\""+v._2+"\"/>\n").collect.mkString+
        " </nodes>\n"+
        "<edges>\n" +
        g.edges.map(e=>"    <edge source=\""+e.srcId+
          "\" target=\""+e.dstId+"\" label=\""+e.attr+
          "\"/>\n").collect.mkString+
        "  </edges>\n"+
        " </graph>\n"+
        " </gexf>"

    val pw=new PrintWriter("D:\\文档\\研究生课题20170424\\论文\\第一篇\\实验结果\\34_2_0.5.gexf")
    pw.write(toGexf(KcoreGraph))
    pw.close()

    sc.stop()
  }


}
