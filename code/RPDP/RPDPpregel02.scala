package RPDP

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object RPDPpregel02 {


  def vprog(VD:VertexId, Value:Map[Int, Array[Int]], Message:Map[Int, Array[Int]]):Map[Int, Array[Int]] ={
    var newvalue=Value
    if(Message.exists(_._1==0)||Message.size==0){
      return(Value)
    }
    else{
      Message.foreach(v=>{
        if(!Value.contains(v._1)){
         newvalue+=(v._2(1)->Array(VD.toInt,v._2(1),v._2(2)+1,v._2(3)))
        }
      }
      )
      newvalue
    }


  }

  def sendMsg( ET:EdgeTriplet[Map[Int, Array[Int]], Int]) :Iterator[(VertexId, Map[Int, Array[Int]])] ={
    Iterator((ET.dstId,ET.srcAttr))

  }

  def mergeMsg (A1: Map[Int, Array[Int]], A2: Map[Int, Array[Int]]) : Map[Int, Array[Int]] = {
  A1.++(A2)
  }


  def main(args: Array[String]): Unit = {
    //spark基本设置
    val conf = new SparkConf().setAppName("RPDP")
    val sc = new SparkContext(conf)
    //图的读取
    //  val graph=GraphLoader.edgeListFile(sc, "D:\\文档\\数据\\社区发现\\karateDemo01.txt")
      //GraphLoader.edgeListFile(sc,"/user/input/email")
    val graph= GraphLoader.edgeListFile(sc,args(0),true,10,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)

     val random=new Random()
     val initGraph=graph.reverse.mapVertices((id,attr)=> {
       if (random.nextInt(9) > 1)
         (Map(id.toInt -> Array(id.toInt, id.toInt, 0, 1)))
       else
         (Map(id.toInt -> Array(id.toInt, id.toInt, 0, 0)))
     })
    initGraph.unpersist()




    // val initalGraph= graph.reverse.outerJoinVertices(graph.reverse.outDegrees){(id,attr,outg)=>outg}
    val rpdpGraph=initGraph.pregel(Map(0->Array(0,0,0,0)),3,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)
    //rpdpGraph.edges.saveAsTextFile("D:\\文档\\数据\\社区发现\\RPDP")
    //rpdpGraph.vertices.saveAsTextFile("user/output/RPDP01")
    //遍历结果
    //rpdpGraph.vertices.foreach(u=>u._2._2.foreach(u1=>println("节点id"+u._1+"节点属性"+u1.toString())))
 /* rpdpGraph.vertices.foreach(
    v=>{
      println(v+" "+v._2)
      for(i<-v._2)
        i._2.foreach(println(_))
    }

  )*/



    



}
 

}

