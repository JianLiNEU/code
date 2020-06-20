package RPDP

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

//应该使用Array（array）（123456,123245,0,2）这种情况使用String的话，就会一直在123这种地方
object RPDPPregel01{
  def vprog(VI:VertexId, value:(PartitionID, ArrayBuffer[Array[PartitionID]]), message:ArrayBuffer[Array[PartitionID]]) :(PartitionID, ArrayBuffer[Array[PartitionID]]) = {
    var arrayBufferVP= value._2//初始化Array数组
    //第一回迭代，message信息设置标志Array("0000"),初始化节点信息为
    if(message.exists(i=>i(1).equals(0))||message.size==0)
      return(value._1,value._2)

    else{
      /*message.foreach(A=>{
        if(!value._2.exists((f=>A(1)==f(1)))){
          value._2+=A
        }
      })

      (value._1,value._2)*/
      (value._1,message++value._2)

    }


  }
  def sendMsg( et:EdgeTriplet[(PartitionID, ArrayBuffer[Array[PartitionID]]), PartitionID]) :Iterator[(VertexId, ArrayBuffer[Array[PartitionID]])] ={
   var arrayBuffer=new ArrayBuffer[Array[PartitionID]]()
    et.srcAttr._2.foreach(u=>{
      if(!et.dstAttr._2.exists(f=>f(1)==u(1))){
        arrayBuffer+=Array(et.dstId.toInt,u(1),u(2)+1,u(3))

      }
    })
    Iterator((et.dstId,arrayBuffer.distinct))


  }

  def mergeMsg(a1:ArrayBuffer[Array[PartitionID]], a2:ArrayBuffer[Array[PartitionID]]) : ArrayBuffer[Array[PartitionID]] = {
    (a1++a2).distinct

  }

  def main(args: Array[String]): Unit = {
    //spark基本设置
    val conf = new SparkConf().setAppName("RPDP")
    val sc = new SparkContext(conf)
    //图的读取
    val graph = {
     // GraphLoader.edgeListFile(sc, "D:\\文档\\数据\\社区发现\\karateDemo01.txt")
      //GraphLoader.edgeListFile(sc,"/user/input/email")
      GraphLoader.edgeListFile(sc,"/user/input/twitter",true,12,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)
    }
    val random=new Random()
  /*  val initalGraph=graph.reverse.mapVertices((id,attr)=>{
      if(random.nextInt(9)>1){(1,ArrayBuffer(Array(id.toInt,id.toInt,0,0)))}
    else{(0,ArrayBuffer(Array(id.toInt,id.toInt,0,0)))}
  }*/
      val initalGraph=graph.reverse.mapVertices((id,attr)=>{
      (random.nextInt(100),ArrayBuffer(Array(id.toInt,id.toInt,0,0)))

  }
  )

   // val initalGraph= graph.reverse.outerJoinVertices(graph.reverse.outDegrees){(id,attr,outg)=>outg}

    val rpdpGraph=initalGraph.pregel(ArrayBuffer(Array(0,0,0,0)),3,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)
    //rpdpGraph.edges.saveAsTextFile("D:\\文档\\数据\\社区发现\\RPDP")
   rpdpGraph.vertices.saveAsTextFile("user/output/RPDP01")
   /* rpdpGraph.vertices.foreach(a=>{
      println()
      print(" "+a._1+" ")

      a._2._2.foreach(b=>{
        b.foreach(c=>print(c))
        println()
      })
    })
*/
    //遍历结果
    //rpdpGraph.vertices.foreach(u=>u._2._2.foreach(u1=>println("节点id"+u._1+"节点属性"+u1.toString())))








  }
}

