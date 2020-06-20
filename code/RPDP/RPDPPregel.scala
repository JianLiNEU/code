/*
package RPDP

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

//应该使用Array（array）（123456,123245,0,2）这种情况使用String的话，就会一直在123这种地方
object RPDPPregel{
  //   sendMsg是从原顶点发送信息到目的顶点，如果顶点属性tuple第一个属性如果为1就要处理，为0则不处理。
  def sendMsg=(ec:EdgeTriplet[(Int, Array[ArrayBuffer[String]]), Int]) =>  {
    var arrayBufferSrc=new ArrayBuffer[ArrayBuffer[String]]

    ec.srcAttr._2.foreach(u=>({

      if(u!=null&&(!ec.dstAttr._2.exists(f=>f(1)==u(1)))){
        arrayBufferSrc+=ArrayBuffer(ec.dstId.toString,u(1).toString,(u(2).toInt+1).toString,u(3))
      }
    }))
   println("ec.srcattr:"+ec.srcAttr._2.foreach(x=>println(x)))
    println("array"+arrayBufferSrc.foreach(x=>println(x)))
    println("ec.disattr:"+ec.dstAttr._2.foreach(x=>println(x)))

    //println("array"+arrayBufferSrc.foreach(x=>println(x)))
      Iterator((ec.dstId,arrayBufferSrc.toArray.distinct))
  }


  def mergeMsg=(a1:Array[ArrayBuffer[String]], a2:Array[ArrayBuffer[String]]) => {

    val arrayBuffer=new ArrayBuffer[ArrayBuffer[String]]()
    arrayBuffer++=a1
    arrayBuffer++=a2
    arrayBuffer.toArray.distinct


  }

  def vprog=(vertexId:VertexId, VD:(Int, Array[ArrayBuffer[String]]),A: Array[ArrayBuffer[String]]) =>{

    var arrayBufferVP=new ArrayBuffer[ArrayBuffer[String]]()
    if(A==Array(ArrayBuffer("0000")))
    (VD._1,VD._2)


    else {
      A.foreach(A=>{
        if(!VD._2.exists(f=>A(1)==f(1))){
          arrayBufferVP+=A
        }
      })
      arrayBufferVP++=VD._2
      (VD._1,arrayBufferVP.toArray.distinct)
    }

  }


/*  def vprog(vertexId:VertexId, VD:(Int, Array[String]), message:Array[String]) : (Int, Array[String]) ={
    var arrayBuffer=new ArrayBuffer[String]()
    if (message.exists(_=="0000")){
      return(VD._1,VD._2)
    }
    else{

        arrayBuffer++=VD._2
        message.foreach(u => {
          if (VD._2.exists(p => {p(1) == u(1)})) {
          }
          else {
            arrayBuffer += (vertexId.toString + u(1) + (u(2).toInt -47).toString + u(3))
          }
        })


      return(VD._1,arrayBuffer.toArray)

    }


  }

  def sendMsg (triplet:EdgeTriplet[(Int, Array[String]), Int] ):Iterator[(VertexId, Array[String])] = {
    var arrayBuffer=new ArrayBuffer[String]()
    triplet.srcAttr._2.foreach(u=> {


      if (triplet.dstAttr._2.exists(p => p(1) == u(1))) {

      }
      else {
        arrayBuffer += u
      }
    }
    )
    Iterator((triplet.dstId,arrayBuffer.toArray.distinct))


  }

  def mergeMsg(a1:Array[String], a2:Array[String]) : Array[String] = {
    (a1++a2)
  }
  */
  def main(args: Array[String]): Unit = {
    //spark基本设置
    val conf = new SparkConf().setAppName("RPDP")
    val sc = new SparkContext(conf)
    //图的读取
    val graph =GraphLoader.edgeListFile(sc,"/user/input/soc-pokec",true , 10,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK)
    val random=new Random()
    val initalGraph=graph.reverse.mapVertices((id,attr)=>{
      if(random.nextInt(9)>1){(1,ArrayBuffer(Array(id.toString+id.toString+0.toString+0.toString)))}
    else{(0,ArrayBuffer(Array(id.toString+id.toString+0.toString+0.toString)))}
  }
  )

   // val initalGraph= graph.reverse.outerJoinVertices(graph.reverse.outDegrees){(id,attr,outg)=>outg}

    val rpdpGraph=initalGraph.pregel(ArrayBuffer(Array("0000")),4,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)


    //遍历结果
    //rpdpGraph.vertices.foreach(u=>u._2._2.foreach(u1=>println("节点id"+u._1+"节点属性"+u1.toString())))








  }
}



*/
