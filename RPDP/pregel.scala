/*
package RPDP

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, GraphLoader, VertexId}

import scala.collection.mutable.ArrayBuffer

object pregel {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RPDPP").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //图的读取
    val graph =GraphLoader.edgeListFile(sc,"D:\\文档\\数据\\社区发现\\karateDemo01.txt")

    val initialGraph = graph.reverse



    var vprog = {(vertexId: VertexId, vd: ArrayBuffer[String], message: Array[String]) =>
     ArrayBuffer(vertexId.toString,vertexId.toString,0.toString,1.toString)
    }

    // EdgeTriplet[vd,ed]  vd:目标顶点与原顶点的属性 ed；边顶点的属性
    val sendMessage = { (triplet: EdgeTriplet[ArrayBuffer[String] , Int]) =>
      {
       var arrayBuffer = new ArrayBuffer[String]()
        val sourceAttr = triplet.srcAttr
        for (i <- sourceAttr) {
          arrayBuffer +=(triplet.srcId.toString + i.charAt(1).toString + (i.charAt(2) + 1).toString + 0.toString)
        }

      //这是将要传入的节点属性和map中的节点属性都放在一起
      val arrayBuffer1=new ArrayBuffer[String]()
        //这样我们就可以不用定义map了,将目的节点属性加入到arrayBuffer中
        triplet.dstAttr.foreach(
          f=>{
            if(f!=None)
           println(f)
            // arrayBuffer+=f
          }
        )
        //将源节点的属性加入到Arraybuffer中
        if(triplet.srcAttr!=null){
          for(i<-triplet.srcAttr){
            //判断节点属性是否重复
            if(arrayBuffer.exists(f=>f(1)==i(1))) {
            }
            else
            {
              arrayBuffer1 += (triplet.dstId.toString ,i(1).toString ,(i(2).toInt+1).toString ,0.toString)
            }
          }
        }

        Iterator((triplet.dstId,arrayBuffer1.toArray.distinct))
      }

    }



    val reduceMessage = { (a1:Array[String],a2:Array[String]) => {
      var arrayBuffer=new ArrayBuffer[String]()
      arrayBuffer++=a1
      arrayBuffer++=a2
      arrayBuffer.toArray.distinct
    } }


    val bfs = initialGraph.pregel(Array("0000"))(vprog, sendMessage, reduceMessage)


  }

}

*/
