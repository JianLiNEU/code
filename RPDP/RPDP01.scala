/*
package RPDP

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeContext, Graph, GraphLoader}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object RPDP01 {
  def main(args: Array[String]): Unit = {
    //spark基本设置
    val conf = new SparkConf().setAppName("RPDP").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //图的读取
    val graph =GraphLoader.edgeListFile(sc,"D:\\文档\\数据\\社区发现\\karateDemo01.txt")
    val random01=new Random()
    var step=0
    //定义需要匿名的节点
    val graphReverse=graph.reverse.mapVertices((id,attr)=>{
      if (random01.nextInt(9) > 1) {
        1
      }
    })



    def sendMsg(ec:EdgeContext[Array[ArrayBuffer[String]],Int,Array[ArrayBuffer[String]]]):Unit={

    }

    def mergeMsg(a1:Array[ArrayBuffer[String]],a2:Array[ArrayBuffer[String]]):Array[ArrayBuffer[String]]={

    }


    def propageteEdge(g:Graph[Array[ArrayBuffer[String]],Int]):Graph[Array[ArrayBuffer[String]],Int]={
      val verts=g.aggregateMessages[Array[ArrayBuffer[String]]](sendMsg,mergeMsg)
      val g2=Graph(verts,g.edges)
      step=step+1
      if(step<3) {
        propageteEdge(g2)
      }
      else
        g
    }

    val initialGraph=graphReverse.mapVertices((id,attr)=>if(attr==1){(Array(ArrayBuffer(id.toString,id.toString,0.toString,0.toString)))})










  }

}
*/
