package FKDA

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, GraphLoader, VertexId}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
object FKDAmain {


  def mergeMsg(A1:Array[Int], A2:Array[Int]) :Array[Int] = {
    A1++A2
  }


  def vprog(vid:VertexId, value:Array[Int], message:Array[Int]):Array[Int] = {
    if(message.contains(0))value
    else{
      val array=new ArrayBuffer[Int]()
      array++=message
      array.toArray
    }
  }

  def senxdMsg(ET:EdgeTriplet[Array[Int], Int]): Iterator[(VertexId, Array[Int])] = {
    Iterator((ET.srcId ,Array(ET.dstId.toInt)),(ET.dstId,Array(ET.srcId.toInt)))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FKDA")
    val sc = new SparkContext(conf)

    //图的读取

    val graph=GraphLoader.edgeListFile(sc, args(0))
   // val graph=GraphLoader.edgeListFile(sc,"D:\\文档\\数据\\社区发现\\karateDemo01.txt")
    val k=50
    val n=graph.vertices.collect().size //节点总数目
    val vertex=graph.degrees.sortBy(_._2,false).collect()

    //寻找节点一邻居
    val graphMap=graph.mapVertices((id,vt)=>Array(0))
    val pregelGraph=graphMap.pregel(Array(0),1,EdgeDirection.Either)(vprog,senxdMsg,mergeMsg)

    //FKDA求节点目标度数
    var i=0
    var j=1
    val anonyGroup=vertex.map(value=>{
      if(vertex(j)._2==vertex(i)._2&(n-j)>=k){
        j=j+1
        (value._1,(value._2,value._2))
      }
      else{
        if(j-i<=k&(n-j)>=k){
          j=j+1
          (value._1,(value._2,vertex(i)._2))}
        else if(j-i>=k&(n-j)>=k){
          i=j
          (value._1,(value._2,value._2))
        }
        else {
          j=j+1
          (value._1,(value._2,vertex(i)._2))
        }
      }
    })
    anonyGroup.foreach(println(_))
    //添加边
    var candidate=1000000000
    val anonyGraphPre=pregelGraph.vertices.collect()
    var anonyResult=Map[VertexId,(Int,Int)]()
    anonyResult++=anonyGroup.toMap
    val anonyPre=ArrayBuffer[String]()
    anonyGraphPre.foreach(value=>{
      var number=anonyResult(value._1)._2-anonyResult(value._1)._1
      if(number>0){
        anonyResult.keys.foreach(key=>{
          if((anonyResult(key)._2-anonyResult(key)._1)>0&number>0&(!value._2.contains(key))&key!=value._1){
            anonyResult(value._1)=(anonyResult(value._1)._1+1,anonyResult(value._1)._2)
            number=number-1
            val a=anonyResult(key)
            anonyResult(key)=(a._1+1,a._2)
            anonyPre+=(value._1+" "+key)
          }
        })
      }
      if(number>0){
        for (i<-0 until(number)){
          candidate+=1
          anonyPre+=(value._1+" "+candidate)

        }
      }
    })
   sc.makeRDD(anonyPre).coalesce(1,true).saveAsTextFile(args(1))









  }

}
