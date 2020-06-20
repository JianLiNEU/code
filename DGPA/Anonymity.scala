package DGPA

import org.apache.spark.graphx.Graph

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Anonymity {
  def anonyRPDP(searchResult: Graph[VertexState, Int], p: Int) = {
    val result=searchResult.mapVertices((vid,Vertice)=>{

      val newVertice=Vertice
      Vertice.RNTE.values.filter(_(2)==1).foreach(RNTE1=>{
        if(RNTE1(3)>p){}
          //遍历跳数大于1，寻找候选节点
        else {
          var number=0
          newVertice.RNTE.values.filter(_(2)>1).foreach(RNTE2=>{
            if(newVertice.nerRNTE.keySet.contains(RNTE1(1))&RNTE2(3)<p&number<1){
              if (newVertice.nerRNTE(RNTE1(1)).contains(RNTE2(1))){
                number+=1
                newVertice.RNTE(RNTE2(1))(3)=100
              }
            }
          })
          if(number==0){//扰动失败

            newVertice.RNTE.values.filter(a=>a(2)>1&a(3)<p).foreach(RNTE2=>{
              if(RNTE2(3)<p&number<1){
                number+=1
                newVertice.RNTE(RNTE2(1))(3)=100
              }
            })
            if(number==0)newVertice.RNTE(RNTE1(1))(3)=101
          }
        }
      })
      newVertice
    })

    result


  }


/*  def anonyRPDP(searchResult: Graph[VertexState, Int], p: Int) = {
    var candidate=ArrayBuffer[Int]()
    val random=new Random()
    val result=searchResult.mapTriplets(triplets=>{
      if(triplets.dstAttr.RNTE(triplets.srcId.toInt)(2)>p){triplets.srcId.toString+" "+triplets.dstId.toString}
      else{
        candidate.clear()
        triplets.dstAttr.RNTE.values.filter(_(2)>1).foreach(key=>{
          if(triplets.dstAttr.nerRNTE.contains(key(1))){
            if(triplets.dstAttr.nerRNTE(key(1)).contains(triplets.srcId.toInt)){
              candidate+=key(1)
            }
          }

        })
        if(candidate.size>0){
          triplets.dstId.toString+" "+candidate(random.nextInt(candidate.size)).toString
        }
        else{
          triplets.dstAttr.RNTE.values.filter(_(2)>1).foreach(key=>{
            if(key(3)!=101){
              candidate+=key(1)
            }
          })
          if(candidate.size>0){
            val candidateNode=candidate(random.nextInt(candidate.size))
            triplets.dstAttr.RNTE(candidateNode)(3)=101
            triplets.dstId+" "+candidateNode
          }
          else{
            triplets.srcId.toString+" "+triplets.dstId.toString
          }

        }

      }
    })
    result

  }*/



//匿名DNR
  def anonyDNR(searchResult: Graph[VertexState, Int], p: Int) = {

    val result=searchResult.vertices.mapValues(attr=>{
      var number=0
      var nodeReturn=""
      //计算需要扰动几条边
      attr.RNTE.values.filter(_(2)==1).foreach(value=>{
          if(value(3)<p) number+=1
        else{nodeReturn+="   "+value(0)+" "+value(1)}
      })
      //在候选节点挑选扰动边
      attr.RNTE.values.filter(_(2)>1).foreach(value1=>{
        if(number>0) {
          nodeReturn += "   "+value1(0) + " " + value1(1)
          number-=1
        }
      })
      //扰动失败，需要在1邻居中挑选边
      if(number>0){
        attr.RNTE.values.filter(_(2)==1).foreach(value=>{
          if(value(3)<p&number>0){
          nodeReturn+="   "+value(0)+" "+value(1)
          number-=1}
        })
      }
      nodeReturn
    })
   result
  }


}
