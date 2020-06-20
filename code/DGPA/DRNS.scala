package DGPA

import org.apache.spark.graphx._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object DRNS {
  val p=39
  def vprog01(vid:VertexId, value:VertexState,message: VertexState): VertexState = {
    val nodeValue=new VertexState()
    nodeValue.RNTE++=value.RNTE
    nodeValue.nerRNTE++=value.nerRNTE
    if(message.RNTE.size==0) return value
    else{
      nodeValue.superStep=value.superStep+1
      message.RNTE.keys.foreach(key=>{
        if(value.RNTE.contains(key)){}
        else{
          val random=new Random()
          if(nodeValue.superStep==1){
            val randomNumber=random.nextInt(100)
            nodeValue.RNTE+=(key->Array(vid.toInt,message.RNTE(key)(1),message.RNTE(key)(2)+1,randomNumber))
            if(randomNumber>p){
              if(nodeValue.nerRNTE.size==0) nodeValue.nerRNTE+=(vid.toInt->ArrayBuffer(message.RNTE(key)(1)))
              else nodeValue.nerRNTE(vid.toInt)+= message.RNTE(key)(1)
            }

          }
          else  {
            nodeValue.RNTE+=(key->Array(vid.toInt,message.RNTE(key)(1),message.RNTE(key)(2)+1,0))
            nodeValue.nerRNTE++=message.nerRNTE
          }
        }
      })
      if(nodeValue.RNTE.size==value.RNTE.size) {
        nodeValue.changed=false
        nodeValue.superStep-=1

      } //判断节点是否为active
      else nodeValue.changed=true
      return nodeValue
    }
  }

  def senxdMsg01(ET:EdgeTriplet[VertexState, Int]): Iterator[(VertexId, VertexState)] = {
    if(ET.srcAttr.changed) Iterator((ET.dstId,ET.srcAttr))
    else  Iterator.empty
  }

  def mergeMsg01(A1:VertexState,A2: VertexState) : VertexState ={
    val state=new VertexState()
    state.RNTE++=A1.RNTE
    state.RNTE++=A2.RNTE
    state.nerRNTE++=A1.nerRNTE
    state.nerRNTE++=A2.nerRNTE
   state
  }
  //RPDP寻找邻居节点与可达节点
  def NerSearchResult(reverseGraph: Graph[VertexState, Int], r: Int) = {
    val state=new VertexState()
    val SearchResultGraph=reverseGraph.pregel(state,r,EdgeDirection.Either)(vprog01,senxdMsg01,mergeMsg01)
    SearchResultGraph
  }













  //DNR寻找邻居节点

  def vprog(vid:VertexId,value: VertexState, message:Map[Int, Array[Int]]) :VertexState = {

    val nodeValue=new VertexState()
    nodeValue.RNTE++=value.RNTE
    if(message.contains(0))return value
    else{
      nodeValue.superStep=value.superStep+1
      message.keys.foreach(key=>{
        if(nodeValue.RNTE.contains(key)){}
        else{
          val random=new Random()
          if(nodeValue.superStep==1){
            nodeValue.RNTE+=(key->Array(vid.toInt,message(key)(1),message(key)(2)+1,random.nextInt(100)))
            if(nodeValue.nerRNTE.size==0) nodeValue.nerRNTE+=(vid.toInt->ArrayBuffer(message(key)(1)))
            else nodeValue.nerRNTE(vid.toInt)+= message(key)(1)
          }
          else  {
            nodeValue.RNTE+=(key->Array(vid.toInt,message(key)(1),message(key)(2)+1,0))
            nodeValue.nerRNTE=value.nerRNTE
          }
        }
      })
      if(nodeValue.RNTE.size==value.RNTE.size) nodeValue.changed=false //判断节点是否为active
      else nodeValue.changed=true
      return nodeValue
    }
  }

  def senxdMsg(ET:EdgeTriplet[VertexState, Int] ):Iterator[(VertexId, Map[Int, Array[Int]])] = {
    if(ET.srcAttr.changed)Iterator((ET.dstId,ET.srcAttr.RNTE))
    else Iterator.empty
  }

  def mergeMsg (A1:Map[Int, Array[Int]], A2:Map[Int, Array[Int]]): Map[Int, Array[Int]] = {
    (A1++A2)
  }

  def SearchResult(initGraph: Graph[VertexState, Int], r: Int) = {
    val SearchResultGraph=initGraph.pregel(Map(0->Array(0)),r,EdgeDirection.Either)(vprog,senxdMsg,mergeMsg)
    SearchResultGraph

  }




}
