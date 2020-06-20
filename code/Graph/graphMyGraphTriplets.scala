package Graph

import org.apache.spark.graphx.{Edge, EdgeContext, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object graphMyGraphTriplets {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val myVertivces=sc.makeRDD(Array((1L,"Ann"),(2L,"Bill"),(3L,"Charles"),(4L,"Diane"),(5L,"Went to gym this morning")))
    val myEdges=sc.makeRDD(Array(Edge(1L,2l,"is-friends-with"),Edge(2L,3L,"is-friends-with"),Edge(3L,4L,"is-friends-with"),Edge(4L,5L,"Likes-status"),Edge(3L,5L,"Wrote_status")))
    /*（顶点ID，顶点姓名）
    （4,Diane)
    (2,Bill)
    (1,Ann)
    (3,Charles)
    (5,Went to gym this morning)*/
    val myGraph=Graph(myVertivces, myEdges)
    val arr=myGraph.vertices.collect()
    //进行Array的遍历，通过for循环进行遍历，不可以直接打印
       /* for (i<-0 until arr.length){
          println("("+arr(i)+")")
       }*/
      for (elem<-arr){
        println(elem)
      }

    //获得triplets形式的图数据
   /* ((2,Bill),(3,Chalers),is-friends-with)
    ((3,Charles),(4,Diane),is-friends-with)
    ((3,Charles),(5,Went to gym this morning),Wrote_status)
    ((4,Diane),(5,Went to gym this morning),Likes-status)*/
   val arrTriplets=myGraph.triplets.collect()
    for (elem<-arrTriplets){
      println(elem)
    }

    /*在边属性上增加布尔类型的属性来表示一个条件限制
    数据类型，首先将triplets数据进行map，map中t.attr是边的属性数据，将边的属性进行判断，
    判断之后(t.attr,t.attr=="is-friends-with"&&t.srcAttr.toLowerCase.contains("a")
    ((2,Bill),(3,Charles),(is-friends-with,false))
    ((1,Ann),(2,Bill),(is-friends-with,true))
    */
    val arrMapTriplets=myGraph.mapTriplets(t=>(t.attr,t.attr=="is-friends-with"&&t.srcAttr.toLowerCase.contains("a"))).triplets.collect()
    for (elem<-arrMapTriplets){
      println(elem)
    }
    /*使用aggregateMessages[]()计算每个定带你的出度
    sendToSrc:将Msg类型的消息发送给源顶点
    sendToDsc：将Msg类型的消息发送给目标定点
    (4,1)
     (2,1)
     (1,1)
     (3,2)
    */
    val arrAggreget=myGraph.aggregateMessages[Int](_.sendToSrc(1) ,_+_).collect()
    for (elem<-arrAggreget){
      println(elem)
    }
/*使用Rdd的join（）操作用于匹配VerticeId与顶点数据
结果数据
* (4,(1,Diane))
(2,(1,Bill))
(1,(1,Ann))
(3,(2,Charles))
* 原始数据一
* (4,1)    （顶点ID，顶点出度值）
     (2,1)
     (1,1)
     (3,2)
     原始数据二
     （4,Diane)（顶点Id，顶点姓名）
    (2,Bill)
    (1,Ann)
    (3,Charles)
*
* 使用map与remap整理输出
* myGraph.aggregateMessages[Int](_.sendToSrc(1) ,_+_).join(myGraph.vertices).map(_._2.swap)
* */
    val arrJoin=myGraph.aggregateMessages[Int](_.sendToSrc(1) ,_+_).join(myGraph.vertices).collect()
    for (elem<-arrJoin){
      println(elem)
    }
  /* 迭代的方式的Mapreduce,用于寻找距离最远的点 */
    def sendMsg(ec:EdgeContext[Int,String,Int]):Unit={
      //这是将原顶点的顶点属性+1发送给目标顶点
      ec.sendToDst(ec.srcAttr+1)
    //sendMsg函数最为参数传入aggregateMessages
      // 这个函数会在图中的每条边上被调用。这里sendMsg只是就爱你单的累加计数器
    }
    def mergeMsg(a:Int,b:Int):Int={
      //这是将源顶点给目标定点发送的距离+1后选取距离的最大值
    math.max(a,b)
      //这里我们定义了mergeMsg函数，这个函数会在在所有的消息传递到顶点后被重复利用
      //消息经过合并后，最后总的出的结果微博敖汉最大距离值得顶点

    }
    //这是一个递归函数，输入为一个
    def propagateEdgeCount(g:Graph[Int,String]):Graph[Int,String]={
      val vert=g.aggregateMessages[Int](sendMsg,mergeMsg)
      val g2=Graph(vert,g.edges)//这是新图，由顶点集与边集构成
      //检查新图的属性与上一轮迭代的顶点属性的比较，如果没有变化就是check=0
      val check=g2.vertices.join(g.vertices).
        map(x=>x._2._1-x._2._2).reduce(_+_)
      if(check>0)
        propagateEdgeCount(g2)
      else
        g

    }

    val initialgraph=myGraph.mapVertices((_,_)=>0)
    val arrMaxVertices=propagateEdgeCount(initialgraph).vertices.collect()
    for (elem<-arrMaxVertices){
      println(elem)
    }





}
}
