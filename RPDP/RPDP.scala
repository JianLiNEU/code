package RPDP

import java.io.PrintWriter

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


object RPDP {
  def main(args: Array[String]): Unit = {
    //spark基本设置
    val conf = new SparkConf().setAppName("RPDP")
    val sc = new SparkContext(conf)
    //图的读取
    val graph =GraphLoader.edgeListFile(sc,"/user/input/soc-pokec")
    /*定义随机邻居表，
     * srcid     dstid     hops   tags
     * 原顶点   目的顶点  跳数  标志位（是否扰动）
     * (1,2,3,0),而不是原先的1230，因为有的节点为1000000这种，没有办法切分
     */
    //初始化，进行图反转,并进行顶点的初始化,将顶点的属性都置为0，active置为1
    val graphReverse=graph.reverse
    //定义超步
    var step =0
    // 对于发送消息时


    def sendMsg(ec:EdgeContext[Array[ArrayBuffer[String]],Int,Array[ArrayBuffer[String]]]):Unit={
      //这是将要传入的节点属性和map中的节点属性都放在一起
      val arrayBuffer=new ArrayBuffer[ArrayBuffer[String]]()
      //这样我们就可以不用定义map了,将目的节点属性加入到arrayBuffer中
      val arrayBufferSrc=new ArrayBuffer[ArrayBuffer[String]]()
      ec.dstAttr.foreach(
        f=>{if(f!=None)
          arrayBuffer+=f //目标节点的属性
        }
      )
      //将源节点的属性加入到Arraybuffer中
      if(ec.srcAttr!=null){
        for(i<-ec.srcAttr){
          //判断节点属性是否重复
          if(arrayBuffer.exists(f=>f(1)==i(1))) {
          }
          else
          {
            arrayBuffer += ArrayBuffer(ec.dstId.toString ,i(1).toString ,(i(2).toInt+1).toString ,1.toString)
          }

        }
      }
      ec.sendToDst(arrayBuffer.toArray.distinct)
    }




    def mergeMsg(a1:Array[ArrayBuffer[String]],a2:Array[ArrayBuffer[String]]):Array[ArrayBuffer[String]]={
      var arrayBuffer=new ArrayBuffer[ArrayBuffer[String]]()
      arrayBuffer++=a1
      arrayBuffer++=a2
      arrayBuffer.map(
        array=>(array(0),array(1),array(3),0.toString)
      )
      arrayBuffer.toArray.distinct
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

    //初始化，将顶点的值进行加入（srcid     dstid     hops   tags）
    val random=new Random
    val initialGraph=graphReverse.mapVertices((id,attr)=>(Array(ArrayBuffer(id.toString,id.toString,0.toString,0.toString))))
    //验证map中的元素
    val a=propageteEdge(initialGraph)
    //a.vertices.foreach((attr)=>println(attr))

    val a1=a.mapVertices(
      (id,attr)=>{
        if(attr==null)
          Array((ArrayBuffer(id.toString,id.toString,0.toString,0.toString)))
        else{
          attr.foreach(i=>println(i.toString()))
          attr
        }


      }
    ).vertices.collect()


/*
    //一个简单的随即函数

    //进行随机扰动，通过设置标志位，来进行随机扰动
    val random01=new Random
    val random02=new Random

    val arrayBufferEdge=new ArrayBuffer[String]()




    val newGraph=a1.mapVertices({case(id,(attr))=>
      val edge=new ArrayBuffer[String]()
      println(id + " " + attr)
      //看他是否有一跳邻居
      attr.foreach(f => {
        val arrayBufferEdge=new ArrayBuffer[String]()
        if (((f(2).toInt)-48)==1) {
          println("f是多少"+f)
          //random产生的随机数0-9
          if (random01.nextInt(9) > 1) {
            //寻找匹配的边
            attr.foreach(f1 =>
              if (((f1(2).toInt))!= 0 && ((f1(2).toInt)) != 1) {
                //要保持节点的可达性，要寻找二跳邻居，二跳邻居要与要扰动的节点是一跳邻居
                a1.vertices.foreach({
                  case(id,attr)=>{
                    if(id==(f1(1).toInt-48)&&attr.length!=0)
                    {
                      attr.foreach(f2=>{
                        if((f2(2).toInt)==1&&((f2(1).toInt)==(f1(1).toInt))){
                          arrayBufferEdge+=(f1(0).toInt).toString+(f2(0).toInt).toString
                        }
                      })
                    }
                  }
                })

              }
            )
            //如果说他没有二跳邻居，则不进行扰动
            if(arrayBufferEdge.length==0){
              edge += (f(0).toString , f(1).toString)
            }else {
              val a = arrayBufferEdge(random02.nextInt(arrayBufferEdge.length))
              edge += a

              println("扰动的边 " + a)
            }
          }
          else //设置的随机函数看是否扰动,这是剩下的不扰动的边
          {
            println("不用动的边" +(f(0).toString ,f(1).toString))
            edge += (f(0).toString ,f(1).toString)

          }
        } //
      }
      )
      //返回edge为顶点的新属性
      edge
    })
    /* newGraph.vertices.foreach(attr=>
     println(attr))*/
    newGraph.vertices.saveAsTextFile("/user/output/RPDP01")*/















  }

}

