package Datasovle

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random/**/

object demo001 {
  def main(args: Array[String]): Unit = {

    var map = new mutable.HashMap[Int, ArrayBuffer[Array[Long]]]()
    map(1) = ArrayBuffer(Array(10L))

    var list = new mutable.ListMap[Int, Int]
    val array =ArrayBuffer[Array[String]]()


    val a=Array("23","22","34")
    println(a.size+" ____________")
    println(a.drop(a.size).size)
    println(a(1).toInt)
    val a1=Array(Array(1),Array(2))
    a1.foreach(f=>f.foreach(w=>println(w)))

    val array01=Array(Array("000"),Array("111"),Array("222"))
    println(array01.flatten)
    val array02=Array(ArrayBuffer("0000"))
    if(ArrayBuffer("0000").equals(array02(0)))println("you are win")
    val  a12="zxcv"
    println(a12(1))
    val array03=new ArrayBuffer[String]()
    val array04=Array("0000")
    if(array04.exists(_=="0000"))println("yes")
    val a11=Array(1.toString)
    println(a11(0).toInt)
    val array004=Array(Array(1111,2222,3333),Array(4444,5555,6666))
    array004.foreach(a=>println(a(0)))
    val map01=mutable.Map(1->"alice",2->"bob",3->"jerry")
    val map04=mutable.Map(1->"alice",2->"bob",3->"jerry")


    println("-----------------------------------")
    map01.keys.foreach(v=>println(v))
    map04.keys.foreach(v=>println(v))
    println(map01(1)(1))
    val map02=mutable.Map()
    map02.keys.foreach(println(_))

    var candidateArray=ArrayBuffer[Int]()
    candidateArray+=1

    candidateArray+=2
    println("------"+candidateArray)

    for (elem <- map01.keys) {
      println(map01(elem))
    }
    if (!map01.contains(3)){
      println("hello alice")
    }else{
      println("no member")
    }
    val random=new Random()
    for(i<-1 to 100){
      print(random.nextInt(10)+" ")
    }

   /* val conf = new SparkConf().setAppName("RPDP").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val pointTest01: RDD[(VertexId,Int)] = sc.parallelize(Array((1L,10),(2L,10),(3L,10)))
    val pointTest: RDD[(VertexId,Int)] = sc.parallelize(Array((1L,20),(2L,20),(3L,20)))
    val result=pointTest.join(pointTest01)
    result.foreach(v=>println(v._1+" "+v._2))

    val myVertice=sc.makeRDD(Array((1L ,(10,20,30)) , (2L, (21,22,23)),(3L,(41,42,43))))
    val myEdge=sc.makeRDD(Array(Edge(1L,2L,40),Edge(2L,3L,50),Edge(1L,3L,60)))
    val newGraph=Graph(myVertice,myEdge)
    newGraph.triplets.foreach(f=>{
      println(f)
    })*/










  }
}
