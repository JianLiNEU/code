import java.io.PrintWriter

import org.apache.spark.graphx.{Edge, EdgeContext, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object FindCircle {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")



    //从《path》/<userID>.enonet格式的文件路径解析出用户ID
    def extract(s: String)={
      val pattern="""^.*?(\d+).egonet""".r //使用匹配表达式来达到识别输入目录
      val pattern(num)=s//表示正则表达式匹配到s字符串会传到num
      num
    }

    //处理egonet文件的每行数据，返回元祖形式的边数据
    def get_edge_from_line(line: String):Array[(Long,Long)]={
      var ary=line.split(":")
      val srcId=ary(0).toInt
      val dstIds=ary(1).split(" ")
      val edges=for{
        dstId<-dstIds
        if(dstId != "")
      }yield {
        (srcId.toLong,dstId.toLong)
      }


      if (edges.size>0) edges else Array((srcId,srcId))
    }

    //从文件内容中构建边元组
    def make_edges(contens:String)={
      val lines=contens.split("\n")
      //这是练习时写的输入到外部文件中
      val out =new PrintWriter("D:\\文档\\数据\\Kaggle_Learning-social-circle\\demo.txt")
      //unflat是一个二维数组，{[(9948,10067),(9948,10252).....],[(9949,10106),(9949,9953,......)],[........]
      val unflat=for{
        line<-lines
      }yield {
        get_edge_from_line(line)
      }

      //unflat时间二维数组进行降维，成为一维数组
      val flat=unflat.flatten
      flat

    }

    //从Edge元祖构建一个对象，执行connectedComponents函数，返回String结果
    def get_Circles(flat:Array[(Long,Long)])={
      val edges=sc.makeRDD(flat)
      val g=Graph.fromEdgeTuples(edges,1)
      val cc=g.connectedComponents()
      cc.vertices.map( x=>(x._2 , Array(x._1))).
        reduceByKey((a,b)  => a ++ b).values.map(_.mkString(" ")).collect().mkString(";")

    }

    val egonets=sc.wholeTextFiles("D:\\文档\\数据\\Kaggle_Learning-social-circle\\egonets")
    val egonet_numbers=egonets.map(x=>extract(x._1)).collect()
    println(egonet_numbers.mkString("\n"))



    val egonet_edges=egonets.map(x=>make_edges(x._2)).collect()
    println("这是egonet_edges的输出：")
    for(i<-0 until egonet_edges.length ){
      println(egonet_edges(i))
    }



    val egonet_circles=egonet_edges.toList.map(x=> get_Circles(x))
    println("这是egonet_circles的输出语句：")
    println(egonet_circles.mkString("\n"))
    println("userId,Prediction")


    val result=egonet_numbers.zip(egonet_circles).map(x=>x._1+","+x._2)
    println(result.mkString("\n"))


  }


}
