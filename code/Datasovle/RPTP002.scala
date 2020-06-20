package Datasovle
//处理数据
import java.io.PrintWriter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object RPTP002 {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("D:\\文档\\研究生课题20170424\\论文\\第二篇\\计算机应用研究\\FKDA\\CA-AstroPh-k50-G-R.txt")
    val line = source.getLines().toArray
    var map = new mutable.HashMap[Int, Int]()
    var array = new ArrayBuffer[Int]()
    for (i <- 0 until line.length) {
      if (line(i).contains("source")) {
        val arrayline = line(i).split("\\s+")
        println(arrayline(2))
        array+=arrayline(2).toInt
      }
      if(line(i).contains("target")){
        var arrayline2=line(i).split("\\s+")
        println(arrayline2(2))
        array+=arrayline2(2).toInt
      }

    }
    val out=new PrintWriter("D:\\文档\\研究生课题20170424\\论文\\第二篇\\计算机应用研究\\FKDA\\CA-AstroPh-k50.txt")

    for(j<-0 until array.length-1){
      if(j%2==0){
        println(array(j)+" "+array(j+1))
        out.println(array(j)+" "+array(j+1))
      }



    }
    out.close()


  }
}
