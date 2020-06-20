package Datasovle


import java.io.PrintWriter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

object demo002 {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("D:\\文档\\研究生课题20170424\\论文\\第二篇\\实验\\实验数据\\实验原始数据\\APINC\\CA-GrQc\\CA-GrQc45\\part-00001")
    val out=new PrintWriter("D:\\文档\\研究生课题20170424\\论文\\第二篇\\实验\\实验数据\\实验原始数据\\APINC\\CA-GrQc\\CA-GrQc45\\part-00002")
    val line = source.getLines().toArray

       for(i<-line){

         val a=i.split(" ")

           out.println(a(0).toInt*100+" "+a(1))

       }
    out.close()
  }




}
