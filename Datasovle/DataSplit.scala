package Datasovle
import java.io.PrintWriter

import scala.io.Source

object DataSplit {
  def main(args: Array[String]): Unit = {
    val address="D:\\文档\\研究生课题20170424\\论文\\第三篇\\IEEE ACCESS\\实验\\ADSW\\polblog\\AddDelete\\polblog301\\"
   /* val source01 = Source.fromFile(address+"part-00000")
    val source02 = Source.fromFile(address+"part-00001")
    val out1=new PrintWriter(address+"part-00003")
    (source01++source02).foreach(a=>out1.print(a))*/
    val source = Source.fromFile(address+"part-00000")
    val out=new PrintWriter(address+"part-00001")


    val line = source.getLines().toArray
//分割同一行中多个节点对
    /*    for(i<-line){
       // 以\\s+分割多个空格字符串
    val a=i.split("\\s+")
       out.println(a(0)+" "+a(1))
      val a=i.dropRight(1).split(",")
       out.println(a(2))
       val a=i.split("  ")
       a.foreach(b=>{
         out.println(b)
       })
     }*/
    //分割/t
    for(i<-line){
      // 以\\s+分割多个空格字符串
      val a=i.split("   ")
      a.filter(_.size>0).foreach(b=>{

           out.println(b)

      })

    }
    out.close()
  }

}
