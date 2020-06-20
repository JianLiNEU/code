package Datasovle

import java.io.PrintWriter

import scala.io.Source

object
demo004 {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("D:\\文档\\研究生课题20170424\\论文\\第一篇\\实验结果\\polblog.txt")
    //val out = new PrintWriter("D:\\文档\\研究生课题20170424\\论文\\第四篇小论文(未投)\\实验\\ADNR\\ADNR\\email\\email2_0.1\\part-00004")
    val line = source.getLines().toArray

  }

}
