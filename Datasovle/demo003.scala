package Datasovle

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

object demo003 {
  def main(args: Array[String]): Unit = {
    var a=Map(1->ArrayBuffer(1,2,3))
    var b=Map(2->ArrayBuffer(2,3,4))
    val c=a++b
    c(1)=ArrayBuffer(2)
    c.foreach(println(_))





  }
}
