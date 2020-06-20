import java.io.File

import scala.io.Source

object scala_IO {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("D:\\word\\Leejian.txt")
    val lineIterator = source.getLines

    val line = source.getLines().toArray
    for (i <- 0 until line.length) {
      println(line(i))
    }
    for (c <- source) print(c)


    //访问目录的做法,首先输入一个目录，返回的是一个迭代器，我们自己也进行迭代
    def subdirs(dir: File): Iterator[File] = {
      val children = dir.listFiles().filter(_.isDirectory)
      children.toIterator ++ children.toIterator.flatMap(subdirs _)
    }

    val file = new File("D:\\")

    for (d<-subdirs(file))println(d)




  }

}
