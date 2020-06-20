package Gephi

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, GraphLoader}

object RPTP {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("toGraph").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val graph =GraphLoader.edgeListFile(sc,"D:\\文档\\数据\\社区发现\\karateDemo.txt")

    def toGexf[VD,ED](g:Graph[VD,ED])=
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+
        "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n"+
        "  <graph mode=\"static\" defaultedgetype=\"directed\">\n"+
        " <nodes>\n"+
        g.vertices.map(v=>" <node id=\""+v._1+"\" lable=\""+v._2+"\"/>\n").collect.mkString+
        " </nodes>\n"+
        "<edges>\n" +
        g.edges.map(e=>"    <edge source=\""+e.srcId+
          "\" target=\""+e.dstId+"\" label=\""+e.attr+
          "\"/>\n").collect.mkString+
        "  </edges>\n"+
        " </graph>\n"+
        " </gexf>"

    val pw=new PrintWriter("karateDemo.gexf")
    pw.write(toGexf(graph))
    pw.close()

  }

}
