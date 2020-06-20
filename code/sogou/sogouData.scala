package sogou

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object sogouData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd = sc.textFile("D:\\文档\\数据\\SogouQ.sample")

    var map=new mutable.HashMap[String,Int]()
    val data = rdd.flatMap(_.split(" ")).filter(_.contains("com")).collect().mkString

    var datasplit=data.split("\\.")
    for(i<-0 until datasplit.length){

      var result=0
      if(datasplit(i).contains("com")){
        result=i-1
        if(map.contains(datasplit(result))){
          map(datasplit(result))+1
        }
        else
        {
          map(datasplit(result))=1

        }
      }
    }
    val wordRdd= println(sc.parallelize(map.toSeq).reduceByKey(_ + _).collect().mkString)













  }

}
