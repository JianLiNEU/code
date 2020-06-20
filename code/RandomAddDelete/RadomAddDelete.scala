package RandomAddDelete

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random



object RadomAddDelete {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AddDelete").setMaster("local[5]")
    val sc = new SparkContext(conf)
    val originGraph=sc.textFile("D:\\文档\\研究生课题20170424\\论文\\第三篇\\IEEE ACCESS\\实验\\萤火虫实验数据集\\polblog.txt")
    //val originGraph=sc.textFile(args(0))
    val p=30
    val random=new Random()
    val splitVertice=originGraph.map(a=>{
      val barray=a.split(" ")
      (barray(0),barray(1))
    }).groupByKey()
    //节点列表
    val Vertice=originGraph.map(a => a.split(" ")).collect().flatten.distinct.toBuffer
    val verticeNum=Vertice.size

    val candidateArray=ArrayBuffer[String]()

    val result=splitVertice.map(attr=>{
      candidateArray.clear()
      var candidate=""
      attr._2.foreach(cand=>{
        if(random.nextInt(100)>p){//不需要扰动
          candidate+="   "+attr._1+" "+cand
        }
        else {//需要扰动
          var flag=true
          while(flag){
            val candone=Vertice(random.nextInt(verticeNum))
            if((!candone.equals(attr._1))&(!attr._2.toBuffer.contains(candone))&(!candidateArray.contains(candone))){
              candidate+="   "+attr._1+" "+candone
              candidateArray+=candone
              flag=false
            }
          }
        }
      })
      candidate
    })

   result.coalesce(1,true)saveAsTextFile("D:\\文档\\研究生课题20170424\\论文\\第三篇\\IEEE ACCESS\\实验\\ADSW\\polblog\\AddDelete\\email30")





  }


}
