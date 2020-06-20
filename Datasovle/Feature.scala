package Datasovle

import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}
import ml.sparkling.graph.operators.OperatorsDSL._

import scala.collection.mutable.ArrayBuffer
/*import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.measures.vertex.eigenvector.EigenvectorCentrality*/

object Feature {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Feature").setMaster("local[5]")
    val sc = new SparkContext(conf)

    //图的读取

    //val graph=GraphLoader.edgeListFile(sc, args(0))

    val graph=GraphLoader.edgeListFile(sc,"D:\\文档\\研究生课题20170424\\论文\\第一篇\\实验结果\\polblog.txt")

    val closecentralityGraph: Graph[Double, _] = graph.closenessCentrality()
    // Graph where each vertex is associated with its closeness centrality
    //val eic = EigenvectorCentrality.computeEigenvector(graph,VertexMeasureConfiguration(),(iteration,oldValue,newValue)=>iteration<999).vertices
    val eignvectorcentralityGraph: Graph[Double, _] = graph.eigenvectorCentrality()
    // Graph where each vertex is associated with its eigenvector centrality
    val freemanCentrality: Double= graph.freemanCentrality()
    // Freeman centrality value for graph
    val HitcentralityGraph = graph.hits(VertexMeasureConfiguration(treatAsUndirected=true))
    //val HitcentralityGraph: Graph[(Double,Double), _] = graph.hits()
    // Graph where each vertex is associated with its hits score (represented as a tuple (auth,hub):(Double,Double))
    //val degreecentralityGraph: Graph[(Int,Int), _] = graph.degreeCentrality()
    val degreecentralityGraph= graph.degreeCentrality(VertexMeasureConfiguration(treatAsUndirected=true))
    // Graph where each vertex is associated with its degree centrality in form of tuple (outdegree,indegree):(Int,Int)
    val neightcentralityGraph: Graph[Double, _] = graph.neighborhoodConnectivity()
    // Graph where each vertex is associated with its neighborhood connectivity
    val EMbedcentralityGraph: Graph[Double, _] = graph.vertexEmbeddedness()
    // Graph where each vertex is associated with its vertex embeddedness
    val localcentralityGraph: Graph[Double, _] = graph.localClustering()
    // Graph where each vertex is associated with its local clustering coefficient
    //val modularity: Double= graph.modularity()



    // Modularity value for graph
    val commonNeighbours: Graph[_, Int] = graph.commonNeighbours()
    // Graph where each edge is associated with number of common neighbours of vertices on edge
    var array=ArrayBuffer[(String,Double)]()
    array+=((("closecentrality"),closecentralityGraph.vertices.values.reduce((x,y)=>(x+y))/graph.vertices.count()))
    array+=((("eignvectorcentrality"),eignvectorcentralityGraph.vertices.values.reduce((x,y)=>(x+y))/graph.vertices.count()))
    array+=((("freeman"),closecentralityGraph.vertices.values.reduce((x,y)=>(x+y))/graph.vertices.count()))
    array+=((("neightcentrality"),neightcentralityGraph.vertices.values.reduce((x,y)=>(x+y))/graph.vertices.count()))
    array+=((("EM"),EMbedcentralityGraph.vertices.values.reduce((x,y)=>(x+y))/graph.vertices.count()))
    array+=((("localcentrality"),localcentralityGraph.vertices.values.reduce((x,y)=>(x+y))/graph.vertices.count()))
  array.foreach(println(_))


      HitcentralityGraph.vertices.values.foreach(println(_))
       println("度中心性")
       degreecentralityGraph.vertices.values.foreach(a=>{
         println(a)
       })






  }

}
