package DGPA

import scala.collection.mutable.ArrayBuffer

/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class VertexState extends Serializable{


  var changed = true
  var RNTE=Map[Int,Array[Int]]()
  var nerRNTE=Map[Int,ArrayBuffer[Int]]()
  var superStep=0

  override def toString = s"VertexState($RNTE)"
}