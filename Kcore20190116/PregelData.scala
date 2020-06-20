/*
package Kcore20190116

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.serializer.KryoSerializer

class PregelData(var verticeSrc :Int , var RNTList: Array[Int])extends Serializable with KryoSerializer{
  def this() = this(0,Array(0))
  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, this.verticeSrc)
    kryo.writeObject(output, this.RNTList)


  }
  override def read(kryo: Kryo, input: Input): Unit = {
    this.verticeSrc = kryo.readObject(input, classOf[Int])
    this.RNTList = kryo.readObject(input, classOf[Array[Int]])


  }

}
*/
