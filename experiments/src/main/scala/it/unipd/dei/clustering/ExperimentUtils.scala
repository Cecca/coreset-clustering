package it.unipd.dei.clustering

import scala.collection.JavaConversions

object ExperimentUtils {


  def jMap(tuples: (String, Any)*): java.util.Map[String, Object] =
    JavaConversions.mapAsJavaMap(tuples.map(fixTuple).toMap)

  def fixTuple(tuple: (Any, Any)): (String, Object) = {
    val second = tuple._2 match {
      case v: Boolean => v: java.lang.Boolean
      case c: Char => c: java.lang.Character
      case v: Byte => v: java.lang.Byte
      case v: Short => v: java.lang.Short
      case v: Int => v: java.lang.Integer
      case v: Float => v: java.lang.Float
      case v: Double => v: java.lang.Double
      case obj: java.lang.Object => obj
    }
    (tuple._1.toString, second)
  }

}
