package it.unipd.dei.clustering

import org.apache.spark.ml.linalg.Vector

object VectorUtils {

  def sum(a: Array[Double], b: Array[Double], result: Array[Double]): Unit = {
    val n = a.size
    var i = 0
    while (i < n) {
      result(i) = a(i) + b(i)
      i += 1
    }
  }

  def sqdist(a: Array[Double], b: Array[Double]): Double = {
    val n = a.size
    var sum: Double = 0.0
    var i = 0
    while (i < n) {
      val diff = a(i) - b(i)
      sum += diff*diff
      i += 1
    }
    math.sqrt(sum)
  }

}
