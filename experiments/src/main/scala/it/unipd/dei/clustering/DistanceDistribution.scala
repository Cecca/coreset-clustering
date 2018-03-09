package it.unipd.dei.clustering

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object DistanceDistribution {

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input = opt[String](required = true)
    val output = opt[String](required = false, default = Some(input.toOption.get + ".distances"))
    verify()
  }

  def distance(a: Vector, b: Vector): Double = math.sqrt(Vectors.sqdist(a, b))

  def main(args: Array[String]): Unit = {
    val arguments = new Args(args)

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("Clustering")
    val sc = new SparkContext(sparkConf)

    val vecs = VectorIO.readKryo(sc, arguments.input()).cache()

    val distances = vecs.cartesian(vecs).map({case (a, b) =>
      distance(a, b)
    })

    println(s"Max distance is ${distances.max()}")
  }

}
