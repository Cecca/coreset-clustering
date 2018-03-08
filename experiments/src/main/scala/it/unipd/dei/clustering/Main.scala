package it.unipd.dei.clustering

import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object Main {

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input = opt[String](required = true)
    val k = opt[Int](required = true)
    val z = opt[Int](required = true)
    val tau = opt[Int](required = false, default = k.toOption)
    verify()
  }


  def main(args: Array[String]): Unit = {

    val arguments = new Args(args)

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("Clustering")
    val sc = new SparkContext(sparkConf)

    val vecs = VectorIO.readKryo(sc, arguments.input()).repartition(8).cache()
    println(s"Loaded ${vecs.count()} vectors")

    val dist: (Vector, Vector) => Double = {case (a, b) => math.sqrt(Vectors.sqdist(a, b))}

    val (centers, outliers, radius) = Algorithm.mapReduce(
      vecs, arguments.k(), arguments.tau(), arguments.z(), dist)

    println(s"The radius is $radius")
  }

}
