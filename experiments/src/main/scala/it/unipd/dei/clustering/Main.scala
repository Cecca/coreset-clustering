package it.unipd.dei.clustering

import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object Main {

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input = opt[String](required = true)
    val k = opt[Int](required = true)
    val z = opt[Int](required = false)
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

    val centers: IndexedSeq[ProxyPoint[Vector]] =
      arguments.z.toOption match {
        case Some(z) =>
          val (centers, outliers) = Algorithm.mapReduce(
            vecs, arguments.k(), arguments.tau(), arguments.z(), dist)
          centers
        case None =>
          Algorithm.mapReduce(
            vecs, arguments.k(), arguments.tau(), dist)
      }

    val radius = Algorithm.radius(vecs, centers, arguments.z.getOrElse(0), dist)

    println(s"The radius is $radius")
  }

}
