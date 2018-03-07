package it.unipd.dei.clustering

import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object Main {

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input = opt[String](required = true)
    val k = opt[Int](required = true)
    val z = opt[Int](required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {

    val arguments = new Args(args)

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("Clustering")
    val sc = new SparkContext(sparkConf)

    val vecs = VectorIO.readKryo(sc, arguments.input())
    println(s"Loaded ${vecs.count()} vectors")

    val lVecs = vecs.collect().map(WeightedPoint(_, 1L))
    val (centers, outliers) = Algorithm.mapReduce(vecs, arguments.k(), arguments.k(), arguments.z(), VectorUtils.sqdist)
    val radius = Utils.maxMinDistance(lVecs.map(_.point), centers, VectorUtils.sqdist)
    println(s"There are ${outliers.size} outliers, the radius is $radius")
  }

}
