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

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("vectorize")
    val sc = new SparkContext(sparkConf)

    val vecs = VectorIO.read(sc, arguments.input())

    val lVecs = vecs.collect().map(WeightedPoint(_, 1L))
    val (centers, outliers) = Outliers.run(lVecs, arguments.k(), arguments.z(), VectorUtils.sqdist _)
    println(centers.mkString(", "))
    println(s"There are ${outliers.size} outliers")
  }

}
