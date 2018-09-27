package it.unipd.dei.clustering

import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object Sample {

  class Opts(args: Array[String]) extends ScallopConf(args) {

    val input = opt[String](required=true)
    val output = opt[String](required=true)
    val size = opt[Int](required=true)

    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("Clustering")
    val sc = new SparkContext(sparkConf)

    val original = VectorIO.readKryo(sc, opts.input())
    val sampled = original.takeSample(true, opts.size())

    VectorIO.writeKryo(sc.parallelize(sampled, 1), opts.output())
  }

}
