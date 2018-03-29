package it.unipd.dei.clustering

import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object Inflate {

  class Opts(args: Array[String]) extends ScallopConf(args) {

    val input = opt[String](required=true)
    val output = opt[String](required=true)
    val replicas = opt[Int](required=true)
    val factor = opt[Double](required=false, default=Some(1.1))

    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("Clustering")
    val sc = new SparkContext(sparkConf)

    val original = VectorIO.readKryo(sc, opts.input())

    val dimZeroMin = original.map(v => v(0)).min()
    val dimZeroMax = original.map(v => v(0)).max()
    val dimZeroExtent = math.abs(dimZeroMax - dimZeroMin)
    println(s"The extent of the 0-th dimension is $dimZeroExtent")
    val baseOffset = opts.factor() * dimZeroExtent
    val replicas = opts.replicas()
    println(s"Writing $replicas replicas with base offset $baseOffset")

    val inflated = original.flatMap({v =>
      (0 until replicas).map { i =>
        val offset = i*baseOffset
        val out = v.copy.toArray
        out(0) = v(0) + offset
        Vectors.dense(out)
      }
    })

    VectorIO.writeKryo(inflated, opts.output())
  }

}
