package it.unipd.dei.clustering

import ExperimentUtils._
import it.unipd.dei.experiment.Experiment
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.util.Random

object BaselineStreaming {

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input = opt[String](required = true)
    val k = opt[Int](required = true)
    val z = opt[Int](required = false)
    val m = opt[Int](default = Some(1))
    verify()

    def getExperiment(): Experiment = {
      new Experiment()
        .tag("input", input())
        .tag("k", k())
        .tag("z", z.getOrElse(-1))
        .tag("m", m())
        .tag("algorithm", "streaming-baseline")
    }
  }

  def main(args: Array[String]): Unit = {
    val dist: (Vector, Vector) => Double = {case (a, b) => math.sqrt(Vectors.sqdist(a, b))}

    val arguments = new Args(args)

    val experiment = arguments.getExperiment()

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("Streaming Clustering")
    val sc = new SparkContext(sparkConf)

    val vecs = VectorIO.readKryo(sc, arguments.input())
    val localVectors = vecs // Randomly partition the vectors
      .keyBy(_ => Random.nextLong())
      .repartition(vecs.getNumPartitions)
      .values
      .collect()
    val total = localVectors.length

    val (radius, time): (Double, Long) = if (arguments.z.isSupplied) {
      val algo = new ParallelStreamingOutliersClustering[Vector](arguments.k(), arguments.z(), arguments.m(), dist)
      val iter = localVectors.iterator
      val (_,t) = timed {
        var cnt = 0
        while (iter.hasNext) {
          cnt += 1
          if (cnt % (arguments.k() * arguments.z()) == 0) {
            println(s"--> $cnt / $total")
          }
          val p = iter.next()
          algo.update(p)
        }
      }
      val r = algo.radius(localVectors)
      (r, t)
    } else {
      val algo = new ParallelStreamingClustering[Vector](arguments.k(), arguments.m(), dist)
      val iter = localVectors.iterator
      val (_,t) = timed {
        while (iter.hasNext) {
          val p = iter.next()
          algo.update(p)
        }
      }
      val r = algo.radius(localVectors)
      (r, t)
    }

    val throughput = localVectors.size / (time/1000.0) // in points per second

    experiment.append("results", jMap(
      "throughput" -> throughput,
      "time" -> time,
      "radius" -> radius
    ))
    println(experiment.toSimpleString)
    System.setProperty("experiment.report.dir", System.getProperty("experiment.report.dir", "."))
    experiment.saveAsJsonFile(false, true)

  }

}
