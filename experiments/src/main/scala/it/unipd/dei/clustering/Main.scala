package it.unipd.dei.clustering

import it.unipd.dei.experiment.Experiment
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import it.unipd.dei.clustering.ExperimentUtils.{appendTimers, jMap, timed}
import MemoryUtils._

import scala.util.Random

object Main {

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input = opt[String](required = true)
    val k = opt[Int](required = true)
    val z = opt[Int](required = false)
    val tau = opt[Int](required = false, default = k.toOption)
    val coreset = opt[String](required = false, default = Some("mapreduce"))
    val forceGmm = toggle(default = Some(false))
    val parallelism = opt[Int](required = false)
    verify()

    def getExperiment(): Experiment = {
      new Experiment()
        .tag("input", input())
        .tag("k", k())
        .tag("z", z.getOrElse(-1))
        .tag("tau", tau())
        .tag("coreset", coreset())
        .tag("force-gmm", forceGmm())
    }
  }

  def main(args: Array[String]): Unit = {

    val arguments = new Args(args)

    val experiment = arguments.getExperiment()

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("Clustering")
    val sc = new SparkContext(sparkConf)

    val parallelism = arguments.parallelism.getOrElse(sc.defaultParallelism)

    val vecs = VectorIO.readKryo(sc, arguments.input()).cache()

    val dist: (Vector, Vector) => Double = {case (a, b) => math.sqrt(Vectors.sqdist(a, b))}

    val (coreset, coresetTime) = arguments.coreset() match {
      case "mapreduce" =>
        experiment.tag("parallelism", parallelism)
        val repartitioned = vecs.repartition(parallelism).cache()
        repartitioned.count()
        println("Repartitioned the vectors")
        timed {
          Algorithm.mapReduce(repartitioned, arguments.tau() + arguments.z.getOrElse(0), dist)
        }
      case "streaming" =>
        experiment.tag("parallelism", 1)
        val localVectors = vecs // Randomly partition the vectors
          .keyBy(_ => Random.nextLong())
          .values
          .collect()
        val result@(c, t) = timed {
          Algorithm.streaming(localVectors.iterator, arguments.tau() + arguments.z.getOrElse(0), dist)
        }
        println("Fixing radii")
        c.fixRadii(localVectors.iterator)
        appendTimers("streaming-profiling", experiment, c.metricRegistry)
        result
      case "random" =>
        experiment.tag("parallelism", parallelism)
        val repartitioned = vecs.repartition(parallelism).cache()
        repartitioned.count()
        println("Repartitioned the vectors")
        timed {
          Algorithm.randomCoreset(repartitioned, arguments.tau() + arguments.z.getOrElse(0), dist)
        }
      case "none" =>
        experiment.tag("parallelism", 1)
        val c = new Coreset[Vector] {
          override def points: scala.Vector[ProxyPoint[Vector]] =
            vecs.map(ProxyPoint.fromPoint).toLocalIterator.toVector
        }
        (c, 0L)
    }

    val (centers, centersTime) = timed {
      (arguments.z.toOption, arguments.forceGmm()) match {
        case (Some(z), false) =>
          val neededBytes = 2*matrixBytes(coreset.points.size)
          val osc = if (neededBytes < freeBytes()) {
            println(s"Needed ${formatBytes(neededBytes)} with ${formatBytes(freeBytes())} free, using local implementation")
            None
          } else {
            println(s"Matrix would require ${formatBytes(neededBytes)}, using distributed implementation")
            Some(sc)
          }
          Outliers.run(coreset.points, arguments.k(), z, dist, osc)._1
        case (_, true) | (None, _) =>
          val centers = GMM.runParallel(coreset.points.map(_.point), arguments.k(), dist)
          centers.map(ProxyPoint.fromPoint)
      }
    }

    val radius = Algorithm.radius(vecs, centers, arguments.z.getOrElse(0), dist)

    println(s"The radius is $radius (coreset time $coresetTime, centers time $centersTime)")
    experiment.append("radius", jMap(
      "radius" -> radius))
    experiment.append("time", jMap(
      "component" -> "coreset",
      "time" -> coresetTime
    ))
    experiment.append("time", jMap(
      "component" -> "centers",
      "time" -> centersTime
    ))
    experiment.append("centers", jMap(
      "centers" -> centers.map(_.point.toArray).toArray
    ))
    experiment.append("coreset", jMap(
      "actual-coreset-size" -> coreset.points.size))

    experiment.saveAsJsonFile()
  }

}
