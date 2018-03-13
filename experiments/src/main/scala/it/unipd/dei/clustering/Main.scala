package it.unipd.dei.clustering

import it.unipd.dei.experiment.Experiment
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import it.unipd.dei.clustering.ExperimentUtils.{jMap, timed}

object Main {

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input = opt[String](required = true)
    val k = opt[Int](required = true)
    val z = opt[Int](required = false)
    val tau = opt[Int](required = false, default = k.toOption)
    val coreset = opt[String](required = false, default = Some("mapreduce"))
    val forceGmm = toggle(default = Some(false))
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

    experiment.tag("parallelism", sc.defaultParallelism)

    val vecs = VectorIO.readKryo(sc, arguments.input()).repartition(sc.defaultParallelism).cache()
    println(s"Loaded ${vecs.count()} vectors")

    val dist: (Vector, Vector) => Double = {case (a, b) => math.sqrt(Vectors.sqdist(a, b))}

    val (coreset, coresetTime) = timed {
      arguments.coreset() match {
        case "mapreduce" =>
          Algorithm.mapReduce(vecs, arguments.tau() + arguments.z.getOrElse(0), dist)
        case "streaming" =>
          Algorithm.streaming(vecs.toLocalIterator, arguments.tau() + arguments.z.getOrElse(0), dist)
        case "random" =>
          Algorithm.randomCoreset(vecs, arguments.tau() + arguments.z.getOrElse(0), dist)
        case "none" =>
          new Coreset[Vector] {
            override def points: scala.Vector[ProxyPoint[Vector]] =
              vecs.map(ProxyPoint.fromPoint).toLocalIterator.toVector
          }
      }
    }

    val (centers, centersTime) = timed {
      (arguments.z.toOption, arguments.forceGmm()) match {
        case (Some(z), false) =>
          Outliers.run(sc, coreset.points, arguments.k(), z, dist)._1
        case (_, true) | (None, _) =>
          val centers = GMM.run(coreset.points.map(_.point), arguments.k(), dist)
          centers.map(ProxyPoint.fromPoint)
      }
    }

    val radius = Algorithm.radius(vecs, centers, arguments.z.getOrElse(0), dist)

    println(s"The radius is $radius (coreset time $coresetTime, centers time $centersTime)")
    experiment.append("radius", jMap(
      "radius" -> radius))
    experiment.append("time", jMap(
      "coreset" -> coresetTime,
      "centers" -> centersTime
    ))

    experiment.saveAsJsonFile()
  }

}
