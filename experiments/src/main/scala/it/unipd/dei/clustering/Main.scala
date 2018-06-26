package it.unipd.dei.clustering

import it.unipd.dei.experiment.Experiment
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import it.unipd.dei.clustering.ExperimentUtils.{appendTimers, jMap, timed}
import MemoryUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object Main {

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input = opt[String](required = true)
    val k = opt[Int](required = true)
    val z = opt[Int](required = false)
    val sizeFactor = opt[Double](required = false, default = Some(1), validate = _ >= 1.0)
    val zFactor = opt[Double](required = false, default = Some(1), validate = _ >= 1.0)
    val smallCoreset = opt[String](default=Some("no"), validate = s => Set("yes", "no").contains(s))
    val coreset = opt[String](required = false, default = Some("mapreduce"))
    val forceGmm = toggle(default = Some(false))
    val parallelism = opt[Int](required = false)
    verify()

    def getExperiment(): Experiment = {
      new Experiment()
        .tag("input", input())
        .tag("k", k())
        .tag("smallCoreset", smallCoreset())
        .tag("z", z.getOrElse(-1))
        .tag("zFactor", zFactor())
        .tag("sizeFactor", sizeFactor())
        .tag("coreset", coreset())
        .tag("force-gmm", forceGmm())
    }
  }

  def countOutliers(points: RDD[Vector]): Seq[Int] = {
    points.mapPartitions({ vs =>
      Iterator.single(vs.count(x => x(0) > 100))
    }).collect().toSeq
  }

  def main(args: Array[String]): Unit = {

    val arguments = new Args(args)

    val experiment = arguments.getExperiment()

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("Clustering")
    val sc = new SparkContext(sparkConf)

    val parallelism = arguments.parallelism.getOrElse(sc.defaultParallelism)

    val vecs = VectorIO.readKryo(sc, arguments.input())//.cache()
//    val numVecs = vecs.count()
//    println(s"Input partitioning of outliers ${countOutliers(vecs)}")

    val dist: (Vector, Vector) => Double = {case (a, b) => math.sqrt(Vectors.sqdist(a, b))}

    val (coreset, coresetTime) = arguments.coreset() match {
      case "mapreduce-shuffle" =>
        experiment.tag("parallelism", parallelism)
        val coresetSize: Int = math.ceil(arguments.sizeFactor() * (arguments.k() + (arguments.zFactor() * arguments.z.getOrElse(0) / parallelism))).toInt
        println(s"Computing coreset of size $coresetSize")
        timed {
          val shuffled = vecs.keyBy(_ => Random.nextInt()).repartition(parallelism).values.persist(StorageLevel.MEMORY_ONLY)
          Algorithm.mapReduce(shuffled, coresetSize, dist)
        }
      case "mapreduce" =>
        experiment.tag("parallelism", parallelism)
        val coresetSize: Int = arguments.smallCoreset() match {
          case "yes" => math.ceil(arguments.sizeFactor() * (arguments.k() + (arguments.zFactor() * arguments.z.getOrElse(0) / parallelism))).toInt
          case "no" => math.ceil(arguments.sizeFactor() * (arguments.k() + arguments.z.getOrElse(0))).toInt
        }
        val repartitioned = if (vecs.getNumPartitions != parallelism)  {
          vecs.repartition(parallelism).persist(StorageLevel.MEMORY_ONLY)
        } else {
          vecs.persist(StorageLevel.MEMORY_ONLY)
        }
        val cnt = repartitioned.count()
//        println(countOutliers(repartitioned))
        println(s"Repartitioned the $cnt vectors")
        timed {
          Algorithm.mapReduce(repartitioned, coresetSize, dist)
        }
      case "streaming" =>
        experiment.tag("parallelism", 1)
        val coresetSize: Int = math.ceil(arguments.sizeFactor() * (arguments.k() + arguments.z.getOrElse(0))).toInt
        val localVectors = vecs // Randomly partition the vectors
          .keyBy(_ => Random.nextLong())
          .repartition(vecs.getNumPartitions)
          .values
          .collect()
        val result@(c, t) = timed {
          Algorithm.streaming(localVectors.iterator, coresetSize, dist)
        }
        println("Fixing radii")
        c.fixRadii(localVectors.iterator)
        appendTimers("streaming-profiling", experiment, c.metricRegistry)
        result
      case "random" =>
        experiment.tag("parallelism", parallelism)
        val coresetSize: Int = math.ceil(arguments.sizeFactor() * (arguments.k() + arguments.z.getOrElse(0))).toInt
        val repartitioned = vecs.repartition(parallelism).cache()
        repartitioned.count()
        println("Repartitioned the vectors")
        timed {
          Algorithm.randomCoreset(repartitioned, coresetSize, dist)
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
          val cacheSize = if (neededBytes < freeBytes()) {
            println(s"Needed ${formatBytes(neededBytes)} with ${formatBytes(freeBytes())} free, using local implementation")
            None
          } else {
            println(s"Matrix would require ${formatBytes(neededBytes)}, using partially cached implementation")
            // use half the free bytes for the cache
            val cs = math.ceil(math.sqrt(freeBytes()/2) /4).toInt
            Some(cs)
          }
          Outliers.run(coreset.points, arguments.k(), z, dist, cacheSize)._1
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
