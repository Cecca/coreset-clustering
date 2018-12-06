package it.unipd.dei.clustering

import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.util.Random

object InjectOutliers {

  class Opts(args: Array[String]) extends ScallopConf(args) {

    val input = opt[String](required=true)
    val output = opt[String](required=true)
    val outliers = opt[Int](required=true)
    val factor = opt[Double](required=true)
    val numPartitions = opt[Int](required=true)

    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("Clustering")
    val sc = new SparkContext(sparkConf)

    val numOutliers = opts.outliers()
    val factor = opts.factor()
    val numPartitions = opts.numPartitions()

    val original = VectorIO.readKryo(sc, opts.input()).cache()

    val minCoords: Vector = original.reduce({case (a, b) =>
      val m = a.toArray.zip(b.toArray).map({case (x: Double, y: Double) => math.min(x, y)})
      Vectors.dense(m)
    })

    val maxCoords: Vector = original.reduce({case (a, b) =>
      val m = a.toArray.zip(b.toArray).map({case (x: Double, y: Double) => math.max(x, y)})
      Vectors.dense(m)
    })

    val dimension = minCoords.size

    val centerData = Array.ofDim[Double](dimension)
    for (i <- centerData.indices) {
      centerData(i) = (maxCoords(i) + minCoords(i)) / 2.0
    }
    val center = Vectors.dense(centerData)
    val radius = math.sqrt(Vectors.sqdist(center, maxCoords))
    println(s"Max: $maxCoords\nMin: $minCoords\nCenter: $center\nRadius: $radius")

    val result = original.repartition(numPartitions).mapPartitionsWithIndex({ case (partIdx, elements) =>
      if (partIdx > 0)  {
        elements
      } else {
        val rnd = new Random()
        // Add outliers just to the first partition
        (0 until numOutliers).map({ _ =>
          val data = Array.ofDim[Double](dimension)
          for (i <- data.indices) {
            data(i) = math.abs(rnd.nextGaussian())
          }
          val norm = Vectors.norm(Vectors.dense(data), 2.0)
          for (i <- data.indices) {
            data(i) = data(i) / norm * radius * factor + center(i)
          }
          Vectors.dense(data)
        }).iterator ++ elements
      }
    }, preservesPartitioning = true).cache()

    VectorIO.writeKryo(result, opts.output())

    val vecsWithDists = result.glom().first().map({ v =>
      val d = math.sqrt(Vectors.sqdist(v, center))
      (d, v)
    }).sortBy(- _._1)

    vecsWithDists.take(numOutliers + 3).foreach({ case (d, v) =>
      println(s"dist: $d, v: $v")
    })

    val outliers = vecsWithDists.take(numOutliers)
    val distances: Seq[Double] = for {
      x <- outliers
      y <- outliers
    } yield math.sqrt(Vectors.sqdist(x._2, y._2))

    println(s"Minimum distance between outliers: ${distances.filter(_ > 0).min}")
    println(s"Average distance between outliers: ${distances.sum / distances.size}")

  }

}
