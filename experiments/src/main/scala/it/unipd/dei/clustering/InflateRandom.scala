package it.unipd.dei.clustering

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.util.Random

object InflateRandom {

  class Opts(args: Array[String]) extends ScallopConf(args) {

    val input = opt[String](required=true)
    val output = opt[String](required=true)
    val sizeFactor = opt[Int](required=true)
    val deviationFactor = opt[Double](required=false, default=Some(0.01))

    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("Clustering")
    val sc = new SparkContext(sparkConf)

    val original = VectorIO.readKryo(sc, opts.input())

    val mins = original.map(_.toArray).reduce({case (a1, a2) =>
      a1.zip(a2).map({case (x, y) => math.min(x, y)})
    })
    val maxs = original.map(_.toArray).reduce({case (a1, a2) =>
      a1.zip(a2).map({case (x, y) => math.max(x, y)})
    })

    val deviations = mins.zip(maxs).map({ case (x, y) =>
      opts.deviationFactor() * math.abs(x-y)
    })

    val sizeFactor = opts.sizeFactor()

    val inflated = original.glom().flatMap({ vecs =>
      (0 until (vecs.length * (sizeFactor-1))).iterator.map({ _ =>
        val data = vecs(Random.nextInt(vecs.length)).copy.toArray
        for (i <- deviations.indices) {
          data(i) += Random.nextGaussian() * deviations(i)
        }
        Vectors.dense(data)
      }) ++ vecs.iterator
    })

    VectorIO.writeKryo(inflated, opts.output())
  }

}
