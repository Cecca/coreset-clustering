package it.unipd.dei.clustering

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object DistanceDistribution {

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input = opt[String](required = true)
    val output = opt[String](required = false, default = Some(input.toOption.get + ".distances"))
    val size = opt[Int](required = false)
    val buckets = opt[Int](default = Some(10))
    val weightBuckets = opt[Int](default = buckets.toOption)
    val lowPercentile = opt[Double](default = Some(0.01))
    val highPercentile = opt[Double](default = Some(0.99))
    verify()
  }

  def distance(a: Vector, b: Vector): Double = math.sqrt(Vectors.sqdist(a, b))

  case class DistanceWithError(dist: Double, numPairs: Long, maxError: Double)

  object DistanceWithError {
    def fromTuple(tup: (ProxyPoint[Vector], ProxyPoint[Vector])): DistanceWithError =
      tup match {
        case (a, b) => DistanceWithError(
          distance(a.point, b.point), a.weight*b.weight/2, a.radius + b.radius)
      }
  }

  def main(args: Array[String]): Unit = {
    val arguments = new Args(args)

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("Clustering")
    val sc = new SparkContext(sparkConf)

    val vecs: RDD[Vector] = VectorIO.readKryo(sc, arguments.input()).cache()
    val cnt = vecs.count()
    println(s"Loaded $cnt vectors")

    val sampleSize = arguments.size.getOrElse(cnt.toInt)
    val coresetSize = sampleSize / vecs.getNumPartitions
    println(s"Computing coresets of size $coresetSize")

    val coreset = vecs.glom
      .map(vecs => MapReduceCoreset.run(vecs, coresetSize, distance))
      .reduce({case (a, b) => MapReduceCoreset.compose(a, b)})
    println(s"Computed coreset")

    println(s"Computing distances between points of the coreset")
    val dCoreset = sc.parallelize(coreset.points)
      .sortBy(_.weight, ascending = true)
      .cache()
    val distances = dCoreset.cartesian(dCoreset).map(DistanceWithError.fromTuple).cache()
    println(s"Computed ${distances.count()} distances (sum of weight products = ${distances.map(_.numPairs).sum})")
    val maxDist = distances.max()(Ordering.by(d => d.dist))

    println("Computing histogram")
    val (bounds, counts) = distances.map(_.dist).histogram(arguments.buckets())

    println("Fixing histogram")
    for (i <- counts.indices) {
      counts(i) = 0
    }
    for (derr <- distances.toLocalIterator) {
      var stop = false
      var i = 0
      while (i < counts.length && !stop) {
        if (derr.dist >= bounds(i) && derr.dist < bounds(i+1)) {
          counts(i) += derr.numPairs
          stop = true
        }
        i += 1
      }
    }

    println(s"Maximum distance is $maxDist\n")
    printHistogram((bounds, counts))

    println("Weight distribution")
    println("===================")

    printHistogram(dCoreset.map(_.weight).histogram(arguments.weightBuckets()))

    val weights = dCoreset.map(_.weight.toDouble).collect().sorted

    println("Percentiles")
    for (p <- Seq(0, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0)) {
      printPercentile(weights, p)
    }


    println("\n\nProxies that cover few points")
    val lowPercIdx = arguments.lowPercentile() * dCoreset.count()

    val lowPercWeight = dCoreset
      .zipWithIndex()
      .filter({case (p, i) => i == lowPercIdx})
      .collect().head._1.weight

    val potentialOutliers = dCoreset
      .filter(p => p.weight <= lowPercWeight)
      .cache()

    println(s"Collected ${potentialOutliers.count()} proxies of potential outliers (weight <= $lowPercWeight)")

    val highPercIdx = arguments.highPercentile() * dCoreset.count()

    val highPercWeight = dCoreset
      .zipWithIndex()
      .filter({case (p, i) => i == highPercIdx})
      .collect().head._1.weight

    val heavyProxies = dCoreset
      .filter(p => p.weight >= highPercWeight)
      .cache()

    println(s"Collected ${heavyProxies.count()} heavy proxies (weight >= $highPercWeight)")

    val interestingHist = heavyProxies.cartesian(potentialOutliers).map { case (hp, o) =>
      distance(hp.point, o.point)
    }.histogram(arguments.buckets())

    printHistogram(interestingHist)

  }

  def printHistogram(tup: (Array[Double], Array[Long])): Unit = tup match {
    case (bounds, counts) =>
      for (((lb, c), ub) <- bounds.zip(counts).zip(bounds.tail)) {
        println(s"$lb <= x < $ub :: $c")
      }
  }

  def printPercentile(arr: Array[Double], perc: Double): Unit = {
    val idx = math.min(arr.length-1, (perc*arr.length).toInt)
    println(s"$perc :: ${arr(idx)}")
  }

}
