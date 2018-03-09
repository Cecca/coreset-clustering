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
    val distances = dCoreset.cartesian(dCoreset).map(DistanceWithError.fromTuple).cache()

    val maxDist = distances.max()(Ordering.by(d => d.dist))

    println("Computing histogram")
    val (bounds, counts) = distances.map(_.dist).histogram(arguments.buckets())
    println(s"Bounds (${bounds.length} elems): ${bounds.toSeq}")
    println(s"Counts (${counts.length} elems): ${counts.toSeq}")

    println("Fixing histogram")
    for (derr <- distances.toLocalIterator) {
      var i = 0
      while (i < counts.length && derr.dist <= bounds(i+1)) {
        if (derr.dist > bounds(i)) {
          counts(i) += derr.numPairs
        }
        i += 1
      }
    }

    println(s"Maximum distance is $maxDist\n")
    for (((lb, c), ub) <- bounds.zip(counts).zip(bounds.tail)) {
      println(s"$lb < d <= $ub :: $c")
    }
  }

}
