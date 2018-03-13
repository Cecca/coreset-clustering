package it.unipd.dei.clustering

import it.unipd.dei.clustering.Debug.DEBUG
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bouncycastle.crypto.prng.drbg.DualECPoints

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Outliers {

  // Takes as a parameter proxyRadius in order not to recompute it every time.
  def run[T](points: IndexedSeq[ProxyPoint[T]], k: Int, r: Double, proxyRadius: Double, distances: Array[Array[Double]])
  : (IndexedSeq[ProxyPoint[T]], IndexedSeq[ProxyPoint[T]]) = {
    val n = points.size
    val centers = new mutable.ArrayBuffer[ProxyPoint[T]]()

    val covered = Array.fill[Boolean](n)(false)

    var iteration = 0
    while (iteration < k && covered.count(!_) > 0) {

      // TODO: Use par
      // Find the disk covering the most weight
      val center = (0 until n).par.map({ idx =>
        var nCov = 0L
        var j = 0
        while (j < n) {
          if (!covered(j) && distances(idx)(j) <= r + 2*proxyRadius) {
            nCov += points(j).weight
          }
          j += 1
        }
        (nCov, idx)
      }).maxBy(_._1)._2

      centers.append(points(center))

      // Mark points in the large disk as covered
      for (j <- 0 until n) {
        if (!covered(j)) {
          covered(j) = distances(center)(j) <= 3*r + 4*proxyRadius
        }
      }
//      DEBUG(s"Selected $center as center | " +
//        s"now the uncovered weight is ${points.zip(covered).filter(!_._2).map(_._1.weight).sum} (${covered.count(!_)} proxies)")
      iteration += 1
    }


    val outliers = points.zip(covered).filter(!_._2).map(_._1)

    (centers.toVector, outliers)
  }

  def runMat[T](points: IndexedSeq[ProxyPoint[T]], k: Int, r: Double, proxyRadius: Double, distances: DistanceMatrix, bWeights: Broadcast[Array[Long]])
  : (IndexedSeq[ProxyPoint[T]], IndexedSeq[ProxyPoint[T]]) = {
    val n = points.size
    val centers = new mutable.ArrayBuffer[ProxyPoint[T]]()

    val weights: Array[Long] = distances.ballWeight(bWeights, r + 2*proxyRadius)

    var iteration = 0
    while (iteration < k && weights.sum > 0) {

      // Find the disk covering the most weight
      val center = weights.par.zipWithIndex.maxBy(_._1)._2
      centers.append(points(center))

      // Mark points in the large disk as covered, that is, zero their weight if they are within the large disk.
      distances.row(center).par.zipWithIndex.foreach { case (d, j) =>
        if (d <= 3*r + 4*proxyRadius) {
          weights(j) = 0
        }
      }

      iteration += 1
    }


    val outliers = points.zip(weights).filter(_._2 > 0).map(_._1)

    (centers.toVector, outliers)
  }


  def run[T](points: IndexedSeq[ProxyPoint[T]], k: Int, z: Int, distance: (T, T) => Double)
  : (IndexedSeq[ProxyPoint[T]], IndexedSeq[ProxyPoint[T]]) = {
    val n = points.size

    val proxyRadius = points.iterator.map(_.radius).max
    DEBUG(s"The proxies radius is $proxyRadius")

    val distances = Array.ofDim[Double](n, n)
    (0 until n).par.foreach { i =>
      for (j <- (i+1) until n) {
        val d = distance(points(i).point, points(j).point)
        distances(i)(j) = d
        distances(j)(i) = d
      }
    }
    val candidates = distances.par.flatMap(_.iterator).toSet.toArray

    var sol: IndexedSeq[ProxyPoint[T]] = Vector.empty[ProxyPoint[T]]
    var outliers: IndexedSeq[ProxyPoint[T]] = points

    // Do a binary search to find the right value
    var upper = candidates.length - 1
    var lower = 0

    DEBUG("============================================")
    DEBUG(s"Lower ${candidates(lower)} upper ${candidates(upper)} ($upper candidates)")

    while (lower < upper-1) {
      val mid: Int = (lower + upper) / 2
      DEBUG(s"Testing ${candidates(mid)} (lower $lower current $mid upper $upper)")
      val (tmpSol, tmpOutliers) = run(points, k, candidates(mid), proxyRadius, distances)
      sol = tmpSol
      outliers = tmpOutliers
      DEBUG(s"Outliers ${outliers.size} (max $z)")
      if (outliers.size > z) {
        DEBUG("Too many outliers, raising the lower bound")
        lower = mid
      } else {
        DEBUG("Too few outliers, lowering upper bound")
        upper = mid
      }
    }

    val outliersSet = outliers.toSet
    val actualRadius = points.iterator.filterNot(outliersSet.contains).map({ p => {
      sol.iterator.map({c => distance(c.point, p.point)}).min
    }}).max

    DEBUG(s"Radius of clustering on proxy set (excluding outliers): $actualRadius")

    (sol, outliers)
  }

  def run[T](sc: SparkContext, points: IndexedSeq[ProxyPoint[T]], k: Int, z: Int, distance: (T, T) => Double)
  : (IndexedSeq[ProxyPoint[T]], IndexedSeq[ProxyPoint[T]]) = {
    val n = points.size

    val bWeights = sc.broadcast(points.map(_.weight).toArray)

    val proxyRadius = points.iterator.map(_.radius).max
    DEBUG(s"The proxies radius is $proxyRadius")

    val distances = DistanceMatrix(sc, points, distance)
    val candidates = distances.allDistances()
    DEBUG("Built candidates array")

    var sol: IndexedSeq[ProxyPoint[T]] = Vector.empty[ProxyPoint[T]]
    var outliers: IndexedSeq[ProxyPoint[T]] = points

    // Do a binary search to find the right value
    var upper = candidates.size - 1
    var lower = 0L

    DEBUG("============================================")
    DEBUG(s"Lower ${candidates(lower)} upper ${candidates(upper)} ($upper candidates)")

    while (lower < upper-1) {
      val mid: Long = (lower + upper) / 2
      DEBUG(s"Testing ${candidates(mid)} (lower $lower current $mid upper $upper)")
      val (tmpSol, tmpOutliers) = runMat(points, k, candidates(mid), proxyRadius, distances, bWeights)
      sol = tmpSol
      outliers = tmpOutliers
      DEBUG(s"Outliers ${outliers.size} (max $z)")
      if (outliers.size > z) {
        DEBUG("Too many outliers, raising the lower bound")
        lower = mid
      } else {
        DEBUG("Too few outliers, lowering upper bound")
        upper = mid
      }
    }

    val outliersSet = outliers.toSet
    val actualRadius = points.iterator.filterNot(outliersSet.contains).map({ p => {
      sol.iterator.map({c => distance(c.point, p.point)}).min
    }}).max

    DEBUG(s"Radius of clustering on proxy set (excluding outliers): $actualRadius")

    (sol, outliers)
  }


}

class DistanceMatrix(data: RDD[Array[Double]]) {

  def ballWeight[T](bWeights: Broadcast[Array[Long]], radius: Double): Array[Long] = {
    data.map { row =>
      var sum = 0L
      var i = 0
      while (i < row.length){
        if (row(i) <= radius) {
          sum += bWeights.value(i)
        }
        i += 1
      }
      sum
    }.collect()
  }

  def row(i: Int): Array[Double] = data.zipWithIndex().filter(_._2 == i).collect()(0)._1

  def allDistances(): SortedDistanceVector = {
    val dists = data.flatMap(_.iterator)
    SortedDistanceVector(dists)
  }

}

object DistanceMatrix {

  def apply[T](sc: SparkContext, points: IndexedSeq[ProxyPoint[T]], distance: (T, T) => Double): DistanceMatrix = {
    DEBUG("Building distance matrix")
    val bPoints = sc.broadcast(points.map(_.point))
    val data = sc.parallelize(points.indices).map { i =>
      val p = bPoints.value(i)
      bPoints.value.map(x => distance(x, p)).toArray
    }.cache()
    new DistanceMatrix(data)
  }

}

class SortedDistanceVector(val data: RDD[(Double, Long)], val size: Long) {

  def apply(i: Long): Double = data.filter(_._2 == i).collect()(0)._1

}

object SortedDistanceVector {

  def apply(dists: RDD[Double]): SortedDistanceVector = {
    val sorted = dists.sortBy(identity).zipWithIndex().cache()
    new SortedDistanceVector(sorted, sorted.count())
  }

}