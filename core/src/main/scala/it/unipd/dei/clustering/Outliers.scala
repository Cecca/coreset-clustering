package it.unipd.dei.clustering

import scala.collection.mutable

object Outliers {

  def run[T](points: IndexedSeq[T], k: Int, r: Double, distances: Array[Array[Double]])
  : (IndexedSeq[T], IndexedSeq[T]) = {
    val n = points.size
    val centers = new mutable.ArrayBuffer[T]()

    val covered = Array.fill[Boolean](n)(false)

    var iteration = 0
    while (iteration < n && covered.count(!_) > 0) {

      // Find the disk covering the most points
      val center = (0 until n).filter(!covered(_)).map({ idx =>
        val nCov = distances(idx).zipWithIndex.count({case (d, i) => !covered(i) && d <= r})
        (nCov, idx)
      }).maxBy(_._1)._2
      println(s"selected $center as center")

      centers.append(points(center))

      // Mark points in the large disk as covered
      for (j <- 0 until n) {
        if (!covered(j)) {
          covered(j) = distances(center)(j) <= 3*r
        }
      }
      iteration += 1
    }

    val outliers = points.zip(covered).filter(_._2).map(_._1)

    (centers.toVector, outliers)
  }

  def run[T](points: IndexedSeq[T], k: Int, z: Int, distance: (T, T) => Double)
  : (IndexedSeq[T], IndexedSeq[T]) = {
    val n = points.size

    val candidatesSet = mutable.SortedSet[Double]()

    val distances = Array.ofDim[Double](n, n)
    for (i <- 0 until n) {
      for (j <- i until n) {
        val d = distance(points(i), points(j))
        candidatesSet += d
        distances(i)(j) = d
        distances(j)(i) = d
      }
    }
    val candidates = candidatesSet.toArray

    // Do a binary search to find the right value
    var upper = candidates.length - 1
    var lower = 0
    var sol: IndexedSeq[T] = Vector.empty[T]
    var outliers: IndexedSeq[T] = Vector.empty[T]

    println("============================================")
    println(s"Lower ${candidates(lower)} upper ${candidates(upper)} ($upper candidates)")

    while (lower < upper-1) {
      val mid: Int = (lower + upper) / 2
      println(s"Testing ${candidates(mid)} (lower $lower current $mid upper $upper)")
      val (tmpSol, tmpOutliers) = run(points, k, candidates(lower), distances)
      sol = tmpSol
      outliers = tmpOutliers
      if (outliers.size > z) {
        lower = mid
      } else {
        upper = mid
      }
    }

    (sol, outliers)
  }

}
