package it.unipd.dei.clustering

import scala.collection.mutable

object Outliers {

  def run[T](points: IndexedSeq[T], k: Int, r: Double, distances: Array[Array[Double]])
  : (IndexedSeq[T], Int) = {
    val n = points.size
    val centers = new mutable.ArrayBuffer[T]()

    val covered = Array.fill[Boolean](n)(false)

    for (iteration <- 0 until n) {
      // Find the disk covering the most points
      val center = (0 until n).map({ idx =>
        val nCov = distances(idx).zipWithIndex.count({case (d, i) => !covered(i) && d <= r})
        (nCov, idx)
      }).maxBy(_._1)._2

      centers.append(points(center))

      // Mark points in the large disk as covered
      for (j <- 0 until n) {
        if (!covered(j)) {
          covered(j) = distances(center)(j) <= 3*r
        }
      }
    }

    (centers.toArray, covered.count(!_))
  }

  def run[T](points: IndexedSeq[T], k: Int, z: Int, distance: (T, T) => Double): IndexedSeq[T] = {
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
    var upper = candidates.length
    var lower = 0
    var sol: IndexedSeq[T] = Array.empty[T]
    var unc: Int = 0

    while (lower <= upper) {
      val mid: Int = (lower + upper) / 2
      val (tmpSol, tmpUnc) = run(points, k, candidates(lower), distances)
      sol = tmpSol
      unc = tmpUnc
      if (unc > z) {
        lower = mid
      } else {
        upper = mid
      }
    }

    sol
  }

}
