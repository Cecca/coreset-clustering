package it.unipd.dei.clustering

import it.unipd.dei.clustering.Debug.DEBUG

import scala.collection.mutable

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
      val center = (0 until n).map({ idx =>
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
      DEBUG(s"Selected $center as center | " +
        s"now the uncovered weight is ${points.zip(covered).filter(!_._2).map(_._1.weight).sum} (${covered.count(!_)} proxies)")
      iteration += 1
    }


    val outliers = points.zip(covered).filter(!_._2).map(_._1)

    (centers.toVector, outliers)
  }

  def run[T](points: IndexedSeq[ProxyPoint[T]], k: Int, z: Int, distance: (T, T) => Double)
  : (IndexedSeq[ProxyPoint[T]], IndexedSeq[ProxyPoint[T]], Double) = {
    val n = points.size

    val proxyRadius = points.iterator.map(_.radius).max
    DEBUG(s"The proxies radius is $proxyRadius")

    val candidatesSet = mutable.SortedSet[Double]()

    val distances = Array.ofDim[Double](n, n)
    for (i <- 0 until n) {
      for (j <- i until n) {
        val d = distance(points(i).point, points(j).point)
        candidatesSet += d
        distances(i)(j) = d
        distances(j)(i) = d
      }
    }
    val candidates = candidatesSet.toArray

    var sol: IndexedSeq[ProxyPoint[T]] = Vector.empty[ProxyPoint[T]]
    var outliers: IndexedSeq[ProxyPoint[T]] = points

    var i = 0
    while (outliers.map(_.weight).sum > z) {
      val (tmpSol, tmpOutliers) = run(points, k, candidates(i), proxyRadius, distances)
      sol = tmpSol
      outliers = tmpOutliers
      DEBUG(s"Candidate $i : ${candidates(i)} : outliers weight=${outliers.map(_.weight).sum} (${outliers.size} proxies)")
      i += 1
    }

    val outliersSet = outliers.toSet
    val actualRadius = points.iterator.filterNot(outliersSet.contains).map({ p => {
      sol.iterator.map({c => distance(c.point, p.point)}).min
    }}).max

    DEBUG(s"Radius of clustering on proxy set: $actualRadius")

//    // Do a binary search to find the right value
//    var upper = candidates.length - 1
//    var lower = 0
//
//    DEBUG("============================================")
//    DEBUG(s"Lower ${candidates(lower)} upper ${candidates(upper)} ($upper candidates)")
//
//    while (lower < upper-1) {
//      val mid: Int = (lower + upper) / 2
//      DEBUG(s"Testing ${candidates(mid)} (lower $lower current $mid upper $upper)")
//      val (tmpSol, tmpOutliers) = run(points, k, candidates(lower), distances)
//      sol = tmpSol
//      outliers = tmpOutliers
//      DEBUG(s"Outliers ${outliers.size} (max $z) : $outliers")
//      if (outliers.size > z) {
//        lower = mid
//      } else {
//        upper = mid
//      }
//    }

    (sol, outliers, actualRadius)
  }

}
