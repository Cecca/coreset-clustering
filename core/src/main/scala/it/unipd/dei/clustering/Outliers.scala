package it.unipd.dei.clustering

import it.unipd.dei.clustering.Debug.DEBUG
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Outliers {

  // Takes as a parameter proxyRadius in order not to recompute it every time.
  @deprecated
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

  def runMat[T](points: IndexedSeq[ProxyPoint[T]], k: Int, r: Double, proxyRadius: Double, distances: DistanceMatrix)
  : (IndexedSeq[ProxyPoint[T]], IndexedSeq[ProxyPoint[T]]) = {
    val n = points.size
    val centers = new mutable.ArrayBuffer[ProxyPoint[T]]()

    val weights: Array[Long] = distances.ballWeight(r + 2*proxyRadius)

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

  @deprecated
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
//    val candidates = distances.par.flatMap(_.iterator).toSet.toArray
    val candidatesArr = Array.ofDim[Double](n*n)
    var i = 0
    for (row <- distances; d <- row) {
      candidatesArr(i) = d
      i += 1
    }
    val candidates = LocalSortedDistanceVector(candidatesArr)


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

  def run[T](points: IndexedSeq[ProxyPoint[T]], k: Int, z: Int, distance: (T, T) => Double, osc: Option[SparkContext])
  : (IndexedSeq[ProxyPoint[T]], IndexedSeq[ProxyPoint[T]]) = {
    val n = points.size
    println(s"Clustering $n points with $k centers and $z outliers")

    val proxyRadius = points.iterator.map(_.radius).max
    DEBUG(s"The proxies radius is $proxyRadius")

    val distances = osc match {
      case Some(sc) =>
        DistributedDistanceMatrix(sc, points, distance)
      case None =>
        LocalDistanceMatrix(points, distance)
    }
    DEBUG("Computed distance matrix")
    val candidates = distances.allDistances()
    DEBUG("Computed candidates distances")

    var sol: IndexedSeq[ProxyPoint[T]] = Vector.empty[ProxyPoint[T]]
    var outliers: IndexedSeq[ProxyPoint[T]] = points

    // Do a binary search to find the right value
    var upper = candidates.size - 1
    var lower = 0L

    DEBUG("============================================")
    DEBUG(s"Lower ${candidates(lower)} upper ${candidates(upper)} ($upper candidates)")

    var lastGood: Option[(IndexedSeq[ProxyPoint[T]], IndexedSeq[ProxyPoint[T]])] = None

    while (lower <= upper) {
      val mid: Long= (lower + upper) / 2
      DEBUG(s"Testing ${candidates(mid)} (lower $lower current $mid upper $upper)")
      val (tmpSol, tmpOutliers) = runMat(points, k, candidates(mid), proxyRadius, distances)
      sol = tmpSol
      outliers = tmpOutliers
      val outliersWeight = outliers.map(_.weight).sum
      DEBUG(s"Outliers weight $outliersWeight (max $z)")

      if (outliersWeight > z) {
        DEBUG("Too many outliers, raising the lower bound")
        lower = mid + 1
      } else {
        DEBUG("Too few outliers, lowering upper bound")
        lastGood = Some(sol, outliers)
        upper = mid - 1
      }
    }

    val outliersSet = outliers.toSet
    val actualRadius = points.iterator.filterNot(outliersSet.contains).map({ p => {
      sol.iterator.map({c => distance(c.point, p.point)}).min
    }}).max

    if (outliers.map(_.weight).sum > z) {
      lastGood match {
        case Some((s, o)) =>
          sol = s
          outliers = o
        case None =>
          throw new IllegalStateException(s"The outliers proxy cannot weight more than z=$z! (current weight is ${outliers.map(_.weight).sum})")
      }
    }

    DEBUG(s"Radius of clustering on proxy set (excluding outliers): $actualRadius")

    (sol, outliers)
  }


}

trait DistanceMatrix {
  def ballWeight[T](radius: Double): Array[Long]
  def row(i: Int): Array[Double]
  def allDistances(): SortedDistanceVector
}

trait SortedDistanceVector {
  def apply(i: Long): Double
  def size: Long
}

class DistributedDistanceMatrix(val data: RDD[Array[Double]], val bWeights: Broadcast[Array[Long]]) extends DistanceMatrix {

  def ballWeight[T](radius: Double): Array[Long] =
    DistributedDistanceMatrix.ballWeight(data, bWeights, radius)

  def row(i: Int): Array[Double] = data.zipWithIndex().filter(_._2 == i).collect()(0)._1

  def allDistances(): SortedDistanceVector = {
    val dists = data.flatMap(_.iterator)
    DistributedSortedDistanceVector(dists)
  }

}

object DistributedDistanceMatrix {

  def apply[T](sc: SparkContext, points: IndexedSeq[ProxyPoint[T]], distance: (T, T) => Double): DistributedDistanceMatrix = {
    DEBUG("Building distance matrix")
    val bWeights = sc.broadcast(points.map(_.weight).toArray)
    val bPoints = sc.broadcast(points.map(_.point))
    val data = sc.parallelize(points.indices, sc.defaultParallelism*4).map { i =>
      val p = bPoints.value(i)
      bPoints.value.map(x => distance(x, p)).toArray
    }.cache()
    new DistributedDistanceMatrix(data, bWeights)
  }

  def ballWeight[T](data: RDD[Array[Double]], bWeights: Broadcast[Array[Long]], radius: Double): Array[Long] = {
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


}

class LocalDistanceMatrix(val data: Array[Array[Double]], val weights: Array[Long]) extends DistanceMatrix {

  def ballWeight[T](radius: Double): Array[Long] = {
    data.map { row =>
      var sum = 0L
      var i = 0
      while (i < row.length){
        if (row(i) <= radius) {
          sum += weights(i)
        }
        i += 1
      }
      sum
    }
  }

  def row(i: Int): Array[Double] = data(i.toInt)

  def allDistances(): SortedDistanceVector = {
    val candidatesArr = Array.ofDim[Double](data.length*data.length)
    var i = 0
    for (row <- data; d <- row) {
      candidatesArr(i) = d
      i += 1
    }
    LocalSortedDistanceVector(candidatesArr)
  }
}

object LocalDistanceMatrix {
  def apply[T](points: IndexedSeq[ProxyPoint[T]], distance: (T, T) => Double): LocalDistanceMatrix = {
    val n = points.length
    val distances = Array.ofDim[Double](n, n)
    (0 until n).par.foreach { i =>
      for (j <- (i+1) until n) {
        val d = distance(points(i).point, points(j).point)
        distances(i)(j) = d
        distances(j)(i) = d
      }
    }
    val weights = points.map(_.weight).toArray
    new LocalDistanceMatrix(distances, weights)
  }
}

class DistributedSortedDistanceVector(val data: RDD[(Double, Long)], val size: Long) extends SortedDistanceVector {

  def apply(i: Long): Double = data.filter(_._2 == i).collect()(0)._1

}

object DistributedSortedDistanceVector {

  def apply(dists: RDD[Double]): DistributedSortedDistanceVector = {
    val sorted = dists.sortBy(identity).zipWithIndex().cache()
    new DistributedSortedDistanceVector(sorted, sorted.count())
  }

}

class LocalSortedDistanceVector(val data: Array[Double], val size: Long) extends SortedDistanceVector {

  def apply(i: Long): Double = data(i.toInt)

}

object LocalSortedDistanceVector {

  def apply(dists: Array[Double]): LocalSortedDistanceVector = {
    java.util.Arrays.sort(dists)
    // Remove possible duplicates
    var in_idx = 1
    var out_idx = 1
    var last = dists(0)
    while(in_idx < dists.length) {
      if (dists(in_idx) > last) {
        last = dists(in_idx)
        dists(out_idx) = last
        out_idx += 1
      } else if (dists(in_idx) < last) {
        throw new IllegalArgumentException("The array should be sorted!")
      }
      in_idx += 1
    }

    new LocalSortedDistanceVector(dists, out_idx)
  }

}