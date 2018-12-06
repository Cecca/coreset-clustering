package it.unipd.dei.clustering

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class StreamingOutliersClustering[T <: AnyRef : ClassTag](val k: Int,
                                                          val z: Int,
                                                          val initialScalingFactor: Double,
                                                          val distance: (T, T) => Double) {
  import StreamingOutliersClustering._

  val alpha = 4
  val beta = 8
  val nu = 16
  val batchSize: Int = z*k

  var _initializing: Boolean = true
  var _currentBatchCnt = 0

  var _r: Double = 0.0

  val _initPeeker: ArrayBuffer[T] = {
    val ab = new ArrayBuffer[T]()
    ab.sizeHint(k+z+1)
    ab
  }

  val _freePoints: ArrayBuffer[T] = {
    val ab = new ArrayBuffer[T]()
    ab.sizeHint(batchSize + z)
    ab
  }

  val _centers: ArrayBuffer[T] = {
    val ab = new ArrayBuffer[T]()
    ab.sizeHint(k)
    ab
  }
  def numCenters: Int = _centers.size

  val _support: ArrayBuffer[Array[T]] = {
    val ab = new ArrayBuffer[Array[T]]()
    ab.sizeHint(k)
    ab
  }

  // True if centers i and j are conflicting
  def areConflicting(i: Int, j: Int): Boolean = {
    var si = 0
    while (si < _support(i).length) {
      var sj = 0
      while (sj < _support(j).length) {
        if (distance(_support(i)(si), _support(j)(sj)) <= 2*alpha*_r) {
          return true
        }
        sj += 1
      }
      si += 1
    }
    false
  }

  private def remove[A](i: Int, buffer: ArrayBuffer[A]) = {
    buffer(i) = buffer.last
    buffer.reduceToSize(buffer.size-1)
  }

  private def remove[A, B](i: Int, bufferA: ArrayBuffer[A], bufferB: ArrayBuffer[B]): Unit = {
    remove(i, bufferA)
    remove(i, bufferB)
  }

  def closestCenterDistance(p: T): Double = {
    var d = Double.PositiveInfinity
    var i = 0
    while (i < numCenters) {
      d = math.min(d, distance(p, _centers(i)))
      i += 1
    }
    d
  }

  private def removePointsCoveredBy(i: Int): Unit = {
    val c = _centers(i)
    var j=0
    while (j < _freePoints.size) {
      if (distance(c, _freePoints(j)) <= nu*_r) {
        // We do not advance j in this branch, because the remove method that we call swaps the j-th and last
        // elements, so at index j we now have an unseen element. We advance toward the end nonetheless
        // because the size of the current batch shrinks by one
        remove(j, _freePoints)
      } else {
        j += 1
      }
    }
  }

  // Returns true if a center is indeed added
  private def addCentersWithSupport(): Boolean = {
    val b = new mutable.ArrayBuilder.ofRef[T]()
    b.sizeHint(z+1)
    var i = 0
    while (i < _freePoints.size) {
      b.clear()
      val candidate = _freePoints(i)
      var supportCnt = 0
      var j = 0
      while (j < _freePoints.size) {
        if (distance(candidate, _freePoints(j)) < beta*_r) {
          b += _freePoints(j)
          supportCnt += 1
        }
        if (supportCnt == z+1) {
          // We have found a center
          _centers.append(candidate)
          _support.append(b.result())
          // Remove points covered by the new center.
          removePointsCoveredBy(numCenters-1)
          return true
        }
        j += 1
      }

      i += 1
    }
    false
  }

  def restructure(): Unit = {
    var i = 0
    while (i < numCenters) {
      var j = i+1
      while (j < numCenters) {
        if (areConflicting(i, j)) {
          remove(j, _centers, _support)
          // We don't update j here because the removal operation brought an unseen
          // center in poisition j, so we want to evaluate it
        } else {
          j += 1
        }
      }
      i += 1
    }
  }

  def processFreePoints(): Unit = {
    // Step 1. Remove points covered by current clusters
    _centers.indices.foreach(removePointsCoveredBy)
    // Step 2. Find new centers with support
    while (addCentersWithSupport()) {}
    // Step 3. Check if the current centers are OK.
    if (numCenters <= k &&
        _freePoints.size <= (k-numCenters)*z + z &&
        outliers(_freePoints, k - numCenters, _r*nu, distance) <= z) {
      // Step 4. Restructure the clusters
      _r = _r * alpha
      restructure()
      // Repeat from step one
      processFreePoints()
    }
  }

  def update(point: T): Unit = {
    if (_initializing) {
      _initPeeker.append(point)
      if (_initPeeker.size == (k + z + 1)) {
        println("Initializing the initial guess for the radius")
        _initializing = false
        _r = initialScalingFactor * _initPeeker.iterator.flatMap({ x =>
          _initPeeker.iterator.map({ y =>
            distance(x,y)
          })
        }).min / 2.0
        // Replay the first points, now using them not for initialization
        println("Replaying the first points")
        _initPeeker.foreach(update)
      }
    }

    if (_currentBatchCnt < batchSize) {
      _freePoints.append(point)
      _currentBatchCnt += 1
    } else {
      processFreePoints()
      _currentBatchCnt = 0
    }
  }

}

object StreamingOutliersClustering {

  // A specialized version of the outliers algorithm that does not deal with the complication of proxy points.
  // This implements the original algorithm by Charikar et al.
  def outliers[T](points: IndexedSeq[T], k: Int, r: Double, distance: (T, T) => Double): Int = {
    val n = points.size

    val distances = Array.tabulate[Double](n, n)({case (i,j) =>
      distance(points(i), points(j))
    })


    val centers = new mutable.ArrayBuffer[T]()

    val weights: Array[Long] = Array.tabulate(n)({i =>
      var j = 0
      var w = 0l
      while (j < n) {
        if (distances(i)(j) <= r) {
          w += 1
        }
        j += 1
      }
      w
    })

    var iteration = 0
    while (iteration < k && weights.sum > 0) {

      // Find the disk covering the most weight
      val center = weights.par.zipWithIndex.maxBy(_._1)._2
      centers.append(points(center))

       // Mark points in the large disk as covered, that is, zero their weight if they are within the large disk.
      distances(center).par.zipWithIndex.foreach { case (d, j) =>
        if (d <= 3*r) {
          weights(j) = 0
        }
      }

      iteration += 1
    }

    // Points with non-zero weight are outliers, since they are not covered by any cluster
    val outliers = weights.count(_ > 0)
    outliers
  }

}