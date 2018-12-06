// diversity-maximization: Diversity maximization in Streaming and MapReduce
// Copyright (C) 2016  Matteo Ceccarello <ceccarel@dei.unipd.it>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package it.unipd.dei.clustering

import java.util
import java.util.ConcurrentModificationException
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import it.unipd.dei.clustering.Utils._
import it.unipd.dei.clustering.Debug.DEBUG

import scala.reflect.ClassTag

object StreamingCoreset {

  def closestPointIndex[T](point: T,
                           points: Array[T],
                           distance: (T, T) => Double,
                           from: Int,
                           until: Int): (Int, Double) = {
    require(from <= until)
    require(until < points.length)
    var minDist = Double.PositiveInfinity
    var minIdx = -1
    var idx = from
    while (idx < until) {
      val dist = distance(point, points(idx))
      if (dist < minDist) {
        minDist = dist
        minIdx = idx
      }
      idx += 1
    }
    (minIdx, minDist)
  }

  /**
    * Swaps the elements at the specified indexes in place
    */
  def swap[T](points: Array[T], i: Int, j: Int): Unit = {
    val tmp = points(i)
    points(i) = points(j)
    points(j) = tmp
  }

}

class StreamingCoreset[T: ClassTag](val kernelSize: Int,
                                    val initialScalingFactor: Double,
                                    val distance: (T, T) => Double)
extends Coreset[T] {
  def this(kernelSize: Int, distance: (T, T) => Double) = this(kernelSize, 1, distance)

  import StreamingCoreset._

  val metricRegistry = new MetricRegistry()
  val updatesTimer = metricRegistry.timer("update")
  val innerUpdateTimer = metricRegistry.timer("innerUpdate")
  val mergeTimer = metricRegistry.timer("merge")

  val reporter = ConsoleReporter.forRegistry(metricRegistry).build();
  reporter.start(5, TimeUnit.SECONDS)

  private def farnessInvariant: Boolean =
    (numKernelPoints == 1) || (minKernelDistance >= threshold)

  // TODO: Keep track of the radius
//  private def radiusInvariant: Boolean = delegatesRadius <= 2*threshold

  private def weightInvariant: Boolean = weight == _seen

  // When true, accept all the incoming points
  private var _initializing = true

  // Keeps track of the first available position for insertion
  private var _insertionIdx: Int = 0

  private var _threshold: Double = Double.PositiveInfinity

  // The number of times the coreset have been restructured by a merge operation
  private var _numRestructurings: Int = 0
  private var _seen: Long = 0L

  private val _kernel = Array.ofDim[T](kernelSize + 1)
  private val _weights = Array.ofDim[Long](_kernel.length)
  private val _radii = Array.ofDim[Double](_kernel.length)

  private[clustering]
  def initializing: Boolean = _initializing

  private[clustering]
  def threshold: Double = _threshold

  /**
    * The number of times the coreset have been restructured by a merge operation
    */
  private[clustering]
  def numRestructurings: Int = _numRestructurings

  private[clustering]
  def numKernelPoints: Int = _insertionIdx

//  /* Only for testing */
//  private[clustering]
//  def setKernelPoint(index: Int, point: T): Unit = {
//    _kernel(index) = point
//  }

  def radius: Double = _radii.max

  private[clustering]
  def addKernelPoint(point: T): Unit = {
    _kernel(_insertionIdx) = point
    _weights(_insertionIdx) = 1L
    _radii(_insertionIdx) = 0.0

    _insertionIdx += 1
  }

  def pointsIterator: Iterator[T] = kernelPointsIterator

  private[clustering]
  def kernelPointsIterator: Iterator[T] =
    new Iterator[T] {
      val maxIdx = numKernelPoints
      var itIdx = 0

      override def hasNext: Boolean = {
        if (numKernelPoints != maxIdx) {
          throw new ConcurrentModificationException()
        }
        itIdx < maxIdx
      }

      override def next(): T = {
        val elem = _kernel(itIdx)
        itIdx += 1
        elem
      }
    }

  // TODO: Avoid this copying
  private[clustering]
  def minKernelDistance: Double = minDistance(kernelPointsIterator.toArray[T], distance)

  private def closestKernelDistance(point: T): Double = {
    var m = Double.PositiveInfinity
    kernelPointsIterator.foreach { kp =>
      val d = distance(kp, point)
      if (d < m) {
        m = d
      }
    }
    m
  }

  private[clustering]
  def initializationStep(point: T): Unit = {
    require(_initializing)
    val (minIdx, minDist) = closestKernelPoint(point)
    if (_kernel(minIdx) == point) {
      DEBUG("Skipping duplicate point!")
      return
    }
    if (minDist < _threshold) {
      _threshold = minDist
    }
    require(_threshold > 0.0, "Threshold is zero!")
    addKernelPoint(point)
//    DEBUG(s"New center: $this")
    assert(weightInvariant, "Weight after new center")
    if (_insertionIdx == _kernel.length) {
      _initializing = false
      // The initialization phase is finished, and we found the values of the initial radius.
      // We scale this value by the initialScalingFactor, which is used for running multiple parallel
      // instances of this algorithm with different running guesses on the radius
      _threshold = _threshold*initialScalingFactor
    }
  }

  private val currentPointToKernelDist = Array.ofDim[Double](_kernel.length)

  private def closestKernelPoint(point: T): (Int, Double) = {
    var idx = 0
    var mDist = Double.PositiveInfinity
    var mIdx = 0
    val maxIdx = numKernelPoints
    while(idx < maxIdx) {
      val d = distance(_kernel(idx), point)
      currentPointToKernelDist(idx) = d
      if (d < mDist) {
        mDist = d
        mIdx = idx
      }
      idx += 1
    }
    (mIdx, mDist)
  }

  private[clustering]
  def updateStep(point: T): Boolean = {
    val t = innerUpdateTimer.time()
    require(!_initializing)
    // Find distance to the closest kernel point
    val (minIdx, minDist) = closestKernelPoint(point)
    val r = if (minDist > 2 * _threshold) {
      // Pick the point as a center
      addKernelPoint(point)
//      DEBUG(s"New center: $this")
      assert(weightInvariant, "Weight after new center")
      true
    } else {
      // Increment the weight of the center
      _weights(minIdx) += 1
      _radii(minIdx) = math.max(_radii(minIdx), minDist)
//      DEBUG(s"Discarded element: $this")
      assert(weightInvariant, "Weight after insertion")
      false
    }
    t.stop()
    r
  }

  private[clustering]
  def swapData(i: Int, j: Int): Unit = {
    swap(_kernel, i, j)
    swap(_weights, i, j)
    swap(_radii, i, j)
  }

  private[clustering]
  def resetData(from: Int): Unit = {
    var idx = from
    while (idx < _kernel.length) {
      _weights(idx) = 0
      _radii(idx) = 0.0
      idx += 1
    }
  }

  private[clustering]
  def merge(): Unit = {
    val t = mergeTimer.time()
    // Use the `kernel` array as if divided in 3 zones:
    //
    //  - selected: initially empty, stores all the selected nodes.
    //  - candidates: encompasses all the array at the beginning
    //  - discarded: initially empty, stores the points that have been merged
    //
    // The boundaries between these regions are, respectively, the
    // indexes `bottomIdx` and `topIdx`
    require(_insertionIdx == _kernel.length)
    _threshold *= 2
    _numRestructurings += 1
    DEBUG(s"Merge, new threshold: ${_threshold}")

//    DEBUG(s"Threshold ${_threshold}")
//    DEBUG(s"Before $this")
    var bottomIdx = 0
    var topIdx = _kernel.length - 1
    while (bottomIdx <= topIdx) {
      val pivotIdx = bottomIdx
//      val pivot = _kernel(bottomIdx)
      var candidateIdx = bottomIdx + 1
      // Discard the points that are too close to the pivot
      while (candidateIdx <= topIdx) {
//        DEBUG(s"bottom: $bottomIdx candidate: $candidateIdx, top: $topIdx")
        if (distance(_kernel(pivotIdx), _kernel(candidateIdx)) <= _threshold) {
          // Add all the weight of the to-be-discarded candidate to the pivot
          _weights(bottomIdx) += _weights(candidateIdx)
          // update the radius as the maximum between the two.
          // Here we are losing something because of the triangle inequality
          _radii(bottomIdx) = math.max(_radii(bottomIdx), _radii(candidateIdx))

          // Move the candidate (and all its data) to the end of the array
          swapData(candidateIdx, topIdx)
//          DEBUG(s"Discadring $candidateIdx")
          topIdx -= 1
        } else {
          // Keep the point in the candidate zone
//          DEBUG(s"Keeping $candidateIdx as candidate")
          candidateIdx += 1
        }
//        DEBUG(s"After $this")
      }
//       Move to the next point to be retained
      bottomIdx += 1
    }
    // Reset the data related to excluded points
    resetData(topIdx+1)
    // Set the new insertionIdx
    _insertionIdx = bottomIdx

//    DEBUG(s"After merge $this")

    // Check the invariant of the minimum distance between kernel points
    assert(farnessInvariant, "Farness after merge")
    // TODO: Check the invariant of radius
//    assert(radiusInvariant, "Radius after merge")
    assert(weightInvariant, s"Weight after merge: $weight when we have seen ${_seen} points")
    t.stop()
  }

  /**
    * This method implements the modified doubling algorithm.
    * Return true if the point is added to the inner core-set
    */
  def update(point: T): Boolean = {
//    DEBUG(s"Processing point $point")
    val t = updatesTimer.time()
    // the _insertionIdx variable is modified inside the merge() method
    while (_insertionIdx == _kernel.length) {
      merge()
    }

    // This counter is updated here because the merge rule that might be applied before
    // refers to the old value, it does not take into account the current point.
    _seen += 1

    val res = if (_initializing) {
      initializationStep(point)
      true
    } else {
      updateStep(point)
    }
    t.stop()
    res
  }

  override def points: Vector[ProxyPoint[T]] =
    kernelPointsIterator.zip(_weights.iterator).zip(_radii.iterator).map { case ((center, weight), radius) =>
      ProxyPoint[T](center, weight, radius)
    }.toVector

  def fixRadii(points: Iterator[T]) = {
    util.Arrays.fill(_radii, 0.0)
    while (points.hasNext) {
      val p = points.next()
      val (minIdx, minDist) = closestKernelPoint(p)
      _radii(minIdx) = math.max(_radii(minIdx), minDist)
    }
  }

}
