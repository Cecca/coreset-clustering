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

import it.unipd.dei.clustering.Debug.DEBUG

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

object GMM {

  def run[T: ClassTag](points: IndexedSeq[T],
                       k: Int,
                       distance: (T, T) => Double): IndexedSeq[T] =
    run(points, k, Random.nextInt(points.length), distance)

  def runParallel[T: ClassTag](points: IndexedSeq[T],
                               k: Int,
                               distance: (T, T) => Double): IndexedSeq[T] =
    runParallel(points, k, Random.nextInt(points.length), distance)


  def run[T: ClassTag](points: IndexedSeq[T],
                       k: Int,
                       start: T,
                       distance: (T, T) => Double): IndexedSeq[T] = {
    val idx = points.indexOf(start)
    require(idx > 0, "The starting point should be in the collection!")
    run(points, k, idx, distance)
  }

  def run[T: ClassTag](points: IndexedSeq[T],
                       k: Int,
                       startIdx: Int,
                       distance: (T, T) => Double): IndexedSeq[T] = {
    if (points.length <= k) {
      points
    } else {
      DEBUG(s"Clustering ${points.size} points with $k centers")
      val minDist = Array.fill(points.size)(Double.PositiveInfinity)
      val result = Array.ofDim[T](k)
      // Init the result with an arbitrary point
      result(0) = points(startIdx)
      var i = 1
      while (i < k) {
        var farthest = points(0)
        var maxDist = 0.0

        var h = 0
        while (h < points.length) {
          // Look for the farthest node
          val lastDist = distance(points(h), result(i-1))
          if (lastDist < minDist(h)) {
            minDist(h) = lastDist
          }
          if (minDist(h) > maxDist) {
            maxDist = minDist(h)
            farthest = points(h)
          }
          h += 1
        }
        result(i) = farthest
        i += 1
      }
      result
    }
  }

  def runParallel[T: ClassTag](points: IndexedSeq[T],
                               k: Int,
                               startIdx: Int,
                               distance: (T, T) => Double): IndexedSeq[T] = {
    if (points.length <= k) {
      points
    } else {
      val minDist = Array.fill(points.size)(Double.PositiveInfinity)
      val result = Array.ofDim[T](k)
      // Init the result with an arbitrary point
      result(0) = points(startIdx)
      var i = 1
      while (i < k) {
        // update distances in parallel, in place
        points.indices.par.foreach { h =>
          val lastDist = distance(points(h), result(i-1))
          if (lastDist < minDist(h)) {
            minDist(h) = lastDist
          }
        }
        // Find the farthest node, again in parallel
//        val farthestIdx = minDist.view.par.zipWithIndex.maxBy(_._1)._2
        var farthestIdx = 0
        var maxDist = 0.0
        var h = 0
        while (h < points.length) {
          if (minDist(h) > maxDist) {
            maxDist = minDist(h)
            farthestIdx = h
          }
          h += 1
        }
        result(i) = points(farthestIdx)
        i += 1
      }
      result
    }
  }

  def runWithAssignement[T: ClassTag](points: IndexedSeq[T],
                                      k: Int,
                                      distance: (T, T) => Double): (Array[Int], Array[Double]) =
    runWithAssignement(points, k, Random.nextInt(points.length), distance)

  def runWithAssignement[T: ClassTag](points: IndexedSeq[T],
                                      k: Int,
                                      startIdx: Int,
                                      distance: (T, T) => Double): (Array[Int], Array[Double]) = {
    if (points.length <= k) {
      (points.indices.toArray, Array.ofDim[Double](points.size))
    } else {
      DEBUG(s"Clustering ${points.size} points with $k centers (with assignement)")
      val minDist = Array.fill(points.size)(Double.PositiveInfinity)
      val assignement = Array.fill(points.size)(0)

      var currentCenterIdx = startIdx

      // We start from 0 here because assignement is done within the loop
      var i = 0
      while (i < k) {
        val currentCenter = points(currentCenterIdx)
        // update distances in parallel, in place
        points.indices.par.foreach { h =>
          val lastDist = distance(points(h), currentCenter)
          if (lastDist < minDist(h)) {
            minDist(h) = lastDist
            assignement(h) = currentCenterIdx
          }
        }

        var farthestIdx = 0
        var maxDist = 0.0

        var h = 0
        while (h < points.length) {
          if (minDist(h) > maxDist) {
            maxDist = minDist(h)
            farthestIdx = h
          }
          h += 1
        }
        currentCenterIdx = farthestIdx
        i += 1
      }

      (assignement, minDist)
    }
  }


  def withRadius[T: ClassTag](points: IndexedSeq[T],
                              radius: Double,
                              distance: (T, T) => Double): IndexedSeq[T] =
    withRadius(points, radius, Random.nextInt(points.length), distance)


  def withRadius[T: ClassTag](points: IndexedSeq[T],
                              targetRadius: Double,
                              startIdx: Int,
                              distance: (T, T) => Double): IndexedSeq[T] = {
    val n = points.size
    val minDist = Array.fill(n)(Double.PositiveInfinity)
    val centers = IndexedSubset.apply(points)
    // Init the result with an arbitrary point
    centers.add(startIdx)
    var i = 0
    var radius: Double = 0d
    var nextCenter = 0
    while (i < n) {
      val d = distance(points(startIdx), points(i))
      minDist(i) = d
      if (d > radius) {
        radius = d
        nextCenter = i
      }
      i += 1
    }

    while (radius > targetRadius && centers.size != n) {
      val center = nextCenter
      centers.add(center)
      radius = 0.0
      i = 0
      // Re-compute the radius and find the farthest node
      while (i < n) {
        val d = distance(points(center), points(i))
        if (d < minDist(i)) {
          minDist(i) = d
        }
        if (minDist(i) > radius) {
          radius = minDist(i)
          nextCenter = i
        }
        i += 1
      }
    }
    centers.toVector
  }

  /* Just for benchmarking purposes: this is a more idiomatic,
   * albeit slower, implementation of the algorithm
   */
  private[clustering]
  def runSlow[T: ClassTag](points: IndexedSeq[T],
                           k: Int,
                           startIdx: Int,
                           distance: (T, T) => Double): IndexedSeq[T] = {
    if (points.length <= k) {
      points
    } else {
      val result = Array.ofDim[T](k)
      // Init the result with an arbitrary point
      result(0) = points(startIdx)
      var i = 1
      while (i < k) {
        var farthest = points(0)
        var dist = 0.0

        var h = 0
        while (h < points.length) {
          var minDist = Double.PositiveInfinity
          var j = 0
          while (j<i) { 
            val d = distance(result(j), points(h))
            if (d < minDist){
              minDist = d
            }
            j += 1
          }
          if (minDist > dist) {
            dist = minDist
            farthest = points(h)
          }
          h += 1
        }
        result(i) = farthest
        i += 1
      }
      result
    }
  }

  /* Just for testing purposes: this is a more idiomatic,
   * albeit slower, implementation of the algorithm
   */
  private[clustering]
  def runIdiomatic[T: ClassTag](points: IndexedSeq[T],
                                k:Int,
                                distance: (T, T) => Double): IndexedSeq[T] = {
    if (points.length <= k) {
      points
    } else {
      // Initialize with the first point of the input
      val result = ArrayBuffer[T](points(0))
      while(result.size < k) {
        // For each point look for the closest center
        val (farthest, dist) = points.map { p =>
          val dist = result.map { kp =>
            distance(p, kp)
          }.min
          (p, dist)
        }.maxBy(_._2)
        result.append(farthest)
      }
      result.toArray[T]
    }
  }

}
