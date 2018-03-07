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

import scala.collection.mutable
import scala.reflect.ClassTag

class MapReduceCoreset[T:ClassTag](val points: Vector[ProxyPoint[T]],
                                   val radius: Double)
extends Coreset[T] with Serializable {

}

object MapReduceCoreset {

  def compose[T:ClassTag](a: MapReduceCoreset[T], b: MapReduceCoreset[T]): MapReduceCoreset[T] =
    new MapReduceCoreset(
      a.points ++ b.points,
      math.max(a.radius, b.radius))

  def run[T:ClassTag](points: Array[T],
                      kernelSize: Int,
                      distance: (T, T) => Double): MapReduceCoreset[T] = {
    if (points.length < kernelSize) {
      new MapReduceCoreset(points.map(ProxyPoint.fromPoint).toVector, 0.0)
    } else {
      val kernel = GMM.run(points, kernelSize, distance)
      // TODO: Use integers as keys
      val counts = mutable.HashMap[T, Long]()
      val radii = mutable.HashMap[T, Double]()

      var radius = 0.0

      var pointIdx = 0
      while (pointIdx < points.length) {
        // Find the closest center
        var centerIdx = 0
        var minDist = Double.PositiveInfinity
        var minIdx = -1
        while (centerIdx < kernel.length) {
          val dist = distance(points(pointIdx), kernel(centerIdx))
          if (dist < minDist) {
            minDist = dist
            minIdx = centerIdx
          }
          centerIdx += 1
        }
        radius = math.max(radius, minDist)
        assert(minDist <= Utils.minDistance(kernel, distance),
          s"Distance: $minDist, farness: ${Utils.minDistance(kernel, distance)}")
        counts.put(kernel(minIdx), counts.getOrElse(kernel(minIdx), 0L) + 1L)
        radii.put(kernel(minIdx), math.max(radii.getOrElse(kernel(minIdx), 0.0), minDist))
        pointIdx += 1
      }
      new MapReduceCoreset(
        counts.keys.map { center =>
          ProxyPoint(center, counts(center), radii(center))
        }.toVector,
        radius)
    }
  }

}
