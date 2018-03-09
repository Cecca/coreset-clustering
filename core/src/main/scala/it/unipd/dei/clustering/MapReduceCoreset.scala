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
      val (assignement, distances) = GMM.runWithAssignement(points, kernelSize, distance)
      DEBUG("Computed kernel")
      DEBUG("Building result corest")
      val counts = Array.fill[Int](points.length)(0)
      val radii = Array.fill[Double](points.length)(0.0)
      var i = 0
      while(i < points.length) {
        counts(assignement(i)) += 1
        radii(assignement(i)) = math.max(radii(assignement(i)), distances(i))
        i += 1
      }
      val proxies = assignement.indices
        .filter(i => assignement(i) == i)
        .map { i =>
          ProxyPoint(points(i), counts(i), radii(i))
        }.toVector
      val radius = radii.max
      new MapReduceCoreset(proxies, radius)
    }
  }

}
