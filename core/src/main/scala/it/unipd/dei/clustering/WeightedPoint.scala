package it.unipd.dei.clustering

case class WeightedPoint[T](point: T, weight: Long)

object WeightedPoint {
  def fromTuple[T](t: (T, Long)): WeightedPoint[T] = WeightedPoint(t._1, t._2)
}

