package it.unipd.dei.clustering

case class ProxyPoint[T](point: T, weight: Long, radius: Double)

object ProxyPoint {
  def fromTuple[T](t: (T, Long, Double)): ProxyPoint[T] = ProxyPoint(t._1, t._2, t._3)

  def fromPoint[T](p: T): ProxyPoint[T] = ProxyPoint(p, 1L, 0.0);
}

