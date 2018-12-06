package it.unipd.dei.clustering

import scala.reflect.ClassTag

class ParallelStreamingClustering[T:ClassTag](val k: Int,
                                              val m: Int,
                                              val distance: (T, T) => Double) {

  val instances = {
    val _is = for (i <- 1 to m) yield new StreamingCoreset[T](k, math.pow(2, (i-1)/m), distance)
    _is.toArray
  }

  def update(point: T): Unit = {
    instances.foreach(_.update(point))
  }

  def radius(points: Iterable[T]): Double = {
    instances.par.map({ instance =>
      instance.fixRadii(points.iterator)
      instance.radius
    }).max
  }

}
