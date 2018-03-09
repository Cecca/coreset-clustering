package it.unipd.dei.clustering

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Algorithm {

  def radius[T:ClassTag](allPoints: RDD[T],
                         centers: IndexedSeq[ProxyPoint[T]],
                         z: Int,
                         distance: (T, T) => Double): Double = {
    val bCenters = allPoints.context.broadcast(centers)

    val distances = allPoints.map({ p =>
      bCenters.value.map {c => distance(c.point, p)}.min
    })

    if (z == 0) {
      distances.max()
    } else {
      distances.top(z+1).last
    }
  }

  def mapReduce[T:ClassTag](rdd: RDD[T], tau: Int, distance: (T, T) => Double): Coreset[T] = {
    rdd.glom().map { points =>
      MapReduceCoreset.run(points, tau, distance)
    }.reduce { case (a, b) =>
      MapReduceCoreset.compose(a, b)
    }
  }

  def streaming[T:ClassTag](stream: Iterator[T], tau: Int, distance: (T, T) => Double): Coreset[T] = {
    val coreset = new StreamingCoreset[T](tau, distance)
    while(stream.hasNext) {
      coreset.update(stream.next())
    }
    coreset
  }

}
