package it.unipd.dei.clustering

import it.unipd.dei.clustering.Debug.DEBUG
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Algorithm {
  
  def radius[T:ClassTag](allPoints: RDD[T],
                         centers: IndexedSeq[ProxyPoint[T]],
                         coveringRadius: Double,
                         distance: (T, T) => Double): Double = {
    val bCenters = allPoints.context.broadcast(centers)
    val result = allPoints.flatMap({p =>
      val minDist = bCenters.value.iterator.map({c => distance(c.point, p)}).min
      if (minDist > coveringRadius) {
        Iterator.empty // This is an outlier
      } else {
        Iterator.single(minDist)
      }
    }).max()

    val actualOutliers = allPoints.filter({ p=>
      bCenters.value.iterator.map({c => distance(c.point, p)}).min > coveringRadius
    }).count()

    DEBUG(s"There are actually $actualOutliers outliers using covering radius $coveringRadius")

    result
  }

  def mapReduce[T:ClassTag](rdd: RDD[T], k: Int, tau: Int, distance: (T, T) => Double): IndexedSeq[ProxyPoint[T]] = {
    val coreset = rdd.glom().map { points =>
      MapReduceCoreset.run(points, tau, distance)
    }.reduce { case (a, b) =>
      MapReduceCoreset.compose(a, b)
    }
    // FIXME: Now this is using a
    GMM.run(coreset.points.map(_.point), k, distance).map(ProxyPoint.fromPoint)
  }

  def mapReduce[T:ClassTag](rdd: RDD[T], k: Int, tau: Int, z: Int, distance: (T, T) => Double)
  : (IndexedSeq[ProxyPoint[T]], IndexedSeq[ProxyPoint[T]], Double) = {
    val coreset = rdd.glom().map { points =>
      MapReduceCoreset.run(points, tau + z, distance)
    }.reduce { case (a, b) =>
      MapReduceCoreset.compose(a, b)
    }
    val (centers, outliers, radiusOnProxies) = Outliers.run(coreset.points, k, z, distance)
    val coresetRadius = coreset.points.iterator.map(_.radius).max
    DEBUG(s"Radius of coreset is $coresetRadius")

    (centers, outliers, radius(rdd, centers, radiusOnProxies + coresetRadius, distance))
  }

  def streaming[T:ClassTag](stream: Iterator[T], k: Int, tau: Int, distance: (T, T) => Double): IndexedSeq[ProxyPoint[T]] = {
    val coreset = new StreamingCoreset[T](tau, distance)
    while(stream.hasNext) {
      coreset.update(stream.next())
    }
    GMM.run(coreset.points.map(_.point), k, distance).map(ProxyPoint.fromPoint)
  }

//  def streaming[T:ClassTag](stream: Iterator[T], k: Int, tau: Int, z: Int, distance: (T, T) => Double)
//  : (IndexedSeq[ProxyPoint[T]], IndexedSeq[ProxyPoint[T]]) = {
//    val coreset = new StreamingCoreset[T](tau + z, distance)
//    while(stream.hasNext) {
//      coreset.update(stream.next())
//    }
//    Outliers.run(coreset.points, k, z, distance)
//  }

}
