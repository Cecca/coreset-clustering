package it.unipd.dei.clustering

import org.apache.spark.rdd.RDD
import Debug.DEBUG

import scala.collection.mutable
import scala.reflect.ClassTag

object Algorithm {

  def radius[T:ClassTag](allPoints: RDD[T],
                         centers: IndexedSeq[ProxyPoint[T]],
                         outliers: IndexedSeq[ProxyPoint[T]],
                         distance: (T, T) => Double): Double = {
    val bCenters = allPoints.context.broadcast(centers)
    val bOutliers = allPoints.context.broadcast(outliers)

    // We have to group points that are close to a given outlier,
    // so to be able to count them correctly

    val nonOutliersRadius = allPoints.filter({ p =>
      // TODO: This point may be covered by a non-outlier proxy
      bOutliers.value.map(o => distance(o.point, p) > o.radius).reduce(_ && _)
    }).map({ p =>
      bCenters.value.iterator.map({ c =>
        distance(c, p)
      }).min
    }).max()

    val potentialOutliers = allPoints.filter({ p =>
      // TODO: Shortcut this operation as soon as it evaluates to true
      bOutliers.value.map(o => distance(o.point, p) <= o.radius).reduce(_ || _)
    }).collect()

    val uncoveredOutliers = mutable.HashSet[T](potentialOutliers :_*)

    outliers.foreach({ o =>
      val covered = potentialOutliers.filter(p => distance(p, o.point) <= o.radius).iterator
      var w = o.weight
      while (w > 0 && covered.hasNext) {
        val p = covered.next()
        if (uncoveredOutliers.contains(p)) {
          uncoveredOutliers.remove(p)
          w -= 1
        }
      }
    })
    DEBUG(s"There are ${uncoveredOutliers.size} outliers not covered by any outlier proxy")
    if (uncoveredOutliers.nonEmpty) {
      val outliersRadius = Utils.maxMinDistance(centers.map(_.point), uncoveredOutliers.toVector, distance)
      math.max(nonOutliersRadius, outliersRadius)
    } else {
      nonOutliersRadius
    }
  }

  def mapReduce[T:ClassTag](rdd: RDD[T], k: Int, tau: Int, distance: (T, T) => Double): IndexedSeq[T] = {
    val coreset = rdd.glom().map { points =>
      MapReduceCoreset.run(points, tau, distance)
    }.reduce { case (a, b) =>
      MapReduceCoreset.compose(a, b)
    }
    GMM.run(coreset.points.map(_.point), k, distance)
  }

  def mapReduce[T:ClassTag](rdd: RDD[T], k: Int, tau: Int, z: Int, distance: (T, T) => Double)
  : (IndexedSeq[T], IndexedSeq[T]) = {
    val coreset = rdd.glom().map { points =>
      MapReduceCoreset.run(points, tau + z, distance)
    }.reduce { case (a, b) =>
      MapReduceCoreset.compose(a, b)
    }
    Outliers.run(coreset.points, k, z, distance)
  }

  def streaming[T:ClassTag](stream: Iterator[T], k: Int, tau: Int, distance: (T, T) => Double): IndexedSeq[T] = {
    val coreset = new StreamingCoreset[T](tau, distance)
    while(stream.hasNext) {
      coreset.update(stream.next())
    }
    GMM.run(coreset.points.map(_.point), k, distance)
  }

  def streaming[T:ClassTag](stream: Iterator[T], k: Int, tau: Int, z: Int, distance: (T, T) => Double)
  : (IndexedSeq[T], IndexedSeq[T]) = {
    val coreset = new StreamingCoreset[T](tau + z, distance)
    while(stream.hasNext) {
      coreset.update(stream.next())
    }
    Outliers.run(coreset.points, k, z, distance)
  }

}
