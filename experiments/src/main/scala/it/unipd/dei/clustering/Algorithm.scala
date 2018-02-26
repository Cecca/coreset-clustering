package it.unipd.dei.clustering

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Algorithm {

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
