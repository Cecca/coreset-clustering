package it.unipd.dei.clustering

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

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

  def randomCoreset[T:ClassTag](rdd: RDD[T], tau: Int, distance: (T, T) => Double): Coreset[T] = {
    rdd.glom().map { points =>
      val sampleSet = mutable.HashSet[Int]()
      sampleSet.sizeHint(tau)
      while (sampleSet.size < tau) {
        sampleSet.add(Random.nextInt(points.length))
      }
      val sampleIndices = sampleSet.toArray
      println("Took sample of indices")
      val pairs = points.map { p =>
        sampleIndices.iterator.map { idx =>
          val c = points(idx)
          (idx, distance(c, p))
        }.minBy(_._2)
      }
      println("Built pairs")
      val assignment = pairs.map(_._1)
      val distances = pairs.map(_._2)
      MapReduceCoreset.fromAssignment[T](points, assignment, distances)
    }.reduce({case (a, b) => MapReduceCoreset.compose[T](a, b)})
  }

}
