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
      val topDists = distances.top(z+1)
//      println(s"Top ${z+1} distances: ${topDists.reverse.mkString("\n")}")
      topDists.last
    }
  }

  def mapReduce[T:ClassTag](rdd: RDD[T], tau: Int, distance: (T, T) => Double): Coreset[T] = {
    val coreset = rdd.glom().map { points =>
      MapReduceCoreset.run(points, tau, distance)
    }.reduce { case (a, b) =>
      MapReduceCoreset.compose(a, b)
    }
    if (coreset.points.size != tau*rdd.getNumPartitions) {
      throw new IllegalArgumentException(s"Coreset has ${coreset.points.size} instead of ${tau*rdd.getNumPartitions}")
    }

    coreset
  }

  def streaming[T:ClassTag](stream: Iterator[T], tau: Int, distance: (T, T) => Double): StreamingCoreset[T] = {
    val coreset = new StreamingCoreset[T](tau, distance)
    while(stream.hasNext) {
      coreset.update(stream.next())
    }
    if (coreset.numPoints > tau) {
      throw new IllegalArgumentException(s"Coreset has ${coreset.points.size} instead of $tau")
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
      val assignment = Array.ofDim[Int](points.length)
      val distances = Array.ofDim[Double](points.length)
      for (i <- points.indices) {
        var minIdx = 0
        var minDist = Double.PositiveInfinity
        for (idx <- sampleIndices) {
          val d = distance(points(idx), points(i))
          if (d < minDist) {
            minDist = d
            minIdx = idx
          }
        }
        assignment(i) = minIdx
        distances(i) = minDist
      }
      println("Built assignment")
      MapReduceCoreset.fromAssignment[T](points, assignment, distances)
    }.reduce({case (a, b) => MapReduceCoreset.compose[T](a, b)})
  }

}
