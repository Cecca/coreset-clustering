import it.unipd.dei.clustering.{GMM, MapReduceCoreset, Outliers}
import org.apache.spark.rdd.RDD

object Algorithm {

  def mapReduce[T](rdd: RDD[T], k: Int, tau: Int, distance: (T, T) => Double): IndexedSeq[T] = {
    val coreset = rdd.glom().map { points =>
      MapReduceCoreset.run(points, tau, distance)
    }.reduce { case (a, b) =>
      MapReduceCoreset.compose(a, b)
    }
    GMM.run(coreset.points.map(_.point), k, distance)
  }

  def mapReduce[T](rdd: RDD[T], k: Int, tau: Int, z: Int, distance: (T, T) => Double): IndexedSeq[T] = {
    val coreset = rdd.glom().map { points =>
      MapReduceCoreset.run(points, tau + z, distance)
    }.reduce { case (a, b) =>
      MapReduceCoreset.compose(a, b)
    }
    val (centers, outliers) = Outliers.run(coreset.points, k, z, distance)
    // TODO Report number of outliers
    centers
  }


}
