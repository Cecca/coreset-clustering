package it.unipd.dei.clustering

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vectors
import org.scalacheck.Prop.{BooleanOperators, forAll}
import org.scalacheck.{Gen, Properties}

class VectorIOChecks extends Properties("VectorIO") {

  val vectorGen = for {
    l <- Gen.nonEmptyListOf(Gen.choose[Double](Double.MinValue, Double.MaxValue))
  } yield (l)

  property("kryo de/serialization") =
    forAll(Gen.nonEmptyListOf(vectorGen)) { lists =>
      val arrays = lists.map(l => Vectors.dense(l.toArray)).filter(_.size > 0)
      arrays.nonEmpty ==> {
        val path = "/tmp/test-vecs"
        FileUtils.deleteDirectory(new File(path))

        val sc = new SparkContext("local", "test")
        val dArrays = sc.parallelize(arrays)
        VectorIO.writeKryo(dArrays, path)
        val readback = VectorIO.readKryo(sc, path).collect().toList

        val reference = arrays
        val tocheck = readback

        sc.stop()

        (tocheck == reference) :| s"$readback\n$reference"
      }
    }

  property("text de/serialization") =
    forAll(Gen.nonEmptyListOf(vectorGen)) { lists =>
      val arrays = lists.map(l => Vectors.dense(l.toArray)).filter(_.size > 0)
      arrays.nonEmpty ==> {
        val path = "/tmp/test-vecs"
        FileUtils.deleteDirectory(new File(path))

        val sc = new SparkContext("local", "test")
        val dArrays = sc.parallelize(arrays)
        VectorIO.writeText(dArrays, path)
        val readback = VectorIO.readText(sc, path).collect().toList

        val reference = arrays
        val tocheck = readback

        sc.stop()

        (tocheck == reference) :| s"$readback\n$reference"
      }
    }

}
