package it.unipd.dei.clustering

import java.nio.file.{Files, Paths}

import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object VectorIO {

  def writeText(rdd: RDD[Array[Double]], path: String): Unit = {
    rdd.map { arr =>
      arr.mkString(",")
    }.saveAsTextFile(path)
  }

  def readText(sc: SparkContext, path: String): RDD[Array[Double]] = {
    sc.textFile(path).map { line =>
      line.split(",").map(_.toDouble)
    }
  }

  def writeKryo(rdd: RDD[Array[Double]], path: String): Unit = {
    val intermediate: RDD[(BytesWritable, NullWritable)] =
      rdd.mapPartitions({ iterator =>
        iterator.map { arr => {
            val bindata = Array.ofDim[Byte](arr.size*8 + 4)
            val output = new Output(bindata)
            output.writeInt(arr.size)
            output.writeDoubles(arr)
            (new BytesWritable(bindata), NullWritable.get())
          }
        }
      }, preservesPartitioning = true)

      intermediate.saveAsSequenceFile(path)
  }

  def readKryo(sc: SparkContext, path: String): RDD[Array[Double]] = {
    sc.sequenceFile(path, classOf[BytesWritable], classOf[NullWritable])
      .mapPartitions({ _.map { case (bytes, _) =>
          val input = new Input(bytes.getBytes)
          val size = input.readInt()
          input.readDoubles(size)
        }
      }, preservesPartitioning = true)
  }

  def main(args: Array[String]): Unit = {
    val path = "/tmp/vecs"
    Files.deleteIfExists(Paths.get(path))
    val sc = new SparkContext("local", "test")
    val vec = Array[Double](1.0, 2.0, 3.0, 3.2123)
    val rdd: RDD[Array[Double]] = sc.parallelize(Seq(vec))
    println("Writing")
    writeKryo(rdd, path)
    println("Reading")
    val readback = readKryo(sc, path)
    print(readback.take(1).head.mkString(" | "))
  }

}
