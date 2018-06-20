package it.unipd.dei.clustering

import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object VectorIO {

  def writeText(rdd: RDD[Vector], path: String): Unit = {
    rdd.map { vec =>
      vec.toArray.mkString(",")
    }.saveAsTextFile(path)
  }

  def readText(sc: SparkContext, path: String): RDD[Vector] = {
    sc.textFile(path).map { line =>
      Vectors.dense(line.split(",").map(_.toDouble))
    }
  }

  def writeKryo(rdd: RDD[Vector], path: String): Unit = {
    val intermediate: RDD[(BytesWritable, NullWritable)] =
      rdd.mapPartitions({ iterator =>
        iterator.map { vec => {
            val bindata = Array.ofDim[Byte](vec.size*8 + 4)
            val output = new Output(bindata)
            output.writeInt(vec.size)
            output.writeDoubles(vec.toArray)
            (new BytesWritable(bindata), NullWritable.get())
          }
        }
      }, preservesPartitioning = true)

      intermediate.saveAsSequenceFile(path, Some(classOf[org.apache.hadoop.io.compress.BZip2Codec]))
  }

  def readKryo(sc: SparkContext, path: String): RDD[Vector] = {
    sc.sequenceFile(path, classOf[BytesWritable], classOf[NullWritable])
      .mapPartitions({ _.map { case (bytes, _) =>
          val input = new Input(bytes.getBytes)
          val size = input.readInt()
          Vectors.dense(input.readDoubles(size))
        }
      }, preservesPartitioning = true)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException("USAGE: convert input output")
    }
    val input = args(0)
    val output = args(1)
    val sc = new SparkContext("local[*]", "vectors conversion")

    writeKryo(readText(sc, input), output)
  }

}
