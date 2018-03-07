package it.unipd.dei.clustering

import java.io.{FileInputStream, InputStream}

import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.io.Source

object Vectorize {

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input = opt[String](required = true)
    val model = opt[String](required = true)
    val output = opt[String](required = true)
    verify()
  }

  def getInputStream(path: String): InputStream = {
    val fis = new FileInputStream(path)
    val ext = path.split("\\.").last
    if ("bz2".equals(ext) || "bzip2".equals(ext)) {
      new CompressorStreamFactory()
        .createCompressorInputStream(CompressorStreamFactory.BZIP2, fis)
    } else {
      fis
    }
  }

  def main(args: Array[String]): Unit = {
    val arguments = new Args(args)

    val model = Source.fromInputStream(getInputStream(arguments.model())).getLines().map { line =>
      val tokens = line.split(" ")
      val word = tokens.head
      val vec: Array[Double] = tokens.tail.map(_.toDouble)
      (word, vec)
    }.toMap

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("vectorize")
    val sc = new SparkContext(sparkConf)
    val bModel = sc.broadcast(model)
    val dim = model.get("be").size

    val vecs = sc.textFile(arguments.input()).map { line =>
      val vec = Array.ofDim[Double](dim)
      var cnt = 0
      for (word <- line.split(" ")) {
        bModel.value.get(word) match {
          case Some(v) =>
            VectorUtils.sum(vec, v, vec)
            cnt += 1
          case None =>
          // SKip word
        }
      }
      for (i <- vec.indices) {
        vec(i) = vec(i) / cnt
      }
      vec
    }

    VectorIO.writeText(vecs, arguments.output())

  }

}
