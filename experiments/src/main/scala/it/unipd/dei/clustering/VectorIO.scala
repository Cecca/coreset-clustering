package it.unipd.dei.clustering

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object VectorIO {

  val schema: Schema = new Schema.Parser().parse(
    """
      |{
      |  "type": "record",
      |  "namespace": "it.unipd.dei.clustering",
      |  "name": "Vector",
      |  "fields": [
      |    { "name": "data", "type": "array", "items": "double" },
      |  ]
      |}
    """.stripMargin)

  def write(vecs: RDD[Array[Double]], output: String): Unit = {
    val job = Job.getInstance()
//    val schema = Schema.createArray(Schema.create(Schema.Type.DOUBLE))
    println(s"Schema is \n${schema.toString(true)}")
    AvroJob.setOutputKeySchema(job, schema)

    vecs.map{ v =>
      (new AvroKey[Array[Double]](v), null)
    }.saveAsNewAPIHadoopFile(
      output,
      classOf[AvroKey[Array[Double]]],
      classOf[NullWritable],
      classOf[AvroKeyOutputFormat[Array[Double]]],
      job.getConfiguration
    )
  }

  def read(sc: SparkContext, path: String): RDD[Array[Double]] = {
    val job = Job.getInstance()
//    val schema = Schema.createArray(Schema.create(Schema.Type.DOUBLE))
    AvroJob.setInputKeySchema(job, schema)
    sc.newAPIHadoopFile(
      path,
      classOf[AvroKeyInputFormat[Array[Double]]],
      classOf[AvroKey[Array[Double]]],
      classOf[NullWritable],
      job.getConfiguration
    ).map { case (k, _) =>
      val buf = ArrayBuffer[Double]()
      for (d <- k.datum()) {
        buf += d
      }
      buf.toArray
    }
  }

}
