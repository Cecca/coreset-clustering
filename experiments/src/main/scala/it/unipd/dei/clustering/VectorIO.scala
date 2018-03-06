package it.unipd.dei.clustering

import org.apache.avro.Schema
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object VectorIO {

  val schema = Schema.parseJson(
    """
      |{
      |
      |}
    """.stripMargin)

  def write(vecs: RDD[Array[Double]], output: String): Unit = {
    val job = Job.getInstance()
    val schema = Schema.createArray(Schema.create(Schema.Type.DOUBLE))
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
    val schema = Schema.createArray(Schema.create(Schema.Type.DOUBLE))
    AvroJob.setInputKeySchema(job, schema)
    sc.newAPIHadoopFile(
      path,
      classOf[AvroKeyInputFormat[Array[Double]]],
      classOf[AvroKey[Array[Double]]],
      classOf[NullWritable],
      job.getConfiguration
    ).map { case (k, _) =>
      k.datum()
    }
  }

}
