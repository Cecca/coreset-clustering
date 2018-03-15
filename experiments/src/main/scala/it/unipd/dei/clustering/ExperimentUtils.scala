package it.unipd.dei.clustering

import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import it.unipd.dei.experiment.Experiment

import scala.collection.JavaConversions

object ExperimentUtils {


  def jMap(tuples: (String, Any)*): java.util.Map[String, Object] =
    JavaConversions.mapAsJavaMap(tuples.map(fixTuple).toMap)

  def s2j(any: Any): Object = any match {
    case v: Boolean => v: java.lang.Boolean
    case c: Char => c: java.lang.Character
    case v: Byte => v: java.lang.Byte
    case v: Short => v: java.lang.Short
    case v: Int => v: java.lang.Integer
    case v: Float => v: java.lang.Float
    case v: Double => v: java.lang.Double
    case Some(v) => s2j(v)
    case None => null
    case obj: java.lang.Object => obj
  }

  def fixTuple(tuple: (Any, Any)): (String, Object) = (tuple._1.toString, s2j(tuple._2))

  def timed[T](fn: => T): (T, Long) = {
    val start = System.currentTimeMillis()
    val res = fn
    val end = System.currentTimeMillis()
    (res, end - start)
  }

  def appendTimers(table: String, experiment: Experiment, registry: MetricRegistry): Unit = {
    for (entry <- JavaConversions.iterableAsScalaIterable(registry.getTimers.entrySet())) {
      val snapshot = entry.getValue.getSnapshot
      // Times get rescaled from nanoseconds to milliseconds
      val values = snapshot.getValues.map(_ / 1000.0)
      val totalTime =
        if (values.length == entry.getValue.getCount) {
          Some(values.sum)
        } else {
          None
        }
      println(s"Total time is $totalTime")
      experiment.append(table, jMap(
        "timer" -> entry.getKey,
        "mean-time" -> snapshot.getMean / 1000.0,
        "max-time" -> snapshot.getMax / 1000.0,
        "min-time" -> snapshot.getMin / 1000.0,
        "median-time" -> snapshot.getMedian / 1000.0,
        "count" -> entry.getValue.getCount,
        "total-time" -> totalTime
      ))
    }
  }

}
