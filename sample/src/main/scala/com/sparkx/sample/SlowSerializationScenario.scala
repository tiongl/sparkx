package com.sparkx.sample

import org.apache.spark.sql.SparkSession

/**
 * Demonstrates SLOW RESULT SERIALIZATION detection.
 *
 * Each task builds a large, deeply nested data structure and returns it
 * to the driver via collect(). Serializing these complex objects takes
 * measurable time, triggering the slow result serialization alert.
 *
 * sparkx UI: sparkx → Stability → Slow Result Serialization
 */
object SlowSerializationScenario extends Scenario {
  val name        = "Slow Serialization"
  val description = "Tasks return large nested objects — result serialization time spikes"
  val uiPath      = "Stability"

  def run(spark: SparkSession): Unit = {
    println("  Running 20 tasks that each build and return a large nested Map …")

    val rdd = spark.sparkContext.parallelize(1 to 20, 20).map { i =>
      // Build a large nested structure that is expensive to serialize
      val nested = (1 to 5000).map { j =>
        s"key_${i}_$j" -> (1 to 50).map(k => s"val_${i}_${j}_$k").toArray
      }.toMap
      (i, nested)
    }

    val results = rdd.collect()
    println(s"  Collected ${results.length} large maps from tasks.")
    println(s"  Check sparkx → Stability → Slow Result Serialization.")
  }
}
