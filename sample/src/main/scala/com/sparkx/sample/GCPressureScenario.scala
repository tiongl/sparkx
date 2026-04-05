package com.sparkx.sample

import org.apache.spark.sql.SparkSession

/**
 * Demonstrates GC PRESSURE detection.
 *
 * Each task allocates and immediately discards millions of short-lived String
 * objects, forcing frequent minor (and some major) GC collections.
 * The demo lowers spark.sparkx.gcRatioThreshold to 1% so the flag fires even
 * on machines with fast GC.  On typical hardware the GC ratio reaches 5–30%.
 *
 * sparkx UI: sparkx → GC
 */
object GCPressureScenario extends Scenario {
  val name        = "GC Pressure"
  val description = "Tasks allocate millions of short-lived objects to stress the JVM GC"
  val uiPath      = "GC"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("  Running 10 partitions × 200,000 records — generating lots of garbage objects …")
    val total = spark.range(1, 2000001).as[Long].repartition(10)
      .mapPartitions { iter: Iterator[Long] =>
        iter.map { i =>
          var s = s"base_${i}"
          var j = 0
          while (j < 30) {
            s = s"${s}_append_${j}_padding_padding_padding"
            j += 1
          }
          s.length.toLong
        }
      }
      .reduce(_ + _)
    println(s"  Sum of string lengths: $total")
    println(s"  (Check GC time column in sparkx → GC — should show elevated GC ratio)")
  }
}
