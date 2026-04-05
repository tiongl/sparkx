package com.sparkx.sample

import org.apache.spark.sql.SparkSession

/**
 * Demonstrates STRAGGLER TASK detection.
 *
 * Runs a job with 20 partitions. Partitions 0 and 1 sleep for 6 s while the
 * remaining 18 finish in ~50–200 ms.  The two slow tasks far exceed the
 * Q3 + 1.5×IQR upper fence and are flagged as stragglers.
 *
 * sparkx UI: sparkx → Stragglers
 */
object StragglerScenario extends Scenario {
  val name        = "Straggler Tasks"
  val description = "2 of 20 tasks sleep 6 s; the rest finish in <200 ms"
  val uiPath      = "Stragglers"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("  Running 20-partition job — 2 tasks will be artificially slow (6 s) …")
    val count = spark.range(1, 2001).as[Long].repartition(20)
      .mapPartitions { iter: Iterator[Long] =>
        val ctx = org.apache.spark.TaskContext.get()
        val idx = if (ctx != null) ctx.partitionId() else 0
        if (idx < 2) {
          Thread.sleep(6000L)
        } else {
          Thread.sleep(50L + (idx * 7L) % 150L)
        }
        iter
      }
      .count()
    println(s"  Processed $count records across 20 tasks.")
  }
}
