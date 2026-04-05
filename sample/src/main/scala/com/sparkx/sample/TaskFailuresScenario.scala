package com.sparkx.sample

import org.apache.spark.sql.SparkSession

/**
 * Demonstrates TASK FAILURES detection.
 *
 * Approximately 10% of tasks throw an exception and must be retried.
 * The demo uses maxTaskFailures = 4 (default) so the job succeeds
 * after retries, but the failure count shows up in sparkx.
 *
 * sparkx UI: sparkx → Stability → Task Failures
 */
object TaskFailuresScenario extends Scenario {
  val name        = "Task Failures"
  val description = "~10% of tasks fail and are retried — shows failure rate per stage"
  val uiPath      = "Stability"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("  Running 40 tasks where ~10% randomly fail on first attempt …")
    val failPartitions = spark.sparkContext.broadcast(Set(2, 7, 12, 17))

    val result = spark.range(1, 401).as[Long].repartition(40)
      .mapPartitions { iter: Iterator[Long] =>
        val ctx = org.apache.spark.TaskContext.get()
        val partIdx = if (ctx != null) ctx.partitionId() else 0
        if (failPartitions.value.contains(partIdx)) {
          if (ctx != null && ctx.attemptNumber() == 0)
            throw new RuntimeException(s"Simulated transient failure in partition $partIdx")
        }
        iter.map(_ * 2)
      }
      .count()

    failPartitions.unpersist()
    println(s"  Counted $result values after retries.")
    println(s"  Check sparkx → Stability → Task Failures for the failure summary.")
  }
}
