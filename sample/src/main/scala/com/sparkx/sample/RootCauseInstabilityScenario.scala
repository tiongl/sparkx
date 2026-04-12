package com.sparkx.sample

import org.apache.spark.sql.SparkSession

/**
 * Demonstrates ROOT CAUSE CORRELATION — "Node / Executor Instability".
 *
 * Combines task failures with speculative-like behavior: some tasks fail
 * on first attempt and must be retried. On the same stage, the retried
 * tasks become stragglers (they start late). The Root Cause page should
 * correlate Task Failures + Speculative Tasks into:
 *   Root Cause: "Node / Executor Instability"
 *
 * sparkx UI: sparkx → Root Cause
 */
object RootCauseInstabilityScenario extends Scenario {
  val name        = "Root Cause: Instability"
  val description = "Task failures + slow retries → Task Failures + Speculative on same stage"
  val uiPath      = "Root Cause"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("  Running 40 tasks where 25% fail on first attempt + slow retries …")
    // Fail many partitions to increase chance of speculative tasks being triggered
    val failPartitions = spark.sparkContext.broadcast(
      Set(1, 3, 5, 7, 9, 11, 13, 15, 17, 19))

    val result = spark.range(1, 401).as[Long].repartition(40)
      .mapPartitions { iter: Iterator[Long] =>
        val ctx = org.apache.spark.TaskContext.get()
        val partIdx = if (ctx != null) ctx.partitionId() else 0
        if (failPartitions.value.contains(partIdx)) {
          if (ctx != null && ctx.attemptNumber() == 0) {
            throw new RuntimeException(s"Simulated hardware fault in partition $partIdx")
          }
          // Retry succeeds but is slow (simulating degraded node)
          Thread.sleep(1000)
        }
        iter.map(v => v * 2)
      }
      .count()

    failPartitions.unpersist()
    println(s"  Counted $result values after failures and retries.")
    println(s"  Check sparkx → Root Cause for correlated Instability symptoms.")
  }
}
