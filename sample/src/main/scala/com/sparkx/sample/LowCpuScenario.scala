package com.sparkx.sample

import org.apache.spark.sql.SparkSession

/**
 * Demonstrates LOW CPU UTILIZATION detection.
 *
 * Tasks spend most of their wall-clock time sleeping (simulating I/O wait)
 * rather than computing. The executor CPU time will be a small fraction
 * of the executor run time, triggering the low CPU utilization alert.
 *
 * sparkx UI: sparkx → Partitioning → Low CPU Utilization
 */
object LowCpuScenario extends Scenario {
  val name        = "Low CPU"
  val description = "Tasks sleep 2s each (simulating I/O wait) — CPU ratio drops below 50%"
  val uiPath      = "Partitioning"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("  Running 20 tasks that each sleep 2s (simulating external I/O wait) …")
    val result = spark.range(1, 201).repartition(20)
      .mapPartitions { iter: Iterator[java.lang.Long] =>
        Thread.sleep(2000)
        iter.map(v => v * 2)
      }
      .count()

    println(s"  Counted $result values. Each task spent ~2s sleeping vs microseconds computing.")
    println(s"  Check sparkx → Partitioning → Low CPU Utilization.")
  }
}
