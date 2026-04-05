package com.sparkx.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Demonstrates SMALL TASKS (over-partitioned) detection.
 *
 * Creates a job with far too many partitions for the data size,
 * resulting in thousands of tiny sub-millisecond tasks where
 * scheduling overhead dwarfs actual computation.
 *
 * sparkx UI: sparkx → Partitioning → Over-partitioned Stages
 */
object SmallTasksScenario extends Scenario {
  val name        = "Small Tasks"
  val description = "Tiny tasks from over-partitioning — scheduling overhead dominates"
  val uiPath      = "Partitioning"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("  Creating 500 partitions over a tiny dataset (triggers small-task warning) …")
    val count = spark.range(1, 1001).repartition(500)
      .withColumn("sq", $"id" * $"id")
      .filter($"sq" % 2 === 0)
      .count()
    println(s"  Counted $count even squares across 500 partitions.")
    println(s"  Most tasks ran in < 10 ms — check sparkx → Partitioning → Over-partitioned.")
  }
}
