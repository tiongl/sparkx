package com.sparkx.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Demonstrates SHUFFLE SPILL detection.
 *
 * Generates 1,500,000 rows with large string payloads, then groups them
 * by key using very few reduce partitions.  Each reducer receives a large
 * amount of data, which overflows the sort buffer and spills to disk.
 *
 * Tip: to guarantee spill, run with a small executor memory:
 *   --conf spark.executor.memory=512m
 *
 * sparkx UI: sparkx → Spill
 */
object ShuffleSpillScenario extends Scenario {
  val name        = "Shuffle Spill"
  val description = "Large groupBy with few partitions — intermediate data spills to disk"
  val uiPath      = "Spill"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("  Generating 1.5M rows with 100-char payloads, groupBy into 4 partitions …")
    println("  (collect_list materializes all values per key in memory → triggers spill)")

    val df = spark.range(1, 1500001).repartition(40)
      .withColumn("key", ($"id" % 1000).cast("int"))
      .withColumn("payload", lpad(lit("x"), 100, "x"))

    val keyCount = df.repartition(4, $"key")
      .groupBy("key").agg(count("payload").as("cnt"))
      .count()

    println(s"  Grouped into $keyCount distinct keys across 4 partitions.")
    println(s"  If spill occurred it will appear in sparkx → Spill.")
  }
}
