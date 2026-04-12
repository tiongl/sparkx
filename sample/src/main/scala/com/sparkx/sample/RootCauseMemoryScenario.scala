package com.sparkx.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Demonstrates ROOT CAUSE CORRELATION — "Memory Pressure".
 *
 * Combines a large broadcast with a shuffle that spills, producing both
 * GC Pressure and Shuffle Spill on the same stage. The Root Cause page
 * should correlate these into:
 *   Root Cause: "Memory Pressure"
 *   Symptoms:   GC Pressure + Shuffle Spill (+ possibly Large Broadcast)
 *
 * sparkx UI: sparkx → Root Cause
 */
object RootCauseMemoryScenario extends Scenario {
  val name        = "Root Cause: Memory"
  val description = "Large broadcast + spilling shuffle → GC Pressure + Spill on same stage"
  val uiPath      = "Root Cause"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("  Broadcasting a 2M-entry map to eat up memory …")
    val bigMap = (1 to 2000000).map(i => i -> s"value_$i").toMap
    val bcast  = spark.sparkContext.broadcast(bigMap)

    println("  Running a shuffle that will spill under memory pressure …")
    val df = spark.range(1, 1500001).repartition(40)
      .withColumn("key", ($"id" % 500).cast("int"))
      .withColumn("payload", lpad(lit("y"), 200, "y"))

    // The broadcast consumes heap, making the shuffle more likely to spill
    // and amplifying GC pressure
    val result = df.repartition(4, $"key")
      .groupBy("key")
      .agg(
        count("payload").as("cnt"),
        // Reference the broadcast to keep it alive
        lit(bcast.value.size).as("lookup_size")
      )
      .count()

    bcast.unpersist()
    println(s"  Grouped into $result distinct keys under memory pressure.")
    println(s"  Check sparkx → Root Cause for correlated Memory Pressure symptoms.")
  }
}
