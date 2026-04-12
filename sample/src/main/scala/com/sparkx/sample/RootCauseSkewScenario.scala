package com.sparkx.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Demonstrates ROOT CAUSE CORRELATION — "Uneven Data Distribution".
 *
 * A single skewed groupBy stage triggers BOTH Data Skew AND Straggler Tasks
 * on the same stage. The GC pressure from the skewed partition may also fire.
 * sparkx's Root Cause page should correlate these into:
 *   Root Cause: "Uneven Data Distribution"
 *   Symptoms:   Data Skew + Straggler Tasks (+ possibly GC Pressure)
 *
 * sparkx UI: sparkx → Root Cause
 */
object RootCauseSkewScenario extends Scenario {
  val name        = "Root Cause: Skew"
  val description = "Skewed groupBy triggers Data Skew + Stragglers + GC on one stage"
  val uiPath      = "Root Cause"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("  Generating 2M records where 98% share one key …")
    println("  (This will trigger Data Skew + Straggler + GC on the reduce stage)")

    val df = spark.range(1, 2000001).repartition(50)
      .withColumn("key", when($"id" % 50 =!= 0, lit("HOT_KEY"))
        .otherwise(concat(lit("k_"), ($"id" % 200).cast("string"))))
      .withColumn("payload", lpad(lit("x"), 200, "x"))

    // collect_list forces all values for HOT_KEY into one reducer's memory,
    // causing skew, straggling, GC pressure, and possibly spill
    val result = df.repartition(10, $"key")
      .groupBy("key")
      .agg(count("payload").as("cnt"), sum(length($"payload")).as("total_len"))
      .collect()

    println(s"  Reduced to ${result.length} distinct keys.")
    println(s"  Check sparkx → Root Cause for correlated symptoms.")
  }
}
