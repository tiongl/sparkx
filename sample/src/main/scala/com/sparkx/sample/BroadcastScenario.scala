package com.sparkx.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Demonstrates LARGE BROADCAST detection.
 *
 * Constructs a ~60 MB lookup DataFrame, then broadcast-joins it with a
 * probe table.  The demo lowers spark.sparkx.broadcastSizeMB to 50 MB so
 * the flag fires reliably.
 *
 * In production, the default threshold is 200 MB.  Large broadcasts strain
 * driver memory and saturate network during task launch.
 *
 * sparkx UI: sparkx → Broadcast
 */
object BroadcastScenario extends Scenario {
  val name        = "Large Broadcast"
  val description = "Broadcast-joins a ~60 MB lookup table (threshold lowered to 50 MB for demo)"
  val uiPath      = "Broadcast"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    // 1,000,000 entries × ~60 bytes serialized ≈ 60 MB → triggers the 50 MB threshold.
    println("  Building 1,000,000-entry lookup table (~60 MB serialized) …")
    val lookup: Map[Int, String] =
      Iterator.range(1, 1000001)
        .map(i => i -> f"lookup_value_for_key_${i}%07d_extra_padding_data")
        .toMap

    val bc = spark.sparkContext.broadcast(lookup)

    val hits = spark.range(1, 100001).as[Long].repartition(10)
      .mapPartitions { iter: Iterator[Long] =>
        val m = bc.value
        iter.filter(v => m.contains(v.toInt))
      }
      .count()

    println(s"  Found $hits matching keys via broadcast lookup.")
    bc.unpersist()
    println(s"  Broadcast variable will appear in sparkx → Broadcast (exceeds 50 MB threshold).")
  }
}
