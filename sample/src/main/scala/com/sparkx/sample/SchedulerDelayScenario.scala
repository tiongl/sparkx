package com.sparkx.sample

import org.apache.spark.sql.SparkSession

/**
 * Demonstrates HIGH SCHEDULER DELAY detection.
 *
 * Launches a large number of tasks (800) that each do minimal work,
 * on a local[2] executor setup. With only 2 cores processing 800 tasks,
 * later tasks queue up and experience significant scheduler delay.
 * The demo lowers the threshold to catch even moderate delays.
 *
 * sparkx UI: sparkx → Partitioning → High Scheduler Delay
 */
object SchedulerDelayScenario extends Scenario {
  val name        = "Scheduler Delay"
  val description = "800 tasks on few cores — later tasks queue up with measurable scheduler delay"
  val uiPath      = "Partitioning"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("  Launching 800 tasks that each sleep 50ms on limited cores …")
    println("  (scheduler delay builds up as tasks queue behind each other)")
    val result = spark.range(1, 8001).repartition(800)
      .mapPartitions { iter: Iterator[java.lang.Long] =>
        Thread.sleep(50)
        iter.map(v => v + 1)
      }
      .count()

    println(s"  Counted $result values across 800 tasks.")
    println(s"  Check sparkx → Partitioning → High Scheduler Delay.")
  }
}
