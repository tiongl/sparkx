package com.sparkx.sample

import org.apache.spark.sql.SparkSession

/**
 * sparkx Demo Application
 *
 * Runs one or more showcase scenarios, each designed to trigger a specific
 * performance issue that sparkx detects and visualises in the Spark UI.
 *
 * Usage:
 *
 *   spark-submit \
 *     --master local[4] \
 *     --conf spark.extraListeners=com.sparkx.SparkXListener \
 *     --class com.sparkx.sample.SparkXDemo \
 *     sparkx-sample-assembly-0.1.0.jar [scenario] [--pause]
 *
 * Arguments:
 *   scenario   One of: skew | straggler | gc | spill | broadcast | all (default: all)
 *   --pause    Wait for Enter key before exiting so you can browse the Spark UI
 *              (the UI at http://localhost:4040 disappears when the driver exits)
 *
 * Examples:
 *   ... SparkXDemo                    # run all scenarios
 *   ... SparkXDemo skew --pause       # run only the skew demo, then pause
 *   ... SparkXDemo broadcast --pause  # run only the broadcast demo, then pause
 */
object SparkXDemo {

  private val scenarios: Map[String, Scenario] = Map(
    "skew"         -> DataSkewScenario,
    "straggler"    -> StragglerScenario,
    "gc"           -> GCPressureScenario,
    "spill"        -> ShuffleSpillScenario,
    "broadcast"    -> BroadcastScenario,
    "smalltasks"   -> SmallTasksScenario,
    "failures"     -> TaskFailuresScenario,
    "lowcpu"       -> LowCpuScenario,
    "scheddelay"   -> SchedulerDelayScenario,
    "slowser"      -> SlowSerializationScenario,
    "rc-skew"      -> RootCauseSkewScenario,
    "rc-memory"    -> RootCauseMemoryScenario,
    "rc-instability" -> RootCauseInstabilityScenario
  )

  def main(args: Array[String]): Unit = {
    val scenarioArg = args.find(a => !a.startsWith("--")).getOrElse("all")
    val pause       = args.contains("--pause")

    val toRun: Seq[Scenario] = scenarioArg match {
      case "all" => Seq(DataSkewScenario, StragglerScenario, GCPressureScenario,
                        ShuffleSpillScenario, BroadcastScenario,
                        SmallTasksScenario, TaskFailuresScenario,
                        LowCpuScenario, SchedulerDelayScenario, SlowSerializationScenario,
                        RootCauseSkewScenario, RootCauseMemoryScenario,
                        RootCauseInstabilityScenario)
      case key   =>
        scenarios.get(key) match {
          case Some(s) => Seq(s)
          case None    =>
            println(s"Unknown scenario '$key'. Valid: ${scenarios.keys.mkString(", ")}, all")
            sys.exit(1)
        }
    }

    // Resolve a cross-platform events directory and create it upfront
    val defaultEventsDir = java.nio.file.Paths
      .get(System.getProperty("java.io.tmpdir"), "spark-events").toString
    val eventsDir = sys.env.getOrElse("SPARK_EVENTS_DIR", defaultEventsDir)
    new java.io.File(eventsDir).mkdirs()

    val spark = SparkSession.builder()
      .appName("sparkx-demo")
      // Lower thresholds so issues are reliably flagged in the demo
      .config("spark.sparkx.skewMultiplier",          "2.0")   // default 3.0
      .config("spark.sparkx.gcRatioThreshold",        "0.01")  // default 0.10 (1% for demo)
      .config("spark.sparkx.stragglerIQRFactor",      "1.5")
      .config("spark.sparkx.broadcastSizeMB",         "50")    // default 200 MB
      .config("spark.sparkx.smallTaskMedianMs",       "50")    // flag tasks < 50 ms median
      .config("spark.sparkx.smallTaskMinCount",       "100")   // need >= 100 tasks
      .config("spark.sparkx.underPartitionRatio",     "0.5")
      // Allow 4 task failures so TaskFailuresScenario can retry and show failure counts
      .config("spark.task.maxFailures",              "4")
      // Lowered thresholds for new detections
      .config("spark.sparkx.lowCpuRatioThreshold",   "0.8")   // default 0.5 — flag anything < 80% CPU
      .config("spark.sparkx.lowCpuMinRunTimeMs",     "5000")  // default 60000 — trigger on short stages
      .config("spark.sparkx.highSchedulerDelayMs",   "50")    // default 500 — flag even 50ms delay
      .config("spark.sparkx.schedulerDelayRatio",    "0.1")   // default 0.5 — flag 10% ratio
      .config("spark.sparkx.resultSerializationMs",  "10")    // default 200 — flag even 10ms
      // Enable event logging so this run can be replayed in the History Server
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir",     eventsDir)
      .getOrCreate()

    printBanner(toRun, spark.sparkContext.uiWebUrl)

    val failures = toRun.flatMap { scenario =>
      try { scenario.execute(spark); None }
      catch {
        case e: Exception =>
          val msg = Option(e.getCause).getOrElse(e).getMessage
          println(s"\n  [WARN] Scenario '${scenario.name}' failed: $msg")
          println(s"  (Continuing to next scenario — you can still inspect completed stages in the UI)\n")
          Some(scenario.name -> msg)
      }
    }

    printSummary(spark.sparkContext.uiWebUrl, failures)

    if (pause) {
      println("\nPress ENTER to exit and shut down the Spark UI …")
      val line = try { scala.io.StdIn.readLine() } catch { case _: Exception => null }
      if (line == null) {
        println("  (stdin unavailable - press Ctrl+C to exit)")
        val latch = new java.util.concurrent.CountDownLatch(1)
        Runtime.getRuntime.addShutdownHook(new Thread {
          override def run(): Unit = latch.countDown()
        })
        latch.await()
      }
    }

    spark.stop()
  }

  private def printBanner(scenarios: Seq[Scenario], uiUrl: Option[String]): Unit = {
    val width = 72
    val line  = "═" * width
    println(s"\n$line")
    println(s"  sparkx Demo — Spark Performance Issue Showcase")
    println(line)
    println(s"  Running ${scenarios.length} scenario(s):")
    scenarios.foreach(s => println(s"    • ${s.name}"))
    uiUrl.foreach(u => println(s"\n  Spark UI : $u\n  sparkx tab: $u/sparkx"))
    println(line)
  }

  private def printSummary(uiUrl: Option[String], failures: Seq[(String, String)]): Unit = {
    val line = "═" * 72
    println(s"\n$line")
    if (failures.nonEmpty) {
      println(s"  ${failures.length} scenario(s) failed (data may still appear in completed stages):")
      failures.foreach { case (name, msg) => println(s"    ✗ $name: $msg") }
      println()
    }
    println(s"  All scenarios complete.  Open the sparkx tab to see detected issues:")
    println()
    println(s"  Overview      → shows all flagged issues in one place")
    println(s"  Root Cause    → groups correlated symptoms into actionable root causes")
    println(s"  Skew          → stages where max task >> median task duration")
    println(s"  GC            → stages / executors with high GC overhead")
    println(s"  Spill         → stages that overflowed shuffle buffers to disk")
    println(s"  Stragglers    → tasks that took far longer than their peers")
    println(s"  Broadcast     → broadcast variables exceeding the size threshold")
    println(s"  Partitioning  → small tasks, under-partitioned, scheduler delay, low CPU")
    println(s"  Stability     → failures, retries, speculative, serialization, memory skew")
    println()
    uiUrl.foreach(u => println(s"  URL: $u/sparkx"))
    println(line)
  }
}
