package com.sparkx.sample

import com.sparkx.autofix.FixProfileStore
import org.apache.spark.sql.sparkx.PlanHints
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * sparkx Auto-Fix Demo — SEPARATE from [[SparkXDemo]] (which showcases the base *detection*
 * features). This runner exercises the closed-loop auto-fix path end-to-end: it registers the
 * `SparkXAutoFixExtension`, runs each query several times, and shows how the extension learns a
 * fix on the first run and injects hints (at the logical-plan level, so it works for both SQL and
 * DataFrame queries) on subsequent runs.
 *
 * Auto-fix only covers *hint-fixable* problems, which is a subset of what detection covers:
 *   - broadcast join   (a small join side that should be broadcast)
 *   - partitioning     (repartition / coalesce / rebalance for under-partitioning & skew)
 * Problems like GC pressure, shuffle spill, stragglers, serialization, scheduler delay, low CPU
 * and task failures are NOT hint-fixable and are therefore out of auto-fix scope.
 *
 * Usage:
 *
 *   spark-submit \
 *     --master local[4] \
 *     --class com.sparkx.sample.SparkXAutoFixDemo \
 *     sparkx-sample-assembly-0.1.0.jar [broadcast|partitioning|all] [--pause]
 */
object SparkXAutoFixDemo {

  private val Runs = 3

  def main(args: Array[String]): Unit = {
    val which = args.find(a => !a.startsWith("--")).getOrElse("all")
    val pause = args.contains("--pause")

    val storeDir = new java.io.File(
      System.getProperty("java.io.tmpdir"), "sparkx-autofix-demo-" + System.currentTimeMillis())
    storeDir.mkdirs()
    val storePath = storeDir.toURI.toString

    val spark = SparkSession.builder()
      .appName("sparkx-autofix-demo")
      .master(sys.env.getOrElse("MASTER", "local[4]"))
      // Register the auto-fix extension (this is what makes the loop run).
      .config("spark.sql.extensions", "org.apache.spark.sql.sparkx.SparkXAutoFixExtension")
      .config("spark.sparkx.autofix.enabled", "true")
      .config("spark.sparkx.autofix.mode", "auto")
      .config("spark.sparkx.autofix.store.path", storePath)
      .config("spark.sparkx.autofix.maxIterations", "4")
      // Disable Spark's own auto-broadcast + AQE so the injected hints are what makes the
      // observable difference between the baseline run and the fixed runs.
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.hadoop.io.native.lib.available", "false")
      .getOrCreate()

    val store = new FixProfileStore(storePath, spark.sparkContext.hadoopConfiguration)

    banner(spark, storePath)

    try {
      if (which == "all" || which == "broadcast")    broadcastScenario(spark, store)
      if (which == "all" || which == "partitioning") partitioningScenario(spark, store)
    } catch {
      case e: Throwable =>
        println(s"\n  [WARN] auto-fix demo error: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }

    println(s"\n  Fix profiles were persisted under: $storePath")
    spark.sparkContext.uiWebUrl.foreach(u => println(s"  See them in the UI at: $u/sparkx/autofix"))

    if (pause) {
      println("\nPress ENTER to exit …")
      try scala.io.StdIn.readLine() catch { case _: Throwable => }
    }
    spark.stop()
  }

  // ── Scenario 1: broadcast a small join side ────────────────────────────────────
  private def broadcastScenario(spark: SparkSession, store: FixProfileStore): Unit = {
    section("Broadcast join", "A large fact table joined to a tiny dimension table.",
      "auto-fix should learn a BROADCAST(dim) hint after the first run")

    spark.range(0, 5000000).selectExpr("id", "id % 1000 AS dim_id").createOrReplaceTempView("fact")
    spark.range(0, 1000).selectExpr("id AS dim_id", "concat('name_', id) AS name")
      .createOrReplaceTempView("dim")

    val sql =
      """SELECT COUNT(*) FROM fact f JOIN dim d ON f.dim_id = d.dim_id WHERE d.name IS NOT NULL"""

    runLoop(spark, store, () => spark.sql(sql),
      planNote = df => if (planContains(df, "BroadcastHashJoin")) "BroadcastHashJoin (hint applied)"
                       else "SortMergeJoin (baseline)")
  }

  // ── Scenario 2: fix a badly-partitioned shuffle ────────────────────────────────
  private def partitioningScenario(spark: SparkSession, store: FixProfileStore): Unit = {
    section("Partitioning", "A small aggregation forced across 200 shuffle partitions (AQE off).",
      "auto-fix should learn a COALESCE hint to reduce the partition count after the first run")

    spark.range(0, 200000).selectExpr("id", "id % 50 AS grp").createOrReplaceTempView("events")
    val sql = """SELECT grp, COUNT(*) AS c FROM events GROUP BY grp"""

    runLoop(spark, store, () => spark.sql(sql),
      planNote = df =>
        if (planContains(df, "Coalesce") || planContains(df, "Repartition")) "partitioning hint applied"
        else "baseline partitioning")
  }

  // ── Shared loop: run the query N times, report learned/applied hints each time ──
  private def runLoop(
      spark: SparkSession,
      store: FixProfileStore,
      build: () => DataFrame,
      planNote: DataFrame => String): Unit = {
    for (run <- 1 to Runs) {
      val df = build()
      val start = System.nanoTime()
      df.collect() // executes df's own plan, so the plan the learner sees matches df.queryExecution
      val ms = (System.nanoTime() - start) / 1000000L

      // The learner runs asynchronously on the listener bus — give it a moment to persist.
      Thread.sleep(1200)

      val fp = try PlanHints.fingerprintOf(df.queryExecution.analyzed) catch { case _: Throwable => "" }
      val profile = store.load(fp)
      val applied = df.queryExecution.analyzed match {
        case p => PlanHints.strip(p)._2
      }

      println(f"  Run $run%d  (${ms}%d ms)  plan: ${planNote(df)}")
      println(s"           applied this run : ${renderHints(applied)}")
      profile match {
        case Some(p) =>
          println(s"           profile status   : ${p.status}")
          println(s"           best hints       : ${renderHints(p.bestHints)}")
          println(s"           pending (next)   : ${renderHints(p.pendingHints)}")
        case None =>
          println(s"           profile status   : (not yet persisted)")
      }
    }
  }

  private def renderHints(hints: Seq[com.sparkx.autofix.Hint]): String =
    if (hints.isEmpty) "none" else hints.map(_.render).mkString(", ")

  private def planContains(df: DataFrame, token: String): Boolean =
    try df.queryExecution.executedPlan.toString.contains(token) catch { case _: Throwable => false }

  private def section(name: String, what: String, expect: String): Unit = {
    val bar = "─" * 74
    println(s"\n$bar")
    println(s"  SCENARIO : $name")
    println(s"  WHAT     : $what")
    println(s"  EXPECT   : $expect")
    println(bar)
  }

  private def banner(spark: SparkSession, storePath: String): Unit = {
    val line = "═" * 74
    println(s"\n$line")
    println(s"  sparkx Auto-Fix Demo — closed-loop hint injection (SQL + DataFrame)")
    println(line)
    println(s"  Store    : $storePath")
    spark.sparkContext.uiWebUrl.foreach(u => println(s"  Spark UI : $u\n  Auto-Fix : $u/sparkx/autofix"))
    println(line)
  }
}
