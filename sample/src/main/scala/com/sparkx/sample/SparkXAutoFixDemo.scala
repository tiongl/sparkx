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
 *   - skew resolution  (inject double-broadcast / salting rewrites for big, imbalanced joins)
 * Problems like GC pressure, shuffle spill, stragglers, serialization, scheduler delay, low CPU
 * and task failures are NOT hint-fixable and are therefore out of auto-fix scope.
 *
 * Usage:
 *
 *   spark-submit \
 *     --master local[4] \
 *     --class com.sparkx.sample.SparkXAutoFixDemo \
 *     sparkx-sample-assembly-0.1.0.jar [broadcast|partitioning|skew|all] [--pause]
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
      // Register the SparkX listener so the SparkX UI tab (including the Auto-Fix page at
      // /sparkx/autofix) is attached to the Spark Web UI. Without this the tab never mounts
      // and /sparkx/* requests fall through to a redirect to /jobs.
      .config("spark.extraListeners", "com.sparkx.SparkXListener")
      .config("spark.sparkx.autofix.enabled", "true")
      .config("spark.sparkx.autofix.mode", "auto")
      .config("spark.sparkx.autofix.store.path", storePath)
      .config("spark.sparkx.autofix.maxIterations", "4")
      // Flag skew-prone joins aggressively for the demo (a 2x size imbalance is enough).
      .config("spark.sparkx.autofix.skew.factor", "2.0")
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
      if (which == "all" || which == "skew")         skewScenario(spark, store)
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

  // ── Scenario 3: resolve skew on a big, imbalanced join via an injected rewrite ──
  private def skewScenario(spark: SparkSession, store: FixProfileStore): Unit = {
    section("Skew resolution",
      "A large fact with a hot key joined to a mid-sized table (too big to broadcast).",
      "auto-fix should inject a skew rewrite (SPLIT_BROADCAST or SALT) after the first run")

    import com.sparkx.join.AutoSaltJoin._
    import org.apache.spark.sql.functions._

    // 20M-row fact where 95% of rows share one hot key → skewed reducer if joined by key.
    val fact = spark.range(0, 20000000)
      .select(when(col("id") % 20 =!= 0, lit(0L)).otherwise(col("id")).as("k"), col("id").as("v"))
    // A mid-sized "dimension" — large enough to defeat plain broadcast, so a plain
    // BROADCAST hint is not the answer and a skew rewrite is injected instead.
    val dim = spark.range(0, 3000000).select(col("id").as("k"), concat(lit("n_"), col("id")).as("name"))

    fact.createOrReplaceTempView("skew_fact")
    dim.createOrReplaceTempView("skew_dim")
    val sql = """SELECT COUNT(*) FROM skew_fact f JOIN skew_dim d ON f.k = d.k"""

    // Correct baseline COUNT(*) for the naive join, to compare the injected-rewrite runs against.
    val expected = spark.sql(sql).collect()(0).getLong(0)
    println(s"           baseline count   : $expected (naive join)")

    runLoop(spark, store, () => spark.sql(sql),
      planNote = df => if (planContains(df, "SortMergeJoin")) "SortMergeJoin (skew-prone)"
                       else "non-shuffle join")

    // Show what sparkx learned for this query.
    val fp = try PlanHints.fingerprintOf(spark.sql(sql).queryExecution.analyzed)
             catch { case _: Throwable => "" }
    store.load(fp).foreach { p =>
      println(s"           learned best     : ${renderHints(p.bestHints)}")
      if (p.recommendations.nonEmpty)
        println(s"           advisory (n/a)   : ${renderHints(p.recommendations)}")
    }

    // Directly validate BOTH injected catalyst rewrites are correctness-preserving by applying
    // each skew hint to the resolved plan and executing it — independent of which strategy the
    // tuner happened to pick above (it chose SPLIT_BROADCAST for this data).
    import com.sparkx.autofix.{SaltedJoinHint, SplitBroadcastHint, TargetedSaltHint}
    import org.apache.spark.sql.sparkx.DemoPlanRunner
    def countWith(hint: com.sparkx.autofix.Hint): String =
      try {
        val base = spark.sql(sql).queryExecution.analyzed
        val got  = DemoPlanRunner.run(spark, PlanHints.apply(base, Seq(hint))).collect()(0).getLong(0)
        if (got == expected) s"OK ($got)" else s"MISMATCH ($got vs $expected)"
      } catch { case e: Throwable => s"${e.getClass.getSimpleName}: ${e.getMessage}" }
    println(s"           inject SPLIT     : ${countWith(SplitBroadcastHint("d", 4))}")
    println(s"           inject SALT      : ${countWith(SaltedJoinHint("d", 16))}")
    println(s"           inject TGT_SALT  : ${countWith(TargetedSaltHint("d", 16, Seq("0")))}")

    // Prove targeted salting DISCOVERS the hot key by sampling the skewed side (95% share key 0).
    try {
      import org.apache.spark.sql.sparkx.SkewKeyDiscovery
      val skewed = spark.sql("SELECT k FROM skew_fact").queryExecution.analyzed
      val cfg = com.sparkx.SparkXConfig.fromConf(spark.sparkContext.getConf)
      val hot = SkewKeyDiscovery.discover(spark, skewed, skewed.output.head, cfg)
      println(s"           discovered hot   : ${hot.mkString(", ")} (expect 0)")
    } catch { case e: Throwable => println(s"           discovered hot   : ${e.getClass.getSimpleName}") }

    // Validate the SAME hints written directly in SQL text (SparkXHintRule resolves them).
    def countSql(q: String): String =
      try { val df = spark.sql(q); val got = df.collect()(0).getLong(0)
            val applied = PlanHints.hasInjected(df.queryExecution.analyzed)
            val note = if (applied) "hint applied" else "hint NOT applied!"
            if (got == expected) s"OK ($got) [$note]" else s"MISMATCH ($got vs $expected)" }
      catch { case e: Throwable => s"${e.getClass.getSimpleName}: ${e.getMessage}" }
    val base = "COUNT(*) FROM skew_fact f JOIN skew_dim d ON f.k = d.k"
    println(s"           SQL /*+ SPLIT_BROADCAST(d, 6) */ : ${countSql(s"SELECT /*+ SPLIT_BROADCAST(d, 6) */ $base")}")
    println(s"           SQL /*+ SALT(d, 16) */          : ${countSql(s"SELECT /*+ SALT(d, 16) */ $base")}")
    println(s"           SQL /*+ TARGETED_SALT(d,16,0) */: ${countSql(s"SELECT /*+ TARGETED_SALT(d, 16, 0) */ $base")}")

    // Prove the recommended DataFrame strategy is correctness-preserving: an AutoSaltJoin
    // over the same skewed join returns the same row count as the naive join.
    try {
      val naive  = fact.join(dim, Seq("k")).count()
      val salted = fact.autoSaltJoin(dim, Seq("k"),
        config = com.sparkx.join.AutoSaltJoinConfig(saltFactor = 16, skewedSide = com.sparkx.join.SkewedSide.Left))
        .count()
      val ok = if (naive == salted) "OK (row counts match)" else s"MISMATCH ($naive vs $salted)"
      println(s"           salt correctness : $ok  [naive=$naive, salted=$salted]")
    } catch {
      case e: Throwable => println(s"           salt check error : ${e.getClass.getSimpleName}: ${e.getMessage}")
    }
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
      val rows = df.collect() // executes df's own plan, so the plan the learner sees matches df.queryExecution
      val ms = (System.nanoTime() - start) / 1000000L

      // The learner runs asynchronously on the listener bus — give it a moment to persist.
      Thread.sleep(1200)

      val fp = try PlanHints.fingerprintOf(df.queryExecution.analyzed) catch { case _: Throwable => "" }
      val profile = store.load(fp)
      // Recover applied hints the same way the learner does: prefer the stamped identity tag
      // (skew rewrites can't be structurally reversed), falling back to structural strip.
      val applied = PlanHints.identityFrom(df.queryExecution.analyzed)
        .map(_._2)
        .getOrElse(PlanHints.strip(df.queryExecution.analyzed)._2)

      println(f"  Run $run%d  (${ms}%d ms)  plan: ${planNote(df)}")
      println(s"           result           : ${resultCell(rows)}")
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

  /** Summarise a single-cell result (e.g. COUNT(*)) so correctness across runs is visible. */
  private def resultCell(rows: Array[org.apache.spark.sql.Row]): String =
    try if (rows.length == 1 && rows(0).length == 1) rows(0).get(0).toString
        else s"${rows.length} row(s)"
    catch { case _: Throwable => "n/a" }

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
