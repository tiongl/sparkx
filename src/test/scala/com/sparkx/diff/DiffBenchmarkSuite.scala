package com.sparkx.diff

import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Benchmark comparing three diff approaches at 10 K – 2 M rows:
 *   - '''NaiveDiff'''  – plain anti-join + inner join (baseline)
 *   - '''BloomDiff'''   – Bloom-filter pre-filter then join
 *   - '''MD5Diff'''     – MD5 hash pre-filter then exact verify
 *
 * Captures shuffle write bytes via a SparkListener and projects
 * distributed runtime at simulated network bandwidths (1 Gbps / 10 Gbps).
 *
 * Run with:  sbt "testOnly com.sparkx.diff.DiffBenchmarkSuite"
 */
class DiffBenchmarkSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _
  @transient private var shuffleTracker: ShuffleTracker = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("DiffBenchmark")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()
    shuffleTracker = new ShuffleTracker()
    spark.sparkContext.addSparkListener(shuffleTracker)
  }

  override def afterAll(): Unit = {
    // Don't stop SparkSession — other suites in the same JVM may need it.
    super.afterAll()
  }

  // ── Shuffle tracking ────────────────────────────────────────────────────

  /** Captures shuffle write bytes across all tasks. */
  private class ShuffleTracker extends SparkListener {
    val shuffleWriteBytes = new AtomicLong(0L)

    def reset(): Unit = shuffleWriteBytes.set(0L)

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        shuffleWriteBytes.addAndGet(metrics.shuffleWriteMetrics.bytesWritten)
      }
    }
  }

  private def drainListenerBus(): Unit = {
    // Listener events are dispatched asynchronously; a short pause ensures
    // onTaskEnd callbacks have been processed before we read shuffle bytes.
    Thread.sleep(200)
  }

  // ── Data generator (Spark-native, no driver collection) ───────────────

  private def generateDatasets(size: Long, numPartitions: Int = 0,
                               changePct: Double = 0.05,
                               addPct: Double = 0.02, removePct: Double = 0.02)
  : (DataFrame, DataFrame) = {
    val changeCount = if (changePct == 0.0) 0L else math.max((size * changePct).toLong, 1L)
    val addCount    = if (addPct == 0.0)    0L else math.max((size * addPct).toLong, 1L)
    val removeCount = if (removePct == 0.0) 0L else math.max((size * removePct).toLong, 1L)

    // Left: ids 1..size — 20 columns wide
    var leftRaw = spark.range(1, size + 1).toDF("id")
      .withColumn("name",     concat(lit("name_"), col("id")))
      .withColumn("value",    col("id").cast("double"))
      .withColumn("category", concat(lit("cat_"), col("id") % 10))
    for (i <- 1 to 16) {
      leftRaw = leftRaw.withColumn(s"attr_$i",
        concat(lit(s"v${i}_"), (col("id") % (i + 5)).cast("string")))
    }

    // Right: remove first `removeCount` ids, change next `changeCount`,
    //        add `addCount` new ids at the end.  20 columns wide.
    var rightRaw = spark.range(1, size + addCount + 1).toDF("id")
      .filter(col("id") > removeCount)   // drop first removeCount
      .withColumn("name", concat(lit("name_"), col("id")))
      .withColumn("value",
        when(col("id") > removeCount && col("id") <= removeCount + changeCount,
          col("id").cast("double") + 999.0)
        .otherwise(col("id").cast("double")))
      .withColumn("category", concat(lit("cat_"), col("id") % 10))
    for (i <- 1 to 16) {
      rightRaw = rightRaw.withColumn(s"attr_$i",
        concat(lit(s"v${i}_"), (col("id") % (i + 5)).cast("string")))
    }

    // Optionally repartition by key for co-partitioned datasets
    val (left, right) = if (numPartitions > 0) {
      (leftRaw.repartition(numPartitions, col("id")),
       rightRaw.repartition(numPartitions, col("id")))
    } else {
      (leftRaw, rightRaw)
    }

    // Cache both to isolate diff timing from data generation
    left.cache().count()
    right.cache().count()
    (left, right)
  }

  // ── Benchmark result ──────────────────────────────────────────────────

  private case class BenchmarkResult(
      strategy: String,
      datasetSize: Long,
      addedCount: Long,
      removedCount: Long,
      changedCount: Long,
      durationMs: Long,
      shuffleBytes: Long
  )

  // ── Strategy runners ──────────────────────────────────────────────────

  private def runStrategy(strategy: DiffStrategy, name: String,
                          left: DataFrame, right: DataFrame,
                          size: Long): BenchmarkResult = {
    val config = DiffConfig(keyColumns = Seq("id"), limit = 50)

    // Warm-up
    strategy.diff(left, right, config)
    drainListenerBus()

    // Timed run with shuffle tracking
    shuffleTracker.reset()
    val start  = System.nanoTime()
    val result = strategy.diff(left, right, config)
    val ms     = (System.nanoTime() - start) / 1000000
    drainListenerBus()
    val shuffle = shuffleTracker.shuffleWriteBytes.get()

    BenchmarkResult(name, size,
      result.summary.addedCount, result.summary.removedCount,
      result.summary.changedCount, ms, shuffle)
  }

  /** Baseline: plain equi-join diff with no Bloom / MD5 acceleration. */
  private def runNaive(left: DataFrame, right: DataFrame,
                       size: Long): BenchmarkResult = {
    val keys = Seq("id")
    val diffCols = left.columns.filterNot(keys.contains).toSeq

    // Warm-up (materialise to avoid cold-path bias vs strategies)
    val (wA, wR, wC) = naiveDiff(left, right, keys, diffCols)
    wA.count(); wR.count(); wC.count()
    drainListenerBus()

    // Timed run with shuffle tracking
    shuffleTracker.reset()
    val start = System.nanoTime()
    val (addedDf, removedDf, changedDf) = naiveDiff(left, right, keys, diffCols)
    val addedCount   = addedDf.count()
    val removedCount = removedDf.count()
    val changedCount = changedDf.count()
    val ms = (System.nanoTime() - start) / 1000000
    drainListenerBus()
    val shuffle = shuffleTracker.shuffleWriteBytes.get()

    BenchmarkResult("NaiveDiff", size, addedCount, removedCount, changedCount, ms, shuffle)
  }

  /** Plain join diff — the simplest possible approach (our baseline). */
  private def naiveDiff(left: DataFrame, right: DataFrame,
                        keys: Seq[String], diffCols: Seq[String])
  : (DataFrame, DataFrame, DataFrame) = {
    val removed = left.join(right, keys, "left_anti")
    val added   = right.join(left, keys, "left_anti")

    val joinCond = keys.map(k => left(k) === right(k)).reduce(_ && _)
    val joined = left.alias("l").join(right.alias("r"), joinCond, "inner")

    val diffCondition = diffCols
      .map(c => org.apache.spark.sql.functions.not(col(s"l.$c") <=> col(s"r.$c")))
      .reduce(_ || _)

    val changed = joined.filter(diffCondition)
      .select(keys.map(k => col(s"l.$k").as(k)): _*)

    (added, removed, changed)
  }

  // ── Formatting helpers ────────────────────────────────────────────────

  // 1 Gbps = 125 MB/s = 125000 bytes/ms
  private val bytesPerMs1G  = 125000L
  // 10 Gbps = 1250 MB/s = 1250000 bytes/ms
  private val bytesPerMs10G = 1250000L

  private def projectedMs(localMs: Long, shuffleBytes: Long, bw: Long): Long =
    localMs + shuffleBytes / bw

  private def speedup(baseline: Long, actual: Long): String =
    if (actual > 0) f"${baseline.toDouble / actual}%.2fx" else "N/A"

  private def mbStr(bytes: Long): String = f"${bytes / 1048576.0}%.1f"

  private val scaleHeader = {
    f"${"Strategy"}%-12s | ${"Rows"}%10s | ${"Local(ms)"}%10s | ${"Shuffle(MB)"}%12s | ${"@1Gbps(ms)"}%11s | ${"@10Gbps(ms)"}%12s | ${"vs Naive @1G"}%14s"
  }

  private def printHeader(subtitle: String = ""): Unit = {
    println()
    println("═" * scaleHeader.length)
    val title = if (subtitle.isEmpty) "SparkX Diff Strategy Benchmark"
                else s"SparkX Diff Strategy Benchmark — $subtitle"
    println(s"  $title")
    println(s"  (Projected = local compute + shuffle bytes / bandwidth)")
    println("═" * scaleHeader.length)
    println(scaleHeader)
    println("─" * scaleHeader.length)
  }

  private def printResultsForSize(results: Seq[BenchmarkResult]): Unit = {
    val naive = results.find(_.strategy == "NaiveDiff").get
    val naiveProj = projectedMs(naive.durationMs, naive.shuffleBytes, bytesPerMs1G)
    results.foreach { r =>
      val proj1G  = projectedMs(r.durationMs, r.shuffleBytes, bytesPerMs1G)
      val proj10G = projectedMs(r.durationMs, r.shuffleBytes, bytesPerMs10G)
      val spd = if (r.strategy == "NaiveDiff") "baseline" else speedup(naiveProj, proj1G)
      println(f"${r.strategy}%-12s | ${r.datasetSize}%10d | ${r.durationMs}%10d | ${mbStr(r.shuffleBytes)}%12s | ${proj1G}%11d | ${proj10G}%12d | ${spd}%14s")
    }
  }

  // ── The benchmarks ─────────────────────────────────────────────────────

  test("benchmark: NaiveDiff vs BloomDiff vs MD5Diff at 10K / 100K / 1M rows") {
    val bloom = new BloomDiffStrategy(expectedNumItems = 2200000L, fpp = 0.01)
    val md5   = new MD5DiffStrategy()
    val sizes = Seq(10000L, 100000L, 1000000L, 2000000L)
    val partitions = 0  // unpartitioned — forces shuffle for all strategies

    printHeader("Unpartitioned (shuffle required)")

    val results = sizes.flatMap { size =>
      val (left, right) = generateDatasets(size, numPartitions = partitions)

      val naiveResult = runNaive(left, right, size)
      val bloomResult = runStrategy(bloom, "BloomDiff", left, right, size)
      val md5Result   = runStrategy(md5, "MD5Diff", left, right, size)

      // Verify all three agree on counts
      bloomResult.addedCount   shouldBe naiveResult.addedCount
      bloomResult.removedCount shouldBe naiveResult.removedCount
      bloomResult.changedCount shouldBe naiveResult.changedCount
      md5Result.addedCount     shouldBe naiveResult.addedCount
      md5Result.removedCount   shouldBe naiveResult.removedCount
      md5Result.changedCount   shouldBe naiveResult.changedCount

      val sizeResults = Seq(naiveResult, bloomResult, md5Result)
      printResultsForSize(sizeResults)
      println("─" * scaleHeader.length)

      left.unpersist()
      right.unpersist()

      sizeResults
    }

    println("═" * scaleHeader.length)
    println()
  }

  test("benchmark: partitioned vs unpartitioned at 100K rows") {
    val bloom = new BloomDiffStrategy(expectedNumItems = 110000L, fpp = 0.01)
    val md5   = new MD5DiffStrategy()
    val size  = 100000L

    val partHdr = f"${"Strategy"}%-12s | ${"Layout"}%-15s | ${"Local(ms)"}%10s | ${"Shuffle(MB)"}%12s | ${"@1Gbps(ms)"}%11s | ${"vs Naive @1G"}%14s"
    println()
    println("═" * partHdr.length)
    println("  Partitioned vs Unpartitioned (100K rows)")
    println(s"  (Projected = local compute + shuffle bytes / bandwidth)")
    println("═" * partHdr.length)

    // Unpartitioned (default spark.range partitioning)
    val (leftUn, rightUn) = generateDatasets(size, numPartitions = 0)
    val naiveUn = runNaive(leftUn, rightUn, size)
    val bloomUn = runStrategy(bloom, "BloomDiff", leftUn, rightUn, size)
    val md5Un   = runStrategy(md5, "MD5Diff", leftUn, rightUn, size)
    leftUn.unpersist(); rightUn.unpersist()

    // Repartitioned by key (8 partitions)
    val (leftPart, rightPart) = generateDatasets(size, numPartitions = 8)
    val naivePart = runNaive(leftPart, rightPart, size)
    val bloomPart = runStrategy(bloom, "BloomDiff", leftPart, rightPart, size)
    val md5Part   = runStrategy(md5, "MD5Diff", leftPart, rightPart, size)
    leftPart.unpersist(); rightPart.unpersist()

    println(partHdr)
    println("─" * partHdr.length)

    // Group by layout, show speedup vs NaiveDiff within each layout
    Seq(
      ("Unpartitioned", naiveUn, bloomUn, md5Un),
      ("Partitioned(8)", naivePart, bloomPart, md5Part)
    ).foreach { case (layout, naive, bloom2, md52) =>
      val baseProj = projectedMs(naive.durationMs, naive.shuffleBytes, bytesPerMs1G)
      Seq(("NaiveDiff", naive), ("BloomDiff", bloom2), ("MD5Diff", md52)).foreach { case (name, r) =>
        val proj1G = projectedMs(r.durationMs, r.shuffleBytes, bytesPerMs1G)
        val spd = if (name == "NaiveDiff") "baseline" else speedup(baseProj, proj1G)
        println(f"${name}%-12s | ${layout}%-15s | ${r.durationMs}%10d | ${mbStr(r.shuffleBytes)}%12s | ${proj1G}%11d | ${spd}%14s")
      }
      println("─" * partHdr.length)
    }
    println("═" * partHdr.length)
    println()
  }

  test("benchmark: low-diff (5%) vs high-diff (50%) at 500K rows") {
    val bloom = new BloomDiffStrategy(expectedNumItems = 600000L, fpp = 0.01)
    val md5   = new MD5DiffStrategy()
    val size  = 500000L

    val diffHdr = f"${"Strategy"}%-12s | ${"Diff%"}%-10s | ${"Local(ms)"}%10s | ${"Shuffle(MB)"}%12s | ${"@1Gbps(ms)"}%11s | ${"vs Naive @1G"}%14s"
    println()
    println("═" * diffHdr.length)
    println("  Low-diff vs High-diff (500K rows, 20 columns)")
    println(s"  (Projected = local compute + shuffle bytes / bandwidth)")
    println("═" * diffHdr.length)
    println(diffHdr)
    println("─" * diffHdr.length)

    Seq(
      ("0% diff",  0.0,  0.0,  0.0),
      ("5% diff",  0.05, 0.02, 0.02),
      ("30% diff", 0.30, 0.10, 0.10),
      ("50% diff", 0.50, 0.15, 0.15)
    ).foreach { case (label, chg, add, rem) =>
      val (left, right) = generateDatasets(size, numPartitions = 0,
        changePct = chg, addPct = add, removePct = rem)

      val naiveResult = runNaive(left, right, size)
      val bloomResult = runStrategy(bloom, "BloomDiff", left, right, size)
      val md5Result   = runStrategy(md5, "MD5Diff", left, right, size)

      // Verify counts agree
      bloomResult.addedCount   shouldBe naiveResult.addedCount
      bloomResult.removedCount shouldBe naiveResult.removedCount
      bloomResult.changedCount shouldBe naiveResult.changedCount
      md5Result.addedCount     shouldBe naiveResult.addedCount
      md5Result.removedCount   shouldBe naiveResult.removedCount
      md5Result.changedCount   shouldBe naiveResult.changedCount

      val baseProj = projectedMs(naiveResult.durationMs, naiveResult.shuffleBytes, bytesPerMs1G)
      Seq(("NaiveDiff", naiveResult), ("BloomDiff", bloomResult), ("MD5Diff", md5Result)).foreach {
        case (name, r) =>
          val proj1G = projectedMs(r.durationMs, r.shuffleBytes, bytesPerMs1G)
          val spd = if (name == "NaiveDiff") "baseline" else speedup(baseProj, proj1G)
          println(f"${name}%-12s | ${label}%-10s | ${r.durationMs}%10d | ${mbStr(r.shuffleBytes)}%12s | ${proj1G}%11d | ${spd}%14s")
      }
      println("─" * diffHdr.length)

      left.unpersist()
      right.unpersist()
    }
    println("═" * diffHdr.length)
    println()
  }
}
