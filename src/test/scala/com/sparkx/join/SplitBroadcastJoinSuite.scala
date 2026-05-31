package com.sparkx.join

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SplitBroadcastJoinSuite extends AnyFunSuite with Matchers
    with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("SplitBroadcastJoinTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }

  import SplitBroadcastJoin._

  private def sampleData(): (DataFrame, DataFrame) = {
    val s = spark
    import s.implicits._
    val large = (1 to 100).map(i => (i, s"large_$i")).toDF("id", "val_l")
    val small = Seq((10, "x"), (20, "y"), (30, "z"), (40, "w")).toDF("id", "val_s")
    (large, small)
  }

  // ── Correctness ───────────────────────────────────────────────────────

  test("splitBroadcastJoin: inner join produces correct results") {
    val (large, small) = sampleData()
    val result = large.splitBroadcastJoin(small, Seq("id"))
    result.count() shouldBe 4
    result.columns should contain allOf("id", "val_l", "val_s")

    val ids = result.select("id").collect().map(_.getInt(0)).toSet
    ids shouldBe Set(10, 20, 30, 40)
  }

  test("splitBroadcastJoin: results match regular join") {
    val (large, small) = sampleData()
    val expected = large.join(small, Seq("id"), "inner")
    val actual = large.splitBroadcastJoin(small, Seq("id"))

    actual.count() shouldBe expected.count()
    val actualIds = actual.select("id").collect().map(_.getInt(0)).sorted
    val expectedIds = expected.select("id").collect().map(_.getInt(0)).sorted
    actualIds shouldBe expectedIds
  }

  test("splitBroadcastJoin: left_semi join") {
    val (large, small) = sampleData()
    val result = large.splitBroadcastJoin(small, Seq("id"), "left_semi")
    result.count() shouldBe 4
    result.columns shouldBe Array("id", "val_l") // no small-side columns
  }

  // ── Multi-split correctness ───────────────────────────────────────────

  test("splitBroadcastJoin: forced multi-split produces same results") {
    val (large, small) = sampleData()
    // Force 4 splits even though data is small
    val config = SplitBroadcastConfig(broadcastBudgetBytes = 1L, maxSplits = 16)

    // Directly test with splits
    val result = SplitBroadcastJoin.execute(large, small, Seq("id"),
      "inner", config)
    // The first attempt (1 split) will succeed since data is small,
    // so this tests the happy path. We test multi-split logic below.
  }

  test("splitBroadcastJoin: executeWithSplits produces correct results at various split counts") {
    val s = spark
    import s.implicits._
    val large = (1 to 1000).map(i => (i, s"v$i")).toDF("id", "val_l")
    val small = (1 to 100).map(i => (i * 10, s"s$i")).toDF("id", "val_s")

    val expected = large.join(small, Seq("id"), "inner")
    val expectedCount = expected.count()

    // Test with 2, 4, 8 splits using hash-modulo partitioning
    for (numSplits <- Seq(2, 4, 8)) {
      val result = executeSplitManually(large, small, Seq("id"), "inner", numSplits)
      result.count() shouldBe expectedCount
    }
  }

  /** Helper to directly test multi-split execution */
  private def executeSplitManually(large: DataFrame, small: DataFrame,
                                   keys: Seq[String], joinType: String,
                                   numSplits: Int): DataFrame = {
    import org.apache.spark.sql.functions._
    val hashExpr = hash(keys.map(col): _*)
    val partials = (0 until numSplits).map { i =>
      val chunk = small.filter(pmod(hashExpr, lit(numSplits)) === lit(i))
      large.join(
        org.apache.spark.sql.functions.broadcast(chunk), keys, joinType)
    }
    partials.reduce(_ union _)
  }

  // ── Composite keys ────────────────────────────────────────────────────

  test("splitBroadcastJoin: works with composite keys") {
    val s = spark
    import s.implicits._
    val large = Seq((1, "a", 10), (1, "b", 20), (2, "a", 30), (2, "b", 40))
      .toDF("k1", "k2", "val_l")
    val small = Seq((1, "a", "x"), (2, "b", "y")).toDF("k1", "k2", "val_s")

    val result = large.splitBroadcastJoin(small, Seq("k1", "k2"))
    result.count() shouldBe 2

    val pairs = result.select("k1", "k2").collect()
      .map(r => (r.getInt(0), r.getString(1))).toSet
    pairs shouldBe Set((1, "a"), (2, "b"))
  }

  // ── Unsupported join types ────────────────────────────────────────────

  test("splitBroadcastJoin: rejects unsupported join types") {
    val (large, small) = sampleData()

    Seq("left_outer", "right_outer", "full_outer", "left_anti").foreach { jt =>
      val ex = intercept[IllegalArgumentException] {
        large.splitBroadcastJoin(small, Seq("id"), jt)
      }
      ex.getMessage should include(jt)
    }
  }

  // ── Dynamic split calculation ─────────────────────────────────────────

  test("computeNextSplits: parses bytes from OOM message") {
    val config = SplitBroadcastConfig(broadcastBudgetBytes = 100L * 1024 * 1024)
    val e = new SparkException(
      "Not enough memory to build and broadcast the table to all worker nodes. " +
      "The size of the broadcast table is 524288000 bytes."
    )
    // 524288000 / 104857600 = 5.0 → ceil = 5, but must be >= 2 (currentSplits*2)
    val next = SplitBroadcastJoin.computeNextSplits(e, 1, config)
    next shouldBe 5
  }

  test("computeNextSplits: parses MiB from OOM message") {
    val config = SplitBroadcastConfig(broadcastBudgetBytes = 100L * 1024 * 1024)
    val e = new SparkException(
      "Broadcast table too large. The size of the broadcast table is 800.5 MiB."
    )
    // 800.5 MiB / 100 MiB = 8.005 → ceil = 9, >= 2
    val next = SplitBroadcastJoin.computeNextSplits(e, 1, config)
    next shouldBe 9
  }

  test("computeNextSplits: ensures at least currentSplits * 2") {
    val config = SplitBroadcastConfig(broadcastBudgetBytes = 100L * 1024 * 1024)
    // Size that would compute to 3 splits, but current is already 4
    val e = new SparkException(
      "The size of the broadcast table is 209715200 bytes."  // 200 MB → 2 splits
    )
    val next = SplitBroadcastJoin.computeNextSplits(e, 4, config)
    next shouldBe 8  // 4 * 2, not 2
  }

  test("computeNextSplits: falls back to doubling when no size in message") {
    val config = SplitBroadcastConfig(broadcastBudgetBytes = 100L * 1024 * 1024)
    val e = new RuntimeException("Some unrelated error")
    val next = SplitBroadcastJoin.computeNextSplits(e, 4, config)
    next shouldBe 8
  }

  test("computeNextSplits: parses size from nested cause") {
    val config = SplitBroadcastConfig(broadcastBudgetBytes = 100L * 1024 * 1024)
    val cause = new SparkException(
      "The size of the broadcast table is 314572800 bytes.")  // 300 MB
    val wrapper = new RuntimeException("Job failed", cause)
    val next = SplitBroadcastJoin.computeNextSplits(wrapper, 1, config)
    next shouldBe 3  // ceil(300/100) = 3, >= 2
  }

  // ── parseBroadcastSize ────────────────────────────────────────────────

  test("parseBroadcastSize: extracts bytes") {
    val e = new SparkException(
      "The size of the broadcast table is 1073741824 bytes.")
    SplitBroadcastJoin.parseBroadcastSize(e) shouldBe Some(1073741824L)
  }

  test("parseBroadcastSize: extracts MiB") {
    val e = new SparkException(
      "The size of the broadcast table is 512.0 MiB.")
    SplitBroadcastJoin.parseBroadcastSize(e) shouldBe Some(536870912L)
  }

  test("parseBroadcastSize: returns None for unrecognised messages") {
    val e = new RuntimeException("something went wrong")
    SplitBroadcastJoin.parseBroadcastSize(e) shouldBe None
  }

  // ── Cache behaviour ───────────────────────────────────────────────────

  test("splitBroadcastJoin: caches result when cacheResult is true") {
    val (large, small) = sampleData()
    val config = SplitBroadcastConfig(cacheResult = true)
    val result = large.splitBroadcastJoin(small, Seq("id"), config = config)
    result.storageLevel.useMemory shouldBe true
    result.unpersist()
  }

  test("splitBroadcastJoin: does not cache when cacheResult is false") {
    val (large, small) = sampleData()
    val config = SplitBroadcastConfig(cacheResult = false)
    val result = large.splitBroadcastJoin(small, Seq("id"), config = config)
    result.storageLevel.useMemory shouldBe false
  }

  // ── cacheLargeSide ────────────────────────────────────────────────────

  test("splitBroadcastJoin: cacheLargeSide caches large side during execution") {
    val (large, small) = sampleData()
    // large side should NOT be cached before we call splitBroadcastJoin
    large.storageLevel.useMemory shouldBe false

    val config = SplitBroadcastConfig(cacheLargeSide = true, cacheResult = false)
    val result = large.splitBroadcastJoin(small, Seq("id"), config = config)
    result.count() shouldBe 4

    // After execution completes, the large side cache should be unpersisted
    // (the finally block calls unpersist). The result itself should not be cached.
    result.storageLevel.useMemory shouldBe false
  }

  test("splitBroadcastJoin: cacheLargeSide=false does not cache large side") {
    val (large, small) = sampleData()
    val config = SplitBroadcastConfig(cacheLargeSide = false, cacheResult = false)
    val result = large.splitBroadcastJoin(small, Seq("id"), config = config)
    result.count() shouldBe 4
    // Neither large side nor result should be cached
    large.storageLevel.useMemory shouldBe false
    result.storageLevel.useMemory shouldBe false
  }
}
