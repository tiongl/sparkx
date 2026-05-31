package com.sparkx.join

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AutoSaltJoinSuite extends AnyFunSuite with Matchers
    with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("AutoSaltJoinTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    // Ensure engine-level resilient join doesn't interfere with these tests
    spark.conf.set("spark.sparkx.resilientJoin.enabled", "false")
  }

  import AutoSaltJoin._

  /**
   * Build a skewed left dataset: key=1 appears 500 times (hot),
   * keys 2-50 appear once each (cold).
   */
  private def skewedData(): (DataFrame, DataFrame) = {
    val s = spark
    import s.implicits._
    val hotRows = (1 to 500).map(i => (1, s"hot_$i"))
    val coldRows = (2 to 50).map(i => (i, s"cold_$i"))
    val left = (hotRows ++ coldRows).toDF("id", "val_l")

    val right = (1 to 60).map(i => (i, s"right_$i")).toDF("id", "val_r")
    (left, right)
  }

  /** Uniform (non-skewed) data. */
  private def uniformData(): (DataFrame, DataFrame) = {
    val s = spark
    import s.implicits._
    val left = (1 to 100).map(i => (i, s"left_$i")).toDF("id", "val_l")
    val right = (1 to 100).map(i => (i, s"right_$i")).toDF("id", "val_r")
    (left, right)
  }

  // ── Correctness ───────────────────────────────────────────────────────

  test("autoSaltJoin: inner join produces correct results on skewed data") {
    val (left, right) = skewedData()
    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,  // full scan for deterministic detection
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 10,
      saltFactor = 5,
      cacheResult = false
    )

    val result = left.autoSaltJoin(right, Seq("id"), config = config)

    // Verify salt column is removed
    result.columns should not contain "__sparkx_auto_salt"

    // Expected: 500 rows for id=1 (hot) + 49 rows for ids 2-50 (cold) = 549
    // But right side only has ids 1-60, and left has ids 1-50
    // id=1: 500 left rows × 1 right row = 500 matches
    // ids 2-50: 1 left row × 1 right row each = 49 matches
    result.count() shouldBe 549

    // Verify id=1 has exactly 500 rows
    result.filter("id = 1").count() shouldBe 500
  }

  test("autoSaltJoin: results match regular join") {
    val (left, right) = skewedData()
    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 10,
      saltFactor = 5,
      cacheResult = false
    )

    val expected = left.join(right, Seq("id"), "inner")
    val actual = left.autoSaltJoin(right, Seq("id"), config = config)

    actual.count() shouldBe expected.count()

    // Verify per-key counts match
    import org.apache.spark.sql.functions._
    val expectedCounts = expected.groupBy("id").count()
      .collect().map(r => (r.getInt(0), r.getLong(1))).toMap
    val actualCounts = actual.groupBy("id").count()
      .collect().map(r => (r.getInt(0), r.getLong(1))).toMap

    actualCounts shouldBe expectedCounts
  }

  // ── No skew passthrough ───────────────────────────────────────────────

  test("autoSaltJoin: falls through to regular join when no skew detected") {
    val (left, right) = uniformData()
    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 10.0,
      minHotKeyCount = 100,  // high threshold → no hot keys in uniform data
      cacheResult = false
    )

    val result = left.autoSaltJoin(right, Seq("id"), config = config)
    result.count() shouldBe 100
    result.columns should not contain "__sparkx_auto_salt"
  }

  // ── Composite keys ────────────────────────────────────────────────────

  test("autoSaltJoin: works with composite keys") {
    val s = spark
    import s.implicits._

    // Skew on composite key (1, "a")
    val hotRows = (1 to 200).map(i => (1, "a", s"hot_$i"))
    val coldRows = Seq((1, "b", "cold1"), (2, "a", "cold2"), (2, "b", "cold3"))
    val left = (hotRows ++ coldRows).toDF("k1", "k2", "val_l")

    val right = Seq(
      (1, "a", "ra"), (1, "b", "rb"), (2, "a", "rc"), (2, "b", "rd")
    ).toDF("k1", "k2", "val_r")

    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 10,
      saltFactor = 4,
      cacheResult = false
    )

    val result = left.autoSaltJoin(right, Seq("k1", "k2"), config = config)
    val expected = left.join(right, Seq("k1", "k2"), "inner")

    result.count() shouldBe expected.count()
    result.columns should not contain "__sparkx_auto_salt"
  }

  // ── Outer joins ───────────────────────────────────────────────────────

  test("autoSaltJoin: left_outer join preserves non-matching left rows") {
    val s = spark
    import s.implicits._

    val hotRows = (1 to 100).map(i => (1, s"hot_$i"))
    val coldRows = Seq((2, "cold"), (3, "unmatched"))
    val left = (hotRows ++ coldRows).toDF("id", "val_l")

    val right = Seq((1, "r1"), (2, "r2")).toDF("id", "val_r")

    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 5,
      saltFactor = 4,
      cacheResult = false,
      skewedSide = SkewedSide.Left
    )

    val result = left.autoSaltJoin(right, Seq("id"), "left_outer", config)
    val expected = left.join(right, Seq("id"), "left_outer")

    result.count() shouldBe expected.count()

    // id=3 has no match on right → val_r should be null
    result.filter("id = 3").select("val_r").collect()(0).isNullAt(0) shouldBe true
  }

  test("autoSaltJoin: right_outer join preserves non-matching right rows") {
    val s = spark
    import s.implicits._

    val left = Seq((1, "l1"), (2, "l2")).toDF("id", "val_l")

    val hotRows = (1 to 100).map(i => (1, s"hot_$i"))
    val coldRows = Seq((2, "cold"), (3, "unmatched"))
    val right = (hotRows ++ coldRows).toDF("id", "val_r")

    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 5,
      saltFactor = 4,
      cacheResult = false,
      skewedSide = SkewedSide.Right
    )

    val result = left.autoSaltJoin(right, Seq("id"), "right_outer", config)
    val expected = left.join(right, Seq("id"), "right_outer")

    result.count() shouldBe expected.count()

    // id=3 has no match on left → val_l should be null
    result.filter("id = 3").select("val_l").collect()(0).isNullAt(0) shouldBe true
  }

  // ── Unsupported join types ────────────────────────────────────────────

  test("autoSaltJoin: rejects unsupported join types") {
    val (left, right) = uniformData()
    Seq("left_semi", "left_anti", "full_outer", "cross").foreach { jt =>
      val ex = intercept[IllegalArgumentException] {
        left.autoSaltJoin(right, Seq("id"), jt)
      }
      ex.getMessage should include(jt)
    }
  }

  // ── Skew detection ────────────────────────────────────────────────────

  test("findHotKeys: identifies hot keys in skewed data") {
    val (left, _) = skewedData()
    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 10
    )

    val hotKeys = AutoSaltJoin.findHotKeys(left, Seq("id"), config)
    val hotIds = hotKeys.collect().map(_.getInt(0)).toSet

    hotIds should contain(1)
    // Cold keys (appearing once each) should not be hot
    hotIds should not contain 2
  }

  test("findHotKeys: returns empty for uniform data") {
    val (left, _) = uniformData()
    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 10.0,
      minHotKeyCount = 100
    )

    val hotKeys = AutoSaltJoin.findHotKeys(left, Seq("id"), config)
    hotKeys.isEmpty shouldBe true
  }

  test("detectSkew: auto-detects the skewed side") {
    val s = spark
    import s.implicits._

    // Left has skew, right is uniform
    val hotRows = (1 to 500).map(i => (1, s"h$i"))
    val coldRows = (2 to 50).map(i => (i, s"c$i"))
    val left = (hotRows ++ coldRows).toDF("id", "val")
    val right = (1 to 60).map(i => (i, s"r$i")).toDF("id", "val")

    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 10
    )

    val (hotKeys, side) = AutoSaltJoin.detectSkew(left, right, Seq("id"), config)
    side shouldBe "left"
    hotKeys.isEmpty shouldBe false
  }

  // ── Forced side ───────────────────────────────────────────────────────

  test("autoSaltJoin: respects SkewedSide.Left") {
    val (left, right) = skewedData()
    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 10,
      saltFactor = 4,
      skewedSide = SkewedSide.Left,
      cacheResult = false
    )

    val result = left.autoSaltJoin(right, Seq("id"), config = config)
    val expected = left.join(right, Seq("id"), "inner")
    result.count() shouldBe expected.count()
  }

  test("autoSaltJoin: respects SkewedSide.Right") {
    val (left, right) = skewedData()
    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 10,
      saltFactor = 4,
      skewedSide = SkewedSide.Right,
      cacheResult = false
    )

    // Right side has uniform data (1-60 each once), so no hot keys →
    // should fall through to regular join
    val result = left.autoSaltJoin(right, Seq("id"), config = config)
    val expected = left.join(right, Seq("id"), "inner")
    result.count() shouldBe expected.count()
  }

  // ── Null keys ─────────────────────────────────────────────────────────

  test("autoSaltJoin: handles null keys correctly") {
    val s = spark
    import s.implicits._

    val hotRows = (1 to 100).map(i => (Some(1), s"hot_$i"))
    val nullRows = Seq((None: Option[Int], "null_left"))
    val left = (hotRows ++ nullRows).toDF("id", "val_l")

    val right = Seq((Some(1), "r1"), (Some(2), "r2"),
      (None: Option[Int], "null_right")).toDF("id", "val_r")

    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 5,
      saltFactor = 4,
      cacheResult = false
    )

    // Inner join: null keys should not match (Spark default equi-join)
    val result = left.autoSaltJoin(right, Seq("id"), config = config)
    val expected = left.join(right, Seq("id"), "inner")
    result.count() shouldBe expected.count()
  }

  // ── Cache behaviour ───────────────────────────────────────────────────

  test("autoSaltJoin: caches result when cacheResult is true") {
    val (left, right) = skewedData()
    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 10,
      saltFactor = 4,
      cacheResult = true
    )
    val result = left.autoSaltJoin(right, Seq("id"), config = config)
    result.storageLevel.useMemory shouldBe true
    result.unpersist()
  }

  test("autoSaltJoin: does not cache when cacheResult is false") {
    val (left, right) = skewedData()
    val config = AutoSaltJoinConfig(
      sampleFraction = 1.0,
      skewThresholdMultiplier = 5.0,
      minHotKeyCount = 10,
      saltFactor = 4,
      cacheResult = false
    )
    val result = left.autoSaltJoin(right, Seq("id"), config = config)
    result.storageLevel.useMemory shouldBe false
  }

  // ── Config validation ─────────────────────────────────────────────────

  test("AutoSaltJoinConfig: rejects invalid parameters") {
    intercept[IllegalArgumentException] {
      AutoSaltJoinConfig(sampleFraction = 0.0)
    }
    intercept[IllegalArgumentException] {
      AutoSaltJoinConfig(skewThresholdMultiplier = 0.5)
    }
    intercept[IllegalArgumentException] {
      AutoSaltJoinConfig(saltFactor = 1)
    }
  }
}
