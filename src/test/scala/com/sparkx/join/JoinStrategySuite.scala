package com.sparkx.join

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for individual [[JoinStrategy]] implementations.
 */
class JoinStrategySuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("JoinStrategyTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }

  private def sampleData(): (DataFrame, DataFrame) = {
    val s = spark
    import s.implicits._
    val left = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "val_l")
    val right = Seq((2, "x"), (3, "y"), (4, "z")).toDF("id", "val_r")
    (left, right)
  }

  // ── BroadcastJoinStrategy ─────────────────────────────────────────────

  test("BroadcastJoinStrategy: produces correct inner join") {
    val (left, right) = sampleData()
    val strategy = new BroadcastJoinStrategy()
    val result = strategy.join(left, right, Seq("id"), "inner")
    result.count() shouldBe 2
    result.columns should contain allOf("id", "val_l", "val_r")
  }

  test("BroadcastJoinStrategy: canHandle respects threshold") {
    val (left, right) = sampleData()
    // Very small threshold — should still handle since data is tiny
    val small = new BroadcastJoinStrategy(thresholdBytes = Long.MaxValue)
    small.canHandle(left, right, None) shouldBe true

    // Threshold of 0 bytes — nothing can be broadcast
    val zero = new BroadcastJoinStrategy(thresholdBytes = 0L)
    zero.canHandle(left, right, None) shouldBe false
  }

  test("BroadcastJoinStrategy: declines after OOM failure context") {
    val (left, right) = sampleData()
    val strategy = new BroadcastJoinStrategy()
    val oomCtx = Some(JoinFailureContext(
      failedStrategy = "broadcast",
      exception = new OutOfMemoryError("Java heap space"),
      attempt = 1
    ))
    strategy.canHandle(left, right, oomCtx) shouldBe false
  }

  test("BroadcastJoinStrategy: accepts after non-OOM failure from different strategy") {
    val (left, right) = sampleData()
    val strategy = new BroadcastJoinStrategy()
    val otherCtx = Some(JoinFailureContext(
      failedStrategy = "sort-merge",
      exception = new RuntimeException("shuffle timeout"),
      attempt = 1
    ))
    strategy.canHandle(left, right, otherCtx) shouldBe true
  }

  // ── SortMergeJoinStrategy ─────────────────────────────────────────────

  test("SortMergeJoinStrategy: produces correct inner join") {
    val (left, right) = sampleData()
    val strategy = new SortMergeJoinStrategy()
    val result = strategy.join(left, right, Seq("id"), "inner")
    result.count() shouldBe 2
  }

  test("SortMergeJoinStrategy: supports left_outer join") {
    val (left, right) = sampleData()
    val strategy = new SortMergeJoinStrategy()
    val result = strategy.join(left, right, Seq("id"), "left_outer")
    result.count() shouldBe 3 // all left rows preserved
  }

  test("SortMergeJoinStrategy: always canHandle") {
    val (left, right) = sampleData()
    val strategy = new SortMergeJoinStrategy()
    strategy.canHandle(left, right, None) shouldBe true
    strategy.canHandle(left, right, Some(JoinFailureContext(
      "broadcast", new RuntimeException("boom"), 1))) shouldBe true
  }

  // ── RepartitionJoinStrategy ───────────────────────────────────────────

  test("RepartitionJoinStrategy: produces correct inner join") {
    val (left, right) = sampleData()
    val strategy = new RepartitionJoinStrategy(basePartitions = 4)
    val result = strategy.join(left, right, Seq("id"), "inner")
    result.count() shouldBe 2
  }

  test("RepartitionJoinStrategy: supports left_anti join") {
    val (left, right) = sampleData()
    val strategy = new RepartitionJoinStrategy(basePartitions = 4)
    val result = strategy.join(left, right, Seq("id"), "left_anti")
    result.count() shouldBe 1 // id=1 only in left
  }

  test("RepartitionJoinStrategy: always canHandle") {
    val (left, right) = sampleData()
    val strategy = new RepartitionJoinStrategy()
    strategy.canHandle(left, right, None) shouldBe true
  }
}
