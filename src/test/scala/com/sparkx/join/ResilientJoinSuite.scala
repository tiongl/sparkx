package com.sparkx.join

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[ResilientJoin]] orchestration: fallback behaviour,
 * exhaustion, and checkpoint.
 */
class ResilientJoinSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("ResilientJoinTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }

  import ResilientJoin._

  private def sampleData(): (DataFrame, DataFrame) = {
    val s = spark
    import s.implicits._
    val left = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "val_l")
    val right = Seq((2, "x"), (3, "y"), (4, "z")).toDF("id", "val_r")
    (left, right)
  }

  // ── Successful join ───────────────────────────────────────────────────

  test("resilientJoin: succeeds with default config") {
    val (left, right) = sampleData()
    val result = left.resilientJoin(right, Seq("id"))
    result.count() shouldBe 2
    result.columns should contain allOf("id", "val_l", "val_r")
  }

  test("resilientJoin: supports different join types") {
    val (left, right) = sampleData()
    val leftOuter = left.resilientJoin(right, Seq("id"), "left_outer")
    leftOuter.count() shouldBe 3

    val leftAnti = left.resilientJoin(right, Seq("id"), "left_anti")
    leftAnti.count() shouldBe 1
  }

  // ── Fallback behaviour ────────────────────────────────────────────────

  test("resilientJoin: falls back to next strategy on failure") {
    val (left, right) = sampleData()

    // First strategy always fails, second succeeds
    val failingStrategy = new JoinStrategy {
      override val name = "always-fails"
      override def canHandle(l: DataFrame, r: DataFrame,
                             ctx: Option[JoinFailureContext]) = true
      override def join(l: DataFrame, r: DataFrame,
                        keys: Seq[String], joinType: String): DataFrame =
        throw new RuntimeException("intentional failure")
    }

    val config = JoinConfig(
      strategies = Seq(failingStrategy, new SortMergeJoinStrategy()),
      maxAttempts = 3
    )
    val result = left.resilientJoin(right, Seq("id"), config = config)
    result.count() shouldBe 2
  }

  test("resilientJoin: skips strategy when canHandle is false") {
    val (left, right) = sampleData()

    // Strategy that declines
    val decliningStrategy = new JoinStrategy {
      override val name = "decliner"
      override def canHandle(l: DataFrame, r: DataFrame,
                             ctx: Option[JoinFailureContext]) = false
      override def join(l: DataFrame, r: DataFrame,
                        keys: Seq[String], joinType: String): DataFrame =
        throw new RuntimeException("should not be called")
    }

    val config = JoinConfig(
      strategies = Seq(decliningStrategy, new SortMergeJoinStrategy()),
      maxAttempts = 3
    )
    val result = left.resilientJoin(right, Seq("id"), config = config)
    result.count() shouldBe 2
  }

  test("resilientJoin: passes failure context to subsequent strategies") {
    val (left, right) = sampleData()

    var receivedCtx: Option[JoinFailureContext] = None

    val failingStrategy = new JoinStrategy {
      override val name = "first-fails"
      override def canHandle(l: DataFrame, r: DataFrame,
                             ctx: Option[JoinFailureContext]) = true
      override def join(l: DataFrame, r: DataFrame,
                        keys: Seq[String], joinType: String): DataFrame =
        throw new RuntimeException("boom")
    }

    val ctxCapturingStrategy = new JoinStrategy {
      override val name = "ctx-capturer"
      override def canHandle(l: DataFrame, r: DataFrame,
                             ctx: Option[JoinFailureContext]): Boolean = {
        receivedCtx = ctx
        true
      }
      override def join(l: DataFrame, r: DataFrame,
                        keys: Seq[String], joinType: String): DataFrame =
        l.join(r, keys, joinType)
    }

    val config = JoinConfig(
      strategies = Seq(failingStrategy, ctxCapturingStrategy),
      maxAttempts = 3
    )
    left.resilientJoin(right, Seq("id"), config = config)

    receivedCtx shouldBe defined
    receivedCtx.get.failedStrategy shouldBe "first-fails"
    receivedCtx.get.attempt shouldBe 1
    receivedCtx.get.exception.getMessage shouldBe "boom"
  }

  // ── Exhaustion ────────────────────────────────────────────────────────

  test("resilientJoin: throws ResilientJoinExhaustedException when all fail") {
    val (left, right) = sampleData()

    val fail1 = new JoinStrategy {
      override val name = "fail-1"
      override def canHandle(l: DataFrame, r: DataFrame,
                             ctx: Option[JoinFailureContext]) = true
      override def join(l: DataFrame, r: DataFrame,
                        keys: Seq[String], joinType: String): DataFrame =
        throw new RuntimeException("error-1")
    }
    val fail2 = new JoinStrategy {
      override val name = "fail-2"
      override def canHandle(l: DataFrame, r: DataFrame,
                             ctx: Option[JoinFailureContext]) = true
      override def join(l: DataFrame, r: DataFrame,
                        keys: Seq[String], joinType: String): DataFrame =
        throw new RuntimeException("error-2")
    }

    val config = JoinConfig(strategies = Seq(fail1, fail2), maxAttempts = 2)

    val ex = intercept[ResilientJoinExhaustedException] {
      left.resilientJoin(right, Seq("id"), config = config)
    }
    ex.failures should have size 2
    ex.failures(0).failedStrategy shouldBe "fail-1"
    ex.failures(1).failedStrategy shouldBe "fail-2"
    ex.getMessage should include("fail-1")
    ex.getMessage should include("fail-2")
  }

  test("resilientJoin: maxAttempts limits strategies tried") {
    val (left, right) = sampleData()

    var attempts = 0
    val failStrategy = new JoinStrategy {
      override val name = "counter"
      override def canHandle(l: DataFrame, r: DataFrame,
                             ctx: Option[JoinFailureContext]) = true
      override def join(l: DataFrame, r: DataFrame,
                        keys: Seq[String], joinType: String): DataFrame = {
        attempts += 1
        throw new RuntimeException(s"fail-$attempts")
      }
    }

    val config = JoinConfig(
      strategies = Seq(failStrategy, failStrategy, failStrategy, failStrategy),
      maxAttempts = 2
    )

    intercept[ResilientJoinExhaustedException] {
      left.resilientJoin(right, Seq("id"), config = config)
    }
    attempts shouldBe 2
  }

  // ── Checkpoint ────────────────────────────────────────────────────────

  test("resilientJoin: caches result when checkpointOnSuccess is true") {
    val (left, right) = sampleData()
    val config = JoinConfig(
      strategies = Seq(new SortMergeJoinStrategy()),
      checkpointOnSuccess = true
    )
    val result = left.resilientJoin(right, Seq("id"), config = config)
    result.storageLevel.useMemory shouldBe true
    result.unpersist()
  }

  test("resilientJoin: does not cache when checkpointOnSuccess is false") {
    val (left, right) = sampleData()
    val config = JoinConfig(
      strategies = Seq(new SortMergeJoinStrategy()),
      checkpointOnSuccess = false
    )
    val result = left.resilientJoin(right, Seq("id"), config = config)
    result.storageLevel.useMemory shouldBe false
  }
}
