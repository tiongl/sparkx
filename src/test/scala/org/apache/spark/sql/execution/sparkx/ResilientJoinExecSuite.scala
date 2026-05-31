package org.apache.spark.sql.execution.sparkx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec,
  SortMergeJoinExec}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ResilientJoinExecSuite extends AnyFunSuite with Matchers
    with BeforeAndAfterAll with BeforeAndAfterEach {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("ResilientJoinExecTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set("spark.sparkx.resilientJoin.enabled", "true")
    spark.experimental.extraStrategies = Seq(new ResilientJoinStrategy())
  }

  override def afterEach(): Unit = {
    try {
      spark.conf.set("spark.sparkx.resilientJoin.enabled", "false")
      spark.experimental.extraStrategies = Nil
    } finally {
      super.afterEach()
    }
  }

  private def sampleData(): (DataFrame, DataFrame) = {
    val s = spark
    import s.implicits._
    val left = (1 to 100).map(i => (i, s"left_$i", i * 10))
      .toDF("id", "name", "value")
    val right = (50 to 150).map(i => (i, s"right_$i", i * 20))
      .toDF("id", "label", "score")
    (left, right)
  }

  // ── Basic correctness ─────────────────────────────────────────────────

  test("ResilientJoinExec: inner join produces correct results") {
    val (left, right) = sampleData()
    val result = left.join(right, Seq("id"))

    // Verify ResilientJoinExec is in the plan
    val plan = result.queryExecution.executedPlan
    val hasResilient = plan.collect { case r: ResilientJoinExec => r }.nonEmpty
    hasResilient shouldBe true

    // ids 50-100 match
    result.count() shouldBe 51
    result.columns should contain allOf("id", "name", "value", "label", "score")
  }

  test("ResilientJoinExec: results match non-resilient join") {
    val (left, right) = sampleData()

    // Disable resilient join temporarily for baseline
    spark.conf.set("spark.sparkx.resilientJoin.enabled", "false")
    val expected = left.join(right, Seq("id")).collect()
      .map(r => r.getInt(0)).sorted
    spark.conf.set("spark.sparkx.resilientJoin.enabled", "true")

    val actual = left.join(right, Seq("id")).collect()
      .map(r => r.getInt(0)).sorted

    actual shouldBe expected
  }

  test("ResilientJoinExec: left_outer join") {
    val (left, right) = sampleData()
    val result = left.join(right, Seq("id"), "left_outer")
    result.count() shouldBe 100

    // ids 1-49 have no match, score should be null
    val nullCount = result.filter("score is null").count()
    nullCount shouldBe 49
  }

  test("ResilientJoinExec: right_outer join") {
    val (left, right) = sampleData()
    val result = left.join(right, Seq("id"), "right_outer")
    result.count() shouldBe 101

    // ids 101-150 have no match, value should be null
    val nullCount = result.filter("value is null").count()
    nullCount shouldBe 50
  }

  // ── Composite keys ────────────────────────────────────────────────────

  test("ResilientJoinExec: composite key join") {
    val s = spark
    import s.implicits._
    val left = Seq((1, "a", 10), (1, "b", 20), (2, "a", 30))
      .toDF("k1", "k2", "val_l")
    val right = Seq((1, "a", "x"), (2, "a", "y"), (3, "c", "z"))
      .toDF("k1", "k2", "val_r")

    val result = left.join(right, Seq("k1", "k2"))
    result.count() shouldBe 2
  }

  // ── Strategy falls through when disabled ──────────────────────────────

  test("ResilientJoinStrategy: returns Nil when disabled") {
    spark.conf.set("spark.sparkx.resilientJoin.enabled", "false")
    val (left, right) = sampleData()
    val result = left.join(right, Seq("id"))

    val plan = result.queryExecution.executedPlan
    val hasResilient = plan.collect { case r: ResilientJoinExec => r }.nonEmpty
    hasResilient shouldBe false

    result.count() shouldBe 51
    spark.conf.set("spark.sparkx.resilientJoin.enabled", "true")
  }

  // ── Broadcast threshold skip ──────────────────────────────────────────

  test("ResilientJoinExec: skips broadcast when threshold is very low") {
    // Set threshold to 1 byte — broadcast should be skipped, fall to shuffled hash
    spark.conf.set("spark.sparkx.resilientJoin.broadcastThreshold", "1")
    val (left, right) = sampleData()
    val result = left.join(right, Seq("id"))

    // Should still produce correct results via fallback
    result.count() shouldBe 51

    // Reset
    spark.conf.set("spark.sparkx.resilientJoin.broadcastThreshold",
      (100L * 1024 * 1024).toString)
  }

  // ── Downstream optimization preserved ─────────────────────────────────

  test("ResilientJoinExec: downstream filter produces correct results") {
    val (left, right) = sampleData()
    val result = left.join(right, Seq("id"))
      .filter("id > 90")
      .select("id", "name", "label")

    result.count() shouldBe 10  // ids 91-100
    result.columns shouldBe Array("id", "name", "label")
  }

  test("ResilientJoinExec: downstream aggregation works") {
    val (left, right) = sampleData()
    import org.apache.spark.sql.functions._
    val result = left.join(right, Seq("id"))
      .agg(sum("value").as("total_value"), count("*").as("cnt"))

    val row = result.collect()(0)
    row.getLong(1) shouldBe 51  // count
  }
}
