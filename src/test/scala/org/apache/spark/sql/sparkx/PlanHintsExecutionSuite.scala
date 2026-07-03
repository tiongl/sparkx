package org.apache.spark.sql.sparkx

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Execution-level correctness tests for the injectable skew rewrites in [[PlanHints]] — the
 * catalyst-level split-broadcast and salted-join rewrites, exercised both directly (via
 * [[PlanHints.apply]] on a resolved plan) and through the SQL-text pseudo-hints resolved by
 * [[SparkXHintRule]]. Every rewrite must return exactly the same rows as the equivalent naive
 * join. Uses skewed (hot-key) data so the rewrites do real work.
 *
 * Runs a local SparkSession, so — like the other SparkSession-based suites — it is skipped on
 * Windows/JDK17 where Spark cannot initialise, but exercises the rewrites end-to-end in CI.
 */
class PlanHintsExecutionSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tmp = java.nio.file.Files.createTempDirectory("sparkx-planhints-exec").toUri.toString
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("PlanHintsExecutionTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      // Register the extension so the SQL-text pseudo-hints resolve via SparkXHintRule.
      .config("spark.sql.extensions", "org.apache.spark.sql.sparkx.SparkXAutoFixExtension")
      // Keep the auto-fix loop out of the way; we only want the hint-resolution rule.
      .config("spark.sparkx.autofix.enabled", "false")
      .config("spark.sparkx.autofix.store.path", tmp)
      // Force real shuffle joins so the rewrites are what changes the plan.
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    seedData()
  }

  override def afterAll(): Unit = {
    try if (spark != null) spark.stop() finally super.afterAll()
  }

  /** A skewed fact (90% share key 0) joined to a small dimension. */
  private def seedData(): Unit = {
    val s = spark
    import s.implicits._
    val fact = (1 to 1000).map(i => (if (i % 10 != 0) 0 else i, s"v$i")).toDF("k", "v")
    val dim  = (0 to 100).map(i => (i, s"n$i")).toDF("k", "name")
    fact.createOrReplaceTempView("f")
    dim.createOrReplaceTempView("d")
  }

  private def rowsOf(df: DataFrame): Seq[String] =
    df.collect().map(_.toString).sorted.toSeq

  private def exec(plan: LogicalPlan): DataFrame = Dataset.ofRows(spark, plan)

  private def analyzedOf(sql: String): LogicalPlan =
    spark.sql(sql).queryExecution.analyzed

  // ── Direct PlanHints.apply rewrites ────────────────────────────────────────

  test("split-broadcast rewrite (inner) returns the same rows as a naive join") {
    val sql = "SELECT f.k, f.v, d.name FROM f JOIN d ON f.k = d.k"
    val expected = rowsOf(spark.sql(sql))
    val rewritten = PlanHints.apply(analyzedOf(sql), Seq(com.sparkx.autofix.SplitBroadcastHint("d", 3)))
    PlanHints.hasInjected(rewritten) shouldBe true
    rowsOf(exec(rewritten)) shouldBe expected
  }

  test("salted-join rewrite (inner) returns the same rows as a naive join") {
    val sql = "SELECT f.k, f.v, d.name FROM f JOIN d ON f.k = d.k"
    val expected = rowsOf(spark.sql(sql))
    val rewritten = PlanHints.apply(analyzedOf(sql), Seq(com.sparkx.autofix.SaltedJoinHint("d", 4)))
    PlanHints.hasInjected(rewritten) shouldBe true
    rowsOf(exec(rewritten)) shouldBe expected
  }

  test("split-broadcast rewrite (left-semi, build on right) matches a naive left-semi join") {
    val sql = "SELECT f.k, f.v FROM f LEFT SEMI JOIN d ON f.k = d.k"
    val expected = rowsOf(spark.sql(sql))
    val rewritten = PlanHints.apply(analyzedOf(sql), Seq(com.sparkx.autofix.SplitBroadcastHint("d", 3)))
    PlanHints.hasInjected(rewritten) shouldBe true
    rowsOf(exec(rewritten)) shouldBe expected
  }

  test("split-broadcast preserves row multiplicity when the dimension has duplicate keys") {
    val s = spark
    import s.implicits._
    // Two dim rows per key => each fact row must match twice.
    (0 to 100).flatMap(i => Seq((i, s"a$i"), (i, s"b$i"))).toDF("k", "name")
      .createOrReplaceTempView("d_dup")
    val sql = "SELECT f.k, f.v, d_dup.name FROM f JOIN d_dup ON f.k = d_dup.k"
    val expected = rowsOf(spark.sql(sql))
    val rewritten = PlanHints.apply(analyzedOf(sql), Seq(com.sparkx.autofix.SplitBroadcastHint("d_dup", 4)))
    rowsOf(exec(rewritten)) shouldBe expected
  }

  test("salted join preserves row multiplicity when the dimension has duplicate keys") {
    val sql = "SELECT f.k, f.v, d_dup.name FROM f JOIN d_dup ON f.k = d_dup.k"
    val expected = rowsOf(spark.sql(sql))
    val rewritten = PlanHints.apply(analyzedOf(sql), Seq(com.sparkx.autofix.SaltedJoinHint("d_dup", 8)))
    rowsOf(exec(rewritten)) shouldBe expected
  }

  // ── Targeted salting (hot keys only) ───────────────────────────────────────

  test("targeted salt (only the hot key) returns the same rows as a naive join") {
    val sql = "SELECT f.k, f.v, d.name FROM f JOIN d ON f.k = d.k"
    val expected = rowsOf(spark.sql(sql))
    // Key 0 is the hot key (90% of the fact); cold keys must stay correct with salt 0.
    val rewritten = PlanHints.apply(analyzedOf(sql), Seq(com.sparkx.autofix.TargetedSaltHint("d", 4, Seq("0"))))
    PlanHints.hasInjected(rewritten) shouldBe true
    rewritten shouldBe a[org.apache.spark.sql.catalyst.plans.logical.Project]
    rowsOf(exec(rewritten)) shouldBe expected
  }

  test("targeted salt is correct when hot-key list includes an absent value and misses a present cold key") {
    val sql = "SELECT f.k, f.v, d.name FROM f JOIN d ON f.k = d.k"
    val expected = rowsOf(spark.sql(sql))
    // 0 is hot & present, 999 is absent; cold keys (10,20,...) are deliberately NOT listed.
    val rewritten = PlanHints.apply(analyzedOf(sql), Seq(com.sparkx.autofix.TargetedSaltHint("d", 6, Seq("0", "999"))))
    rowsOf(exec(rewritten)) shouldBe expected
  }

  test("targeted salt preserves row multiplicity when the dimension has duplicate keys") {
    val sql = "SELECT f.k, f.v, d_dup.name FROM f JOIN d_dup ON f.k = d_dup.k"
    val expected = rowsOf(spark.sql(sql))
    val rewritten = PlanHints.apply(analyzedOf(sql), Seq(com.sparkx.autofix.TargetedSaltHint("d_dup", 8, Seq("0"))))
    rowsOf(exec(rewritten)) shouldBe expected
  }

  test("SkewKeyDiscovery finds the hot key by sampling the skewed side") {
    val skewed = analyzedOf("SELECT k FROM f")
    val keyExpr = skewed.output.head
    val cfg = com.sparkx.SparkXConfig.fromConf(spark.sparkContext.getConf)
      .copy(autofixSkewSampleFraction = 1.0, autofixSkewThresholdMult = 5.0, autofixSkewMaxKeys = 10)
    val hot = SkewKeyDiscovery.discover(spark, skewed, keyExpr, cfg)
    hot should contain ("0")
  }

  test("SQL /*+ TARGETED_SALT(d, n, hot...) */ hint applies and stays correct") {
    val base = "SELECT f.k, f.v, d.name FROM f JOIN d ON f.k = d.k"
    val expected = rowsOf(spark.sql(base))
    val hinted = spark.sql("SELECT /*+ TARGETED_SALT(d, 6, 0) */ f.k, f.v, d.name FROM f JOIN d ON f.k = d.k")
    PlanHints.hasInjected(hinted.queryExecution.analyzed) shouldBe true
    rowsOf(hinted) shouldBe expected
  }

  // ── SQL-text pseudo-hints (SparkXHintRule) ─────────────────────────────────

  test("SQL /*+ SPLIT_BROADCAST(d, n) */ hint applies and stays correct") {
    val base = "SELECT f.k, f.v, d.name FROM f JOIN d ON f.k = d.k"
    val expected = rowsOf(spark.sql(base))
    val hinted = spark.sql("SELECT /*+ SPLIT_BROADCAST(d, 5) */ f.k, f.v, d.name FROM f JOIN d ON f.k = d.k")
    PlanHints.hasInjected(hinted.queryExecution.analyzed) shouldBe true
    rowsOf(hinted) shouldBe expected
  }

  test("SQL /*+ SALT(d, n) */ hint applies and stays correct") {
    val base = "SELECT f.k, f.v, d.name FROM f JOIN d ON f.k = d.k"
    val expected = rowsOf(spark.sql(base))
    val hinted = spark.sql("SELECT /*+ SALT(d, 6) */ f.k, f.v, d.name FROM f JOIN d ON f.k = d.k")
    PlanHints.hasInjected(hinted.queryExecution.analyzed) shouldBe true
    rowsOf(hinted) shouldBe expected
  }

  test("an aggregate over a split-broadcast-hinted join yields the same count") {
    val expected = spark.sql("SELECT COUNT(*) FROM f JOIN d ON f.k = d.k").collect()(0).getLong(0)
    val got = spark.sql("SELECT /*+ SPLIT_BROADCAST(d, 4) */ COUNT(*) FROM f JOIN d ON f.k = d.k")
      .collect()(0).getLong(0)
    got shouldBe expected
  }
}
