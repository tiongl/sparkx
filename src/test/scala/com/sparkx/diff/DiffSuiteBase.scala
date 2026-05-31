package com.sparkx.diff

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Shared SparkSession and test data helpers for diff strategy suites. */
trait DiffSuiteBase extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient protected var spark: SparkSession = _

  // Stable reference for Spark implicits (var cannot be used directly)
  protected object testImplicits extends Serializable {
    def session: SparkSession = spark
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("DiffStrategyTests")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    // Don't stop SparkSession — other suites in the same JVM may still need it.
    // It will be cleaned up at JVM shutdown.
    super.afterAll()
  }

  // ── Helpers ────────────────────────────────────────────────────────────

  protected def leftDf: DataFrame = {
    val s = spark
    import s.implicits._
    Seq(
      (1, "alice", 10.0),
      (2, "bob",   20.0),
      (3, "carol", 30.0),
      (4, "dave",  40.0)
    ).toDF("id", "name", "value")
  }

  /** Right side: row 2 changed, row 4 removed, row 5 added. */
  protected def rightDf: DataFrame = {
    val s = spark
    import s.implicits._
    Seq(
      (1, "alice",  10.0),
      (2, "bob",    25.0),  // value changed
      (3, "carol",  30.0),
      (5, "eve",    50.0)   // added
    ).toDF("id", "name", "value")
  }

  protected def defaultConfig: DiffConfig =
    DiffConfig(keyColumns = Seq("id"))
}
