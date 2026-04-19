package com.sparkx.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Demonstrates OPTIMIZATION SUGGESTION detection.
 *
 * Triggers multiple suggestion types that SuggestionDetector analyses from
 * SQL physical plans:
 *
 *   1. Suboptimal file format — reads from CSV (row-based, no predicate pushdown)
 *   2. Broadcast join candidate — sort-merge joins a small table that could
 *      be broadcast (shuffle side well under the broadcast threshold)
 *   3. Excessive shuffles — chains multiple repartitions to generate many
 *      Exchange nodes in a single SQL execution
 *   4. Cartesian product — a cross-join that produces O(N×M) rows
 *   5. Missing AQE — AQE is disabled (demo default) so any execution with
 *      shuffles triggers the hint
 *
 * sparkx UI: sparkx → Suggestions
 */
object SuggestionScenario extends Scenario {
  val name        = "Optimization Suggestions"
  val description = "Triggers broadcast-candidate, excessive shuffle, suboptimal format, cartesian, repeated scan, and collect suggestions"
  val uiPath      = "Suggestions"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val tmpDir = System.getProperty("java.io.tmpdir")
    val csvPath = s"$tmpDir/sparkx-suggestion-demo"

    // ── 1. Suboptimal file format: write CSV, then read it back ──────────
    println("  Writing CSV data to trigger suboptimal-format suggestion …")
    spark.range(1, 50001)
      .withColumn("category", ($"id" % 10).cast("string"))
      .withColumn("value", $"id" * 3)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(csvPath)

    val csvDf = spark.read.option("header", "true").csv(csvPath)
    csvDf.createOrReplaceTempView("csv_data")

    // Query with filter — will show as CSV FileScan with data filters
    // but no partition pruning (CSV isn't partitioned)
    val csvResult = spark.sql(
      """SELECT category, SUM(CAST(value AS LONG)) AS total
        |FROM csv_data
        |WHERE CAST(id AS LONG) > 10000
        |GROUP BY category""".stripMargin).collect()
    println(s"  CSV query returned ${csvResult.length} rows (suboptimal format flagged).")

    // ── 2. Broadcast join candidate: small-side sort-merge join ──────────
    println("  Running sort-merge join with a small side to trigger broadcast suggestion …")
    val bigTable = spark.range(1, 200001).repartition(20)
      .withColumn("dept_id", ($"id" % 50).cast("int"))
      .withColumn("amount", $"id" * 7)
    bigTable.createOrReplaceTempView("sales")

    // Small dimension table — only 50 rows; shuffle write will be tiny
    val smallTable = spark.range(1, 51)
      .withColumn("dept_id", $"id".cast("int"))
      .withColumn("dept_name", concat(lit("Dept-"), $"id"))
    smallTable.createOrReplaceTempView("departments")

    // Disable auto-broadcast so Spark uses SortMergeJoin
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val joinResult = spark.sql(
      """SELECT d.dept_name, SUM(s.amount) AS total_sales
        |FROM sales s
        |JOIN departments d ON s.dept_id = d.dept_id
        |GROUP BY d.dept_name""".stripMargin).collect()
    println(s"  Sort-merge join returned ${joinResult.length} departments (broadcast candidate flagged).")
    // Restore default
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

    // ── 3. Excessive shuffles: chain multiple repartitions ──────────────
    println("  Running query with excessive shuffles …")
    val base = spark.range(1, 100001)
      .withColumn("k1", ($"id" % 100).cast("string"))
      .withColumn("k2", ($"id" % 50).cast("string"))
      .withColumn("v",  $"id")

    // Each repartition + aggregation adds Exchange nodes; chaining 5+
    // exceeds the default threshold of 4
    val shuffled = base
      .repartition(20, $"k1")
      .groupBy("k1").agg(sum("v").as("s1"))
      .repartition(10, $"k1")
      .join(
        base.repartition(20, $"k2")
          .groupBy("k2").agg(sum("v").as("s2")),
        $"k1" === $"k2"
      )
      .repartition(5)
      .agg(sum("s1").as("total"))
      .collect()
    println(s"  Excessive shuffle query complete (${shuffled.head.getLong(0)} total).")

    // ── 4. Cartesian product ────────────────────────────────────────────
    println("  Running cross-join to trigger cartesian product suggestion …")
    val left  = spark.range(1, 51).toDF("a")
    val right = spark.range(1, 21).toDF("b")
    left.createOrReplaceTempView("t_left")
    right.createOrReplaceTempView("t_right")

    val crossResult = spark.sql(
      """SELECT COUNT(*) AS cnt
        |FROM t_left, t_right
        |WHERE t_left.a + t_right.b > 30""".stripMargin).collect()
    println(s"  Cross-join produced ${crossResult.head.getLong(0)} rows (cartesian flagged).")

    // ── 5. Repeated table scan — read CSV twice in same query ────────────
    println("  Running self-join on CSV to trigger repeated-scan suggestion …")
    val repeatedResult = spark.sql(
      """SELECT a.category, COUNT(*) AS cnt
        |FROM csv_data a
        |JOIN csv_data b ON a.category = b.category
        |GROUP BY a.category""".stripMargin).collect()
    println(s"  Repeated scan query returned ${repeatedResult.length} rows.")

    // ── 6. Collect on large data ─────────────────────────────────────────
    println("  Running collect on large dataset to trigger collect-large-data suggestion …")
    val bigData = spark.range(1, 50001).repartition(10)
      .withColumn("payload", concat(lit("data_"), $"id".cast("string")))
    bigData.write.mode("overwrite").csv(s"$csvPath-big")
    val bigCsv = spark.read.csv(s"$csvPath-big")
    bigCsv.createOrReplaceTempView("big_csv")
    val collected = spark.sql("SELECT * FROM big_csv").collect()
    println(s"  Collected ${collected.length} rows from large dataset (collect warning flagged).")

    // Cleanup temp CSV directories
    try {
      for (path <- Seq(csvPath, s"$csvPath-big")) {
        val dir = new java.io.File(path)
        if (dir.exists()) {
          dir.listFiles().foreach(_.delete())
          dir.delete()
        }
      }
    } catch { case _: Exception => }

    println("  All suggestion triggers complete — check sparkx → Suggestions tab.")
  }
}
