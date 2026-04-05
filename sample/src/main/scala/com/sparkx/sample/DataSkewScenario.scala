package com.sparkx.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Demonstrates DATA SKEW detection.
 *
 * Creates 1,000,000 rows where 95% share the key "SKEWED_KEY".
 * After groupBy, one reducer processes ~950,000 records while the other
 * 9 each process ~5,000.  The max/median task duration ratio will be ~190×,
 * far exceeding the default 3× threshold.
 *
 * sparkx UI: sparkx → Skew — the reduce stage will be flagged as Critical.
 */
object DataSkewScenario extends Scenario {
  val name        = "Data Skew"
  val description = "95% of 1M records share one key → extreme task imbalance in the reduce stage"
  val uiPath      = "Skew"

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("  Generating 1,000,000 records (95% on SKEWED_KEY) …")
    val df = spark.range(1, 1000001).repartition(50)
      .withColumn("key", when($"id" % 20 =!= 0, lit("SKEWED_KEY"))
        .otherwise(concat(lit("key_"), ($"id" % 100).cast("string"))))
      .withColumn("value", $"id")

    // Register as a temp view and use spark.sql() so the SQL tab populates
    df.repartition(10, $"key").createOrReplaceTempView("skew_data")

    val result = spark.sql(
      """SELECT key, SUM(value) AS total
        |FROM skew_data
        |GROUP BY key""".stripMargin).collect()
    println(s"  Reduced to ${result.length} distinct keys.")
    val skewedSum = result.find(_.getAs[String]("key") == "SKEWED_KEY")
      .map(_.getAs[Long]("total")).getOrElse(0L)
    println(s"  SKEWED_KEY sum = $skewedSum")
  }
}
