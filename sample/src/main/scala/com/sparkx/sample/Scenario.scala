package com.sparkx.sample

import org.apache.spark.sql.SparkSession

trait Scenario {
  def name: String
  def description: String
  def uiPath: String
  def run(spark: SparkSession): Unit

  final def execute(spark: SparkSession): Unit = {
    val bar = "─" * 70
    println(s"\n$bar")
    println(s"  SCENARIO : $name")
    println(s"  WHAT     : $description")
    println(s"  SEE IT AT: Spark UI → sparkx → $uiPath")
    println(bar)
    run(spark)
    println(s"  ✓ $name complete")
  }
}
