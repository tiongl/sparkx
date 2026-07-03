package org.apache.spark.sql.sparkx

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Demo-only helper: execute a hand-built [[LogicalPlan]] as a [[DataFrame]]. Lives in the
 * `org.apache.spark.sql.sparkx` package so it can reach the `private[sql]` `Dataset.ofRows`,
 * letting [[com.sparkx.sample.SparkXAutoFixDemo]] validate an injected skew rewrite directly
 * (independent of which strategy the tuner happens to pick for the demo data).
 */
object DemoPlanRunner {
  def run(spark: SparkSession, plan: LogicalPlan): DataFrame = Dataset.ofRows(spark, plan)
}
