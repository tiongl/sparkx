package org.apache.spark.sql.sparkx

import com.sparkx.SparkXConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.functions.{col, count, lit}

/**
 * Discovers the *hot* values of a single join key by sampling the skewed side of a join, so the
 * auto-fix loop can salt only those keys ([[com.sparkx.autofix.TargetedSaltHint]]) instead of the
 * whole relation. Mirrors the sampling heuristic of [[com.sparkx.join.AutoSaltJoin]]: sample a
 * fraction of rows, count per key, and keep keys whose sampled frequency exceeds
 * `median × thresholdMultiplier`, capped at `maxKeys` by descending frequency.
 *
 * The discovered values are returned as strings (parsed back to the key's own type at rewrite
 * time via a cast), which keeps them serialisable inside a persisted fix profile.
 */
object SkewKeyDiscovery extends Logging {

  /**
   * @param spark          active session used to run the (one-off) sampling job
   * @param skewedSubplan  the analyzed subtree feeding the skewed side of the join
   * @param keyExpr        the join-key expression on that side
   * @param config         sampling thresholds (see `spark.sparkx.autofix.skew.*`)
   * @return the hot key values as strings, or `Nil` on any failure / no skew detected
   */
  def discover(
      spark: SparkSession,
      skewedSubplan: LogicalPlan,
      keyExpr: Expression,
      config: SparkXConfig): Seq[String] = {
    try {
      val projected = Project(Seq(Alias(keyExpr, "k")()), skewedSubplan)
      val df = Dataset.ofRows(spark, projected)

      val sampled = df.sample(withReplacement = false, config.autofixSkewSampleFraction)
        .filter(col("k").isNotNull)
      val freqs = sampled.groupBy(col("k")).agg(count(lit(1)).as("_freq"))

      val medianArr = freqs.stat.approxQuantile("_freq", Array(0.5), 0.05)
      val median = if (medianArr.nonEmpty) medianArr(0) else 1.0
      val threshold = math.max(median * config.autofixSkewThresholdMult, 1.0)

      val hot = freqs
        .filter(col("_freq") >= lit(threshold))
        .orderBy(col("_freq").desc)
        .limit(config.autofixSkewMaxKeys)
        .select(col("k"))
        .collect()

      val values = hot.iterator.map(_.get(0)).filter(_ != null).map(_.toString).toSeq
      if (values.nonEmpty) {
        logInfo(s"SkewKeyDiscovery: found ${values.size} hot key(s) (threshold=$threshold)")
      }
      values
    } catch {
      case t: Throwable =>
        logWarning(s"SkewKeyDiscovery failed, falling back to uniform salting: ${t.getMessage}")
        Nil
    }
  }
}
