package com.sparkx.analysis

/**
 * A Spark *configuration* tuning recommendation, surfaced in the sparkx UI.
 *
 * Unlike [[PerformanceIssue]] (a stage-health symptom) and [[OptimizationSuggestion]] (a SQL/plan
 * rewrite), a `ConfigRecommendation` is advisory guidance about a `SparkConf` knob — something that
 * cannot be expressed as a per-query plan hint and therefore lives outside the auto-fix hint path
 * (e.g. spill thresholds, memory fraction, off-heap, serializer). It is purely informational:
 * sparkx never mutates the running configuration.
 */
case class ConfigRecommendation(
  severity:           Severity,
  configKey:          String,
  currentValue:       String,      // as observed in the app's SparkConf, or a default note
  recommendedValue:   String,
  title:              String,
  rationale:          String,      // why this helps, including any trade-off
  relatedIssue:       String,      // the detected symptom that triggered this (e.g. "GC Pressure")
  estimatedSavingsMs: Option[Long] = None
) {
  val detailPath: String = "config"
}

object ConfigRecommendation {
  /** Higher rank = more urgent, for sorting and de-duplication. */
  def rank(s: Severity): Int = s match {
    case Critical => 3
    case Warning  => 2
    case Info     => 1
  }
}
