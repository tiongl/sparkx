package org.apache.spark.sql.execution.sparkx

import org.apache.spark.sql.internal.SQLConf

/**
 * Configuration for the engine-level resilient join strategy.
 *
 * All keys are prefixed with `spark.sparkx.resilientJoin.` and can be
 * set via `SparkConf` or `spark.conf.set(...)` at runtime.
 *
 * When enabled, the [[ResilientJoinStrategy]] planner rule intercepts
 * `Join` logical nodes and produces a [[ResilientJoinExec]] physical
 * node that contains a multi-strategy fallback chain inside
 * `doExecute()`.
 */
object ResilientJoinConf {

  private val PREFIX = "spark.sparkx.resilientJoin"

  // ── Master switch ─────────────────────────────────────────────────────

  /** Whether the resilient join planner strategy is active. */
  val ENABLED = s"$PREFIX.enabled"
  val ENABLED_DEFAULT = false

  // ── Broadcast ─────────────────────────────────────────────────────────

  /** Maximum estimated size (bytes) of the smaller side for broadcast attempt. */
  val BROADCAST_THRESHOLD = s"$PREFIX.broadcastThreshold"
  val BROADCAST_THRESHOLD_DEFAULT: Long = 100L * 1024 * 1024 // 100 MB

  // ── Split-broadcast ───────────────────────────────────────────────────

  /** Whether to try split-broadcast after a broadcast OOM. */
  val SPLIT_BROADCAST_ENABLED = s"$PREFIX.splitBroadcast.enabled"
  val SPLIT_BROADCAST_ENABLED_DEFAULT = true

  /** Target size per broadcast chunk for split-broadcast. */
  val SPLIT_BROADCAST_BUDGET = s"$PREFIX.splitBroadcast.budgetBytes"
  val SPLIT_BROADCAST_BUDGET_DEFAULT: Long = 100L * 1024 * 1024

  /** Maximum number of splits before giving up. */
  val SPLIT_BROADCAST_MAX_SPLITS = s"$PREFIX.splitBroadcast.maxSplits"
  val SPLIT_BROADCAST_MAX_SPLITS_DEFAULT = 16

  // ── Auto-salt ─────────────────────────────────────────────────────────

  /** Whether to try auto-salt join (skew detection + salting). */
  val AUTO_SALT_ENABLED = s"$PREFIX.autoSalt.enabled"
  val AUTO_SALT_ENABLED_DEFAULT = true

  /** Fraction of rows to sample for skew detection. */
  val AUTO_SALT_SAMPLE_FRACTION = s"$PREFIX.autoSalt.sampleFraction"
  val AUTO_SALT_SAMPLE_FRACTION_DEFAULT = 0.01

  /** Key frequency must exceed median × multiplier to be hot. */
  val AUTO_SALT_SKEW_MULTIPLIER = s"$PREFIX.autoSalt.skewMultiplier"
  val AUTO_SALT_SKEW_MULTIPLIER_DEFAULT = 10.0

  /** Number of salt buckets per hot key. */
  val AUTO_SALT_FACTOR = s"$PREFIX.autoSalt.saltFactor"
  val AUTO_SALT_FACTOR_DEFAULT = 10

  /** Absolute minimum estimated row count for a hot key. */
  val AUTO_SALT_MIN_HOT_KEY_COUNT = s"$PREFIX.autoSalt.minHotKeyCount"
  val AUTO_SALT_MIN_HOT_KEY_COUNT_DEFAULT: Long = 100L

  // ── Helpers ───────────────────────────────────────────────────────────

  /** Read a boolean config with fallback. */
  def getBoolean(sqlConf: SQLConf, key: String, default: Boolean): Boolean =
    sqlConf.getConfString(key, default.toString).toBoolean

  /** Read a long config with fallback. */
  def getLong(sqlConf: SQLConf, key: String, default: Long): Long =
    sqlConf.getConfString(key, default.toString).toLong

  /** Read an int config with fallback. */
  def getInt(sqlConf: SQLConf, key: String, default: Int): Int =
    sqlConf.getConfString(key, default.toString).toInt

  /** Read a double config with fallback. */
  def getDouble(sqlConf: SQLConf, key: String, default: Double): Double =
    sqlConf.getConfString(key, default.toString).toDouble
}
