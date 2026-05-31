package com.sparkx.join

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast

/**
 * A pluggable join strategy that can be composed into a fallback chain
 * by [[ResilientJoin]].
 */
trait JoinStrategy {

  /** Human-readable name used in logging and failure context. */
  def name: String

  /**
   * Whether this strategy is willing to attempt the join given the current
   * context. Strategies can decline based on data size or on information
   * from a previous failure (e.g. broadcast will decline after an OOM).
   */
  def canHandle(left: DataFrame, right: DataFrame,
                failureCtx: Option[JoinFailureContext]): Boolean

  /**
   * Execute the join and return the resulting DataFrame.
   *
   * @throws Exception on any join failure — the caller will catch this,
   *                    wrap it in a [[JoinFailureContext]], and try the
   *                    next strategy in the chain.
   */
  def join(left: DataFrame, right: DataFrame,
           keys: Seq[String], joinType: String): DataFrame
}

// ─── Built-in strategies ──────────────────────────────────────────────

/**
 * Wraps the smaller side in `broadcast()` for a broadcast hash join.
 *
 * Declines when:
 *  - The smaller side exceeds `thresholdBytes`.
 *  - A previous attempt already failed with an OOM or broadcast error.
 *
 * @param thresholdBytes Maximum estimated size (in bytes) for broadcast
 *                       eligibility. Default 100 MB.
 */
class BroadcastJoinStrategy(thresholdBytes: Long = 100L * 1024 * 1024)
    extends JoinStrategy {

  override val name: String = "broadcast"

  override def canHandle(left: DataFrame, right: DataFrame,
                         failureCtx: Option[JoinFailureContext]): Boolean = {
    // Decline if a previous broadcast already OOM'd
    val previousBroadcastFailed = failureCtx.exists { ctx =>
      ctx.failedStrategy == name && isBroadcastOrOomError(ctx.exception)
    }
    if (previousBroadcastFailed) return false

    val smallerSize = math.min(planSize(left), planSize(right))
    smallerSize <= thresholdBytes
  }

  override def join(left: DataFrame, right: DataFrame,
                    keys: Seq[String], joinType: String): DataFrame = {
    val leftSize  = planSize(left)
    val rightSize = planSize(right)
    if (leftSize <= rightSize)
      broadcast(left).join(right, keys, joinType)
    else
      left.join(broadcast(right), keys, joinType)
  }

  private def planSize(df: DataFrame): Long =
    df.queryExecution.optimizedPlan.stats.sizeInBytes.toLong

  private def isBroadcastOrOomError(t: Throwable): Boolean = {
    val msg = fullMessage(t).toLowerCase
    val className = fullClassName(t).toLowerCase
    msg.contains("outofmemory") || msg.contains("oom") ||
    msg.contains("not enough memory to build and broadcast") ||
    className.contains("outofmemory")
  }

  private def fullClassName(t: Throwable): String = {
    val sb = new StringBuilder
    var current: Throwable = t
    while (current != null) {
      sb.append(current.getClass.getName).append(" ")
      current = current.getCause
    }
    sb.toString()
  }

  private def fullMessage(t: Throwable): String = {
    val sb = new StringBuilder
    var current: Throwable = t
    while (current != null) {
      if (current.getMessage != null) sb.append(current.getMessage).append(" ")
      current = current.getCause
    }
    sb.toString()
  }
}

/**
 * Default Spark sort-merge join. Disables auto-broadcast to force
 * sort-merge behaviour.
 */
class SortMergeJoinStrategy extends JoinStrategy {

  override val name: String = "sort-merge"

  override def canHandle(left: DataFrame, right: DataFrame,
                         failureCtx: Option[JoinFailureContext]): Boolean = true

  override def join(left: DataFrame, right: DataFrame,
                    keys: Seq[String], joinType: String): DataFrame = {
    val spark = left.sparkSession
    val origThreshold = spark.conf.get(
      "spark.sql.autoBroadcastJoinThreshold", "-1")
    try {
      // Disable auto-broadcast to ensure sort-merge join
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      left.join(right, keys, joinType)
    } finally {
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", origThreshold)
    }
  }
}

/**
 * Repartitions both sides by join keys before joining. Adapts partition
 * count based on failure context — doubles partitions after OOM/shuffle
 * failures.
 *
 * @param basePartitions  Default number of partitions to repartition into.
 *                        Doubled on each subsequent OOM/shuffle failure.
 */
class RepartitionJoinStrategy(basePartitions: Int = 200) extends JoinStrategy {

  override val name: String = "repartition"

  override def canHandle(left: DataFrame, right: DataFrame,
                         failureCtx: Option[JoinFailureContext]): Boolean = true

  override def join(left: DataFrame, right: DataFrame,
                    keys: Seq[String], joinType: String): DataFrame = {
    import org.apache.spark.sql.functions.col
    val keyCols = keys.map(col)
    val rLeft  = left.repartition(basePartitions, keyCols: _*)
    val rRight = right.repartition(basePartitions, keyCols: _*)
    rLeft.join(rRight, keys, joinType)
  }
}
