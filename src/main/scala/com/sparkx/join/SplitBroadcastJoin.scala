package com.sparkx.join

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
 * Configuration for [[SplitBroadcastJoin]].
 *
 * @param broadcastBudgetBytes Target size per broadcast chunk. When an OOM
 *                             reveals the actual table size, the split count
 *                             is computed as `ceil(actualSize / budget)`.
 *                             Default 100 MB.
 * @param maxSplits            Upper limit on chunk count before giving up.
 * @param cacheResult          Whether to cache the final union result.
 * @param cacheLargeSide       Whether to cache the large (probe) DataFrame
 *                             before the split loop. When `true` the large
 *                             side is read from memory on each split instead
 *                             of being recomputed. Recommended when the large
 *                             side is expensive to derive (e.g. a complex
 *                             subquery chain). Default `true`.
 */
case class SplitBroadcastConfig(
    broadcastBudgetBytes: Long = 100L * 1024 * 1024,
    maxSplits: Int = 16,
    cacheResult: Boolean = true,
    cacheLargeSide: Boolean = true
)

/**
 * A join strategy that splits the smaller DataFrame into multiple chunks,
 * broadcasts each chunk individually against the larger side, and unions
 * the partial results.
 *
 * When a full broadcast fails with OOM, the strategy parses the exception
 * to extract the actual broadcast table size and dynamically calculates
 * the optimal number of splits. If parsing fails, it falls back to
 * doubling the split count on each retry.
 *
 * ==Usage==
 * {{{
 *   import com.sparkx.join.SplitBroadcastJoin._
 *
 *   // Auto-detect splits from OOM (starts with full broadcast)
 *   val result = largeDf.splitBroadcastJoin(smallDf, Seq("id"))
 *
 *   // With custom config
 *   val result = largeDf.splitBroadcastJoin(smallDf, Seq("id"), "inner",
 *     SplitBroadcastConfig(broadcastBudgetBytes = 50L * 1024 * 1024))
 * }}}
 *
 * '''Supported join types:''' `inner`, `left_semi`, `cross`.
 * Other join types (outer, anti) are rejected with an
 * [[IllegalArgumentException]] because union-of-partials produces
 * incorrect results for them.
 */
object SplitBroadcastJoin {

  private val log = LoggerFactory.getLogger(getClass)

  private val SupportedJoinTypes = Set("inner", "left_semi", "cross")

  implicit class SplitBroadcastJoinOps(private val large: DataFrame)
      extends AnyVal {

    /**
     * Join `large` with `small` using split-broadcast.
     *
     * @param small    The smaller DataFrame to be split and broadcast.
     * @param keys     Column names to join on.
     * @param joinType Spark join type (must be inner, left_semi, or cross).
     * @param config   Split-broadcast options.
     * @return The joined DataFrame.
     */
    def splitBroadcastJoin(small: DataFrame, keys: Seq[String],
                           joinType: String = "inner",
                           config: SplitBroadcastConfig =
                             SplitBroadcastConfig()): DataFrame = {
      SplitBroadcastJoin.execute(large, small, keys, joinType, config)
    }
  }

  /**
   * Execute a split-broadcast join.
   *
   * Starts by attempting a full broadcast (1 split). On OOM, parses the
   * exception to determine the actual table size and calculates the
   * optimal split count. Falls back to doubling on each subsequent OOM
   * if parsing fails.
   */
  def execute(large: DataFrame, small: DataFrame,
              keys: Seq[String], joinType: String,
              config: SplitBroadcastConfig): DataFrame = {
    require(SupportedJoinTypes.contains(joinType),
      s"SplitBroadcastJoin does not support join type '$joinType'. " +
      s"Supported: ${SupportedJoinTypes.mkString(", ")}")

    // Cache the large side so repeated scans (one per split) hit memory
    // instead of recomputing a potentially expensive subquery chain.
    val cachedLarge = if (config.cacheLargeSide) large.cache() else large

    var splits = 1  // start with full broadcast
    var lastException: Exception = null

    try {
      while (splits <= config.maxSplits) {
        log.info(s"SplitBroadcastJoin: attempting with $splits split(s)")
        try {
          val result = executeWithSplits(cachedLarge, small, keys, joinType, splits)
          // Materialise to surface lazy errors
          result.queryExecution.executedPlan
          result.isEmpty

          log.info(s"SplitBroadcastJoin: succeeded with $splits split(s)")
          if (config.cacheResult) result.cache()
          return result

        } catch {
          case e: Exception =>
            lastException = e
            val newSplits = computeNextSplits(e, splits, config)
            if (newSplits > config.maxSplits) {
              log.warn(s"SplitBroadcastJoin: $splits split(s) failed and " +
                s"next would be $newSplits (exceeds maxSplits=${config.maxSplits})")
              throw new SplitBroadcastExhaustedException(splits, config.maxSplits, e)
            }
            log.warn(s"SplitBroadcastJoin: $splits split(s) failed " +
              s"(${e.getClass.getSimpleName}), escalating to $newSplits split(s)")
            splits = newSplits
        }
      }

      throw new SplitBroadcastExhaustedException(
        splits, config.maxSplits, lastException)
    } finally {
      // Clean up the cached large side (but not the result cache)
      if (config.cacheLargeSide) cachedLarge.unpersist(blocking = false)
    }
  }

  /**
   * Execute the join with a specific number of splits.
   */
  private def executeWithSplits(large: DataFrame, small: DataFrame,
                                keys: Seq[String], joinType: String,
                                numSplits: Int): DataFrame = {
    if (numSplits == 1) {
      // Full broadcast — no splitting needed
      return large.join(broadcast(small), keys, joinType)
    }

    val hashExpr = hash(keys.map(col): _*)
    val partials = (0 until numSplits).map { i =>
      val chunk = small.filter(pmod(hashExpr, lit(numSplits)) === lit(i))
      large.join(broadcast(chunk), keys, joinType)
    }

    partials.reduce(_ union _)
  }

  /**
   * Determine the next split count after an OOM failure.
   *
   * Tries to parse the actual broadcast table size from Spark's error
   * message. If found, computes `ceil(actualSize / budget)` and ensures
   * it's at least `currentSplits * 2` (never go backwards). Falls back
   * to simple doubling if parsing fails.
   */
  private[join] def computeNextSplits(e: Throwable, currentSplits: Int,
                                      config: SplitBroadcastConfig): Int = {
    val parsed = parseBroadcastSize(e)
    parsed match {
      case Some(actualBytes) =>
        val computed = math.ceil(actualBytes.toDouble / config.broadcastBudgetBytes).toInt
        // Ensure we always advance beyond current splits
        math.max(computed, currentSplits * 2)
      case None =>
        currentSplits * 2
    }
  }

  /**
   * Attempt to extract the broadcast table size from an exception chain.
   *
   * Spark's broadcast OOM message typically contains patterns like:
   *  - "The size of the broadcast table is 123456 bytes"
   *  - "is X MiB" or "is X KiB"
   */
  private[join] def parseBroadcastSize(t: Throwable): Option[Long] = {
    val bytesPattern = """(?i)size\s+(?:of\s+)?(?:the\s+)?broadcast.*?(\d[\d,]*)\s*bytes""".r
    val mibPattern = """(?i)size\s+(?:of\s+)?(?:the\s+)?broadcast.*?([\d.]+)\s*(?:MiB|MB)""".r
    val kibPattern = """(?i)size\s+(?:of\s+)?(?:the\s+)?broadcast.*?([\d.]+)\s*(?:KiB|KB)""".r
    val gibPattern = """(?i)size\s+(?:of\s+)?(?:the\s+)?broadcast.*?([\d.]+)\s*(?:GiB|GB)""".r

    var current: Throwable = t
    while (current != null) {
      val msg = Option(current.getMessage).getOrElse("")
      bytesPattern.findFirstMatchIn(msg).foreach { m =>
        return Some(m.group(1).replace(",", "").toLong)
      }
      gibPattern.findFirstMatchIn(msg).foreach { m =>
        return Some((m.group(1).toDouble * 1024 * 1024 * 1024).toLong)
      }
      mibPattern.findFirstMatchIn(msg).foreach { m =>
        return Some((m.group(1).toDouble * 1024 * 1024).toLong)
      }
      kibPattern.findFirstMatchIn(msg).foreach { m =>
        return Some((m.group(1).toDouble * 1024).toLong)
      }
      current = current.getCause
    }
    None
  }
}

/**
 * Thrown when all split-broadcast attempts have been exhausted.
 */
class SplitBroadcastExhaustedException(
    lastSplits: Int,
    maxSplits: Int,
    cause: Throwable
) extends RuntimeException(
  s"SplitBroadcastJoin exhausted: tried up to $lastSplits splits " +
  s"(max $maxSplits). Last error: ${Option(cause).map(_.getMessage).getOrElse("unknown")}",
  cause
)
