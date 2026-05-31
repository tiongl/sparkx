package com.sparkx.join

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
 * Resilient join that tries a chain of [[JoinStrategy]] implementations
 * and falls back to the next one on failure.
 *
 * ==Usage==
 * {{{
 *   import com.sparkx.join.ResilientJoin._
 *
 *   val result = left.resilientJoin(right, Seq("id"))
 *   val result = left.resilientJoin(right, Seq("id"), "left_outer")
 *   val result = left.resilientJoin(right, Seq("id"),
 *     config = JoinConfig(checkpointOnSuccess = false))
 * }}}
 */
object ResilientJoin {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Implicit enrichment that adds `resilientJoin` to any DataFrame.
   */
  implicit class ResilientJoinOps(private val left: DataFrame) extends AnyVal {

    /**
     * Join `left` with `right` on `keys`, automatically retrying with
     * alternative strategies on failure.
     *
     * @param right     The right-side DataFrame.
     * @param keys      Column names to join on.
     * @param joinType  Spark join type: "inner", "left_outer", "left_anti", etc.
     * @param config    Strategy chain and options (defaults to [[JoinConfig.default]]).
     * @return The joined DataFrame.
     * @throws ResilientJoinExhaustedException if all strategies fail.
     */
    def resilientJoin(right: DataFrame, keys: Seq[String],
                      joinType: String = "inner",
                      config: JoinConfig = JoinConfig.default): DataFrame = {
      ResilientJoin.execute(left, right, keys, joinType, config)
    }
  }

  /**
   * Execute a resilient join — tries each eligible strategy in order until
   * one succeeds or all have been exhausted.
   */
  def execute(left: DataFrame, right: DataFrame,
              keys: Seq[String], joinType: String,
              config: JoinConfig): DataFrame = {

    val leftSize  = estimateSize(left)
    val rightSize = estimateSize(right)

    var failureCtx: Option[JoinFailureContext] = None
    val failures = scala.collection.mutable.ListBuffer.empty[JoinFailureContext]
    var attempt = 0

    for (strategy <- config.strategies if attempt < config.maxAttempts) {
      if (strategy.canHandle(left, right, failureCtx)) {
        attempt += 1
        log.info(s"ResilientJoin: attempting strategy '${strategy.name}' " +
          s"(attempt $attempt/${config.maxAttempts})")

        try {
          val result = strategy.join(left, right, keys, joinType)
          // Force materialisation to surface lazy execution errors
          result.queryExecution.executedPlan
          // Trigger a minimal action to fully validate the plan
          result.isEmpty

          log.info(s"ResilientJoin: strategy '${strategy.name}' succeeded")

          if (config.checkpointOnSuccess) {
            result.cache()
          }
          return result

        } catch {
          case e: Exception =>
            val ctx = JoinFailureContext(
              failedStrategy = strategy.name,
              exception = e,
              attempt = attempt,
              leftSizeBytes = leftSize,
              rightSizeBytes = rightSize
            )
            failures += ctx
            failureCtx = Some(ctx)
            log.warn(s"ResilientJoin: strategy '${strategy.name}' failed: " +
              s"${e.getClass.getSimpleName} — ${e.getMessage}")
        }
      } else {
        log.info(s"ResilientJoin: skipping strategy '${strategy.name}' " +
          s"(canHandle returned false)")
      }
    }

    throw new ResilientJoinExhaustedException(failures.toList)
  }

  private def estimateSize(df: DataFrame): Option[Long] = {
    try {
      Some(df.queryExecution.optimizedPlan.stats.sizeInBytes.toLong)
    } catch {
      case _: Exception => None
    }
  }
}

/**
 * Thrown when all join strategies in the fallback chain have been exhausted.
 *
 * @param failures All failure contexts from the attempted strategies,
 *                 in the order they were tried.
 */
class ResilientJoinExhaustedException(val failures: List[JoinFailureContext])
    extends RuntimeException(
      s"All ${failures.size} join strategies failed. " +
      s"Tried: ${failures.map(f => s"${f.failedStrategy} (${f.exception.getClass.getSimpleName})").mkString(" → ")}"
    ) {
  // Attach the last failure as the cause for stack trace visibility
  if (failures.nonEmpty) initCause(failures.last.exception)
}
