package org.apache.spark.sql.sparkx

import com.sparkx.SparkXConfig
import com.sparkx.autofix._
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

/**
 * Derives candidate hints for a completed query by inspecting its analyzed logical plan
 * (join sizes, table names) and its physical plan (shuffle presence). These recommendations
 * feed [[com.sparkx.autofix.HintPolicy]], which decides what to actually try next.
 */
object PlanAdvisor {

  def advise(qe: QueryExecution, config: SparkXConfig): Seq[Hint] = {
    val analyzed = try qe.analyzed catch { case _: Throwable => null }
    if (analyzed == null) return Nil
    val out = scala.collection.mutable.ArrayBuffer[Hint]()
    out ++= broadcastAdvice(analyzed, config)
    out ++= partitioningAdvice(qe, analyzed, config)
    HintRewriter.dedupe(out.toSeq)
  }

  /** Suggest broadcasting the smaller side of an equi-join when it is below the threshold. */
  private def broadcastAdvice(analyzed: LogicalPlan, config: SparkXConfig): Seq[Hint] = {
    val threshold = BigInt(config.autofixBroadcastMaxBytes)
    analyzed.collect { case j: Join if j.condition.isDefined => j }.flatMap { j =>
      val leftSize  = sizeOf(j.left)
      val rightSize = sizeOf(j.right)
      val (smallSide, smallSize) =
        if (leftSize <= rightSize) (j.left, leftSize) else (j.right, rightSize)
      if (smallSize < threshold) relationName(smallSide).map(n => BroadcastHint(Seq(n)))
      else None
    }
  }

  /** When AQE is off and the shuffle-partition count is badly sized, suggest repartition/coalesce. */
  private def partitioningAdvice(
    qe: QueryExecution, analyzed: LogicalPlan, config: SparkXConfig): Seq[Hint] = {
    val conf = qe.sparkSession.sessionState.conf
    if (conf.adaptiveExecutionEnabled) return Nil // AQE already coalesces partitions at runtime

    val hasShuffle =
      try qe.executedPlan.find(_.isInstanceOf[ShuffleExchangeExec]).isDefined
      catch { case _: Throwable => false }
    if (!hasShuffle) return Nil

    val current = conf.numShufflePartitions
    val sizeBytes = sizeOf(analyzed)
    val target = BigInt(config.autofixTargetPartitionBytes)
    if (sizeBytes <= 0 || target <= 0) return Nil

    val suggested = math.max(1, (sizeBytes / target).toInt)
    val ratio = current.toDouble / suggested
    if (ratio >= 3.0)      Seq(CoalesceHint(suggested))     // far too many small partitions
    else if (ratio <= 0.33) Seq(RepartitionHint(suggested)) // far too few, oversized partitions
    else Nil
  }

  private def sizeOf(plan: LogicalPlan): BigInt =
    try plan.stats.sizeInBytes catch { case _: Throwable => BigInt(Long.MaxValue) }

  private def relationName(plan: LogicalPlan): Option[String] =
    plan.collectFirst { case SubqueryAlias(id, _) => id.name }
}
