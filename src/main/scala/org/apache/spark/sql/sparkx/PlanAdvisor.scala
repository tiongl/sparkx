package org.apache.spark.sql.sparkx

import com.sparkx.SparkXConfig
import com.sparkx.autofix._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec

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
    out ++= skewHints(qe, config, discover = true)._1 // injectable skew hints join the measured tuning loop
    HintRewriter.dedupe(out.toSeq).filterNot(_.advisory)
  }

  /**
   * Advisory skew-resolution recommendations: skew strategies for join shapes that *cannot*
   * be injected as a plan hint (e.g. outer/anti joins), surfaced for the user to apply through
   * the DataFrame API. Injectable shapes flow through [[advise]] instead and are auto-tuned.
   */
  def recommendations(qe: QueryExecution, config: SparkXConfig): Seq[Hint] =
    skewHints(qe, config, discover = false)._2

  /**
   * Detect skew-prone equi-joins and, for each, produce a skew-resolution hint. A join is
   * skew-prone when its physical plan uses a sort-merge join (a real shuffle join, not a
   * broadcast) — or when skew is *forced* via `autofixSkewFactor <= 1` — and its build (smaller)
   * side is bigger than the size ratio `autofixSkewFactor` tolerates. The build side's size then
   * selects the strategy (double-broadcast below the `autofixSkewBroadcastMaxBytes` ceiling,
   * salting above). Results are partitioned into `(injectable, advisory)` by whether the join
   * type is one [[PlanHints]] can safely rewrite: split-broadcast supports inner (either side) and
   * left-semi (build on the right); salting supports inner. Everything else is advisory-only.
   */
  def skewHints(qe: QueryExecution, config: SparkXConfig, discover: Boolean = false): (Seq[Hint], Seq[Hint]) = {
    val analyzed = try qe.analyzed catch { case _: Throwable => null }
    if (analyzed == null) return (Nil, Nil)

    val forced = config.autofixSkewFactor <= 1.0
    val hasSortMergeJoin =
      try qe.executedPlan.find(_.isInstanceOf[SortMergeJoinExec]).isDefined
      catch { case _: Throwable => false }
    if (!forced && !hasSortMergeJoin) return (Nil, Nil)

    val broadcastMax = BigInt(config.autofixBroadcastMaxBytes)
    val skewCeiling  = BigInt(config.autofixSkewBroadcastMaxBytes)

    val injectable = scala.collection.mutable.ArrayBuffer[Hint]()
    val advisory   = scala.collection.mutable.ArrayBuffer[Hint]()
    analyzed.collect { case j: Join if j.condition.isDefined => j }.foreach { j =>
      val leftSize  = sizeOf(j.left)
      val rightSize = sizeOf(j.right)
      val buildIsRight = rightSize <= leftSize
      val buildSide = if (buildIsRight) j.right else j.left
      val skewedSide = if (buildIsRight) j.left else j.right
      relationName(buildSide).foreach { table =>
        skewStrategy(table, leftSize.min(rightSize), leftSize.max(rightSize), broadcastMax,
          skewCeiling, config.autofixSkewFactor, config.autofixSaltFactor, forced).foreach { hint0 =>
          // Prefer targeted salting (salt only discovered hot keys) over uniform salting when
          // enabled and the skewed side yields hot keys via sampling.
          val hint = hint0 match {
            case SaltedJoinHint(t, n) if j.joinType == Inner && discover && config.autofixSkewTargeted =>
              val hotKeys = PlanHints.sideKeys(j.condition.get, skewedSide).headOption
                .map(k => SkewKeyDiscovery.discover(qe.sparkSession, skewedSide, k, config))
                .getOrElse(Nil)
              if (hotKeys.nonEmpty) TargetedSaltHint(t, n, hotKeys) else hint0
            case _ => hint0
          }
          val supported = hint match {
            case _: SplitBroadcastHint => j.joinType match {
              case Inner    => true
              case LeftSemi => buildIsRight
              case _        => false
            }
            case _: SaltedJoinHint   => j.joinType == Inner
            case _: TargetedSaltHint => j.joinType == Inner
            case _                   => false
          }
          if (supported) injectable += hint else advisory += hint
        }
      }
    }
    (injectable.distinct.toSeq, advisory.distinct.toSeq)
  }

  /**
   * Pure strategy selection for a single skew-prone equi-join, factored out for testing.
   * Returns the skew-resolution hint (double-broadcast or salting) for the build side `table`,
   * or None when the build side is plainly broadcastable or the join is not imbalanced enough.
   */
  private[sparkx] def skewStrategy(
    table:        String,
    smallSize:    BigInt,
    largeSize:    BigInt,
    broadcastMax: BigInt,
    skewCeiling:  BigInt,
    skewFactor:   Double,
    saltFactor:   Int,
    forced:       Boolean
  ): Option[Hint] = {
    if (smallSize < broadcastMax) None // plain broadcast already covers a tiny build side
    else if (!forced && !(largeSize >= smallSize * BigDecimal(skewFactor).toBigInt)) None
    else if (smallSize <= skewCeiling) {
      // "double broadcast": split the mid-sized build side into broadcastable chunks.
      val splits = math.max(2,
        (smallSize / broadcastMax + (if (smallSize % broadcastMax > 0) 1 else 0)).toInt)
      Some(SplitBroadcastHint(table, splits))
    } else {
      Some(SaltedJoinHint(table, saltFactor))
    }
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
