package org.apache.spark.sql.execution.sparkx

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences, Expression, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashPartitioning, Partitioning,
  UnknownPartitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, SortExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelation,
  HashedRelationBroadcastMode, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * A physical join operator that tries a broadcast hash join first and
 * automatically falls back to shuffled hash join (then sort-merge join)
 * on OOM or broadcast failure — all within `doExecute()`.
 *
 * Because the fallback happens inside the physical plan execution,
 * downstream operators remain in the same optimised query plan and
 * Catalyst can still push filters/projections through the join.
 *
 * The strategy chain is:
 *  1. '''Broadcast hash join''' — collect the build side and broadcast it.
 *  2. '''Shuffled hash join''' — hash-partition both sides, per-partition
 *     hash join (no broadcast needed).
 *  3. '''Sort-merge join''' — hash-partition + sort both sides, merge join.
 *
 * Produced by [[ResilientJoinStrategy]] when
 * `spark.sparkx.resilientJoin.enabled` is `true`.
 */
case class ResilientJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan
) extends BinaryExecNode {

  // SparkPlan already provides `log` via the Logging trait

  // ── Output schema ─────────────────────────────────────────────────────

  override def output: Seq[Attribute] = joinType match {
    case _: InnerLike        => left.output ++ right.output
    case LeftOuter           => left.output ++ right.output.map(_.withNullability(true))
    case RightOuter          => left.output.map(_.withNullability(true)) ++ right.output
    case FullOuter           => (left.output ++ right.output).map(_.withNullability(true))
    case j: ExistenceJoin    => left.output :+ j.exists
    case LeftExistence(_)    => left.output
    case x =>
      throw new IllegalArgumentException(
        s"ResilientJoinExec does not support join type: $x")
  }

  // We don't constrain child distribution — we handle it internally.
  override def requiredChildDistribution: Seq[Distribution] =
    Seq(UnspecifiedDistribution, UnspecifiedDistribution)

  // Output partitioning/ordering are unknown since the strategy is dynamic.
  override def outputPartitioning: Partitioning =
    UnknownPartitioning(left.outputPartitioning.numPartitions)
  override def outputOrdering: Seq[SortOrder] = Nil

  // ── Metrics ───────────────────────────────────────────────────────────

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "strategyUsed" -> SQLMetrics.createMetric(sparkContext,
      "strategy used (1=broadcast, 2=shuffled-hash, 3=sort-merge)")
  )

  // ── Build-side selection ──────────────────────────────────────────────

  private def buildSide: BuildSide = {
    // For outer joins, the build side must be the non-preserved side:
    // left_outer preserves left → build right
    // right_outer preserves right → build left
    joinType match {
      case LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => BuildRight
      case RightOuter => BuildLeft
      case _ =>
        val leftSize  = left.logicalLink.map(_.stats.sizeInBytes.toLong).getOrElse(Long.MaxValue)
        val rightSize = right.logicalLink.map(_.stats.sizeInBytes.toLong).getOrElse(Long.MaxValue)
        if (leftSize <= rightSize) BuildLeft else BuildRight
    }
  }

  private def buildPlan: SparkPlan = if (buildSide == BuildLeft) left else right
  private def streamPlan: SparkPlan = if (buildSide == BuildLeft) right else left
  private def buildKeys: Seq[Expression] = if (buildSide == BuildLeft) leftKeys else rightKeys

  // ── Execution with fallback chain ─────────────────────────────────────

  override protected def doExecute(): RDD[InternalRow] = {
    // 1. Try broadcast hash join
    tryBroadcast().getOrElse {
      // 2. Try shuffled hash join
      tryShuffledHash().getOrElse {
        // 3. Last resort: sort-merge join
        trySortMerge()
      }
    }
  }

  /** Try broadcast hash join — collect build side and broadcast it. */
  private def tryBroadcast(): Option[RDD[InternalRow]] = {
    val threshold = ResilientJoinConf.getLong(conf,
      ResilientJoinConf.BROADCAST_THRESHOLD,
      ResilientJoinConf.BROADCAST_THRESHOLD_DEFAULT)

    // Quick size check — skip broadcast if build side is clearly too large
    val buildSize = buildPlan.logicalLink
      .map(_.stats.sizeInBytes.toLong).getOrElse(Long.MaxValue)
    if (buildSize > threshold) {
      log.info(s"ResilientJoinExec: build side too large ($buildSize > $threshold), " +
        "skipping broadcast")
      return None
    }

    try {
      log.info("ResilientJoinExec: attempting broadcast hash join")

      // Broadcast hash join for outer joins requires codegen preparation
      // that is not available when creating nodes manually.
      // Skip broadcast for non-inner-like joins.
      joinType match {
        case _: InnerLike | LeftSemi | LeftAnti | _: ExistenceJoin => // ok
        case _ =>
          log.info("ResilientJoinExec: skipping broadcast for outer joins")
          return None
      }

      // Bind build keys to the build plan's output so they can be evaluated
      // against InternalRow data (unbound AttributeReferences are Unevaluable)
      val boundBuildKeys = buildKeys.map(
        BindReferences.bindReference(_, buildPlan.output))
      val bcMode = HashedRelationBroadcastMode(boundBuildKeys, isNullAware = false)
      val bcExchange = BroadcastExchangeExec(bcMode, buildPlan)

      // Force the broadcast — this is where OOM surfaces
      bcExchange.executeBroadcast[HashedRelation]()

      val broadcastJoin = if (buildSide == BuildLeft) {
        BroadcastHashJoinExec(leftKeys, rightKeys, joinType, BuildLeft,
          condition, bcExchange, right)
      } else {
        BroadcastHashJoinExec(leftKeys, rightKeys, joinType, BuildRight,
          condition, left, bcExchange)
      }

      val rdd = broadcastJoin.execute()
      longMetric("strategyUsed").add(1)
      log.info("ResilientJoinExec: broadcast hash join succeeded")
      Some(rdd)
    } catch {
      case e: Exception if isOomOrBroadcastError(e) =>
        log.warn(s"ResilientJoinExec: broadcast failed " +
          s"(${e.getClass.getSimpleName}), trying shuffled hash join")
        None
    }
  }

  /** Try shuffled hash join — hash-partition both sides. */
  private def tryShuffledHash(): Option[RDD[InternalRow]] = {
    try {
      log.info("ResilientJoinExec: attempting shuffled hash join")
      val numParts = conf.numShufflePartitions
      val leftShuffle  = ShuffleExchangeExec(
        HashPartitioning(leftKeys, numParts), left)
      val rightShuffle = ShuffleExchangeExec(
        HashPartitioning(rightKeys, numParts), right)

      val shuffledJoin = ShuffledHashJoinExec(leftKeys, rightKeys, joinType,
        buildSide, condition, leftShuffle, rightShuffle)

      val rdd = shuffledJoin.execute()
      longMetric("strategyUsed").add(2)
      log.info("ResilientJoinExec: shuffled hash join succeeded")
      Some(rdd)
    } catch {
      case e: Exception =>
        log.warn(s"ResilientJoinExec: shuffled hash join failed " +
          s"(${e.getClass.getSimpleName}), trying sort-merge join")
        None
    }
  }

  /** Sort-merge join — hash-partition + sort both sides. */
  private def trySortMerge(): RDD[InternalRow] = {
    log.info("ResilientJoinExec: falling back to sort-merge join")
    val numParts = conf.numShufflePartitions

    val leftShuffle  = ShuffleExchangeExec(
      HashPartitioning(leftKeys, numParts), left)
    val rightShuffle = ShuffleExchangeExec(
      HashPartitioning(rightKeys, numParts), right)

    val leftSort  = SortExec(
      leftKeys.map(SortOrder(_, Ascending)), global = false, leftShuffle)
    val rightSort = SortExec(
      rightKeys.map(SortOrder(_, Ascending)), global = false, rightShuffle)

    val smjExec = SortMergeJoinExec(leftKeys, rightKeys, joinType,
      condition, leftSort, rightSort)

    longMetric("strategyUsed").add(3)
    smjExec.execute()
  }

  // ── OOM detection ─────────────────────────────────────────────────────

  private def isOomOrBroadcastError(t: Throwable): Boolean = {
    var current: Throwable = t
    while (current != null) {
      val className = current.getClass.getName.toLowerCase
      val msg = Option(current.getMessage).getOrElse("").toLowerCase
      if (className.contains("outofmemory") ||
          msg.contains("not enough memory to build and broadcast") ||
          msg.contains("cannot broadcast the table")) {
        return true
      }
      current = current.getCause
    }
    false
  }

  // ── TreeNode plumbing ─────────────────────────────────────────────────

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    copy(left = newLeft, right = newRight)
}
