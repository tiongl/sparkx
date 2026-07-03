package com.sparkx.autofix

/**
 * A SparkSQL join/partitioning hint that can be spliced into a query hint block.
 * Each hint knows how to [[render]] itself and exposes a [[key]] identifying its
 * category, so that a hint set contains at most one hint per category.
 */
sealed trait Hint {
  /** SparkSQL hint text, e.g. `BROADCAST(t)` or `REPARTITION(200)`. */
  def render: String
  /** Category key used for de-duplication/merging (e.g. `broadcast`, `partitioning`). */
  def key: String
  /**
   * Advisory hints describe a skew-resolution strategy that is applied through the
   * DataFrame API (see [[dataframeOp]]) rather than injected into the logical plan.
   * The closed-loop tuner never applies or measures advisory hints; they are surfaced
   * as recommendations only.
   */
  def advisory: Boolean = false
}

/** Broadcast (map-side) join hint for the listed relations. */
case class BroadcastHint(tables: Seq[String]) extends Hint {
  def render: String = s"BROADCAST(${tables.mkString(", ")})"
  def key: String = s"broadcast:${tables.mkString(",")}"
}

/** Shuffle to exactly `n` partitions. */
case class RepartitionHint(numPartitions: Int) extends Hint {
  def render: String = s"REPARTITION($numPartitions)"
  def key: String = "partitioning"
}

/** Reduce to `n` partitions without a full shuffle. */
case class CoalesceHint(numPartitions: Int) extends Hint {
  def render: String = s"COALESCE($numPartitions)"
  def key: String = "partitioning"
}

/** Let AQE rebalance partitions to even sizes. */
case object RebalanceHint extends Hint {
  def render: String = "REBALANCE"
  def key: String = "partitioning"
}

/**
 * Skew resolution: "N-way / double broadcast" — split the build side `table` into `splits`
 * hash-disjoint chunks and broadcast each chunk against the large side, unioning the partials.
 * Injected at the plan level for inner / left-semi joins (see [[org.apache.spark.sql.sparkx.PlanHints]]).
 */
case class SplitBroadcastHint(table: String, splits: Int) extends Hint {
  def render: String = s"SPLIT_BROADCAST($table, $splits)"
  def key: String = s"skew:$table"
}

/**
 * Skew resolution: salt the skewed side across `saltFactor` buckets and replicate the build
 * side `table`, so a hot key's rows spread across many reducers. Injected at the plan level
 * for inner equi-joins (see [[org.apache.spark.sql.sparkx.PlanHints]]).
 */
case class SaltedJoinHint(table: String, saltFactor: Int) extends Hint {
  def render: String = s"SALT($table, $saltFactor)"
  def key: String = s"skew:$table"
}

/**
 * Skew resolution: *targeted* salting. Like [[SaltedJoinHint]], but only the discovered hot
 * `hotKeys` are spread across `saltFactor` buckets — cold keys keep salt 0 and the build side
 * `table` is replicated for hot keys only, avoiding the ×saltFactor blow-up of uniform salting.
 * The hot key values are learned once (sampling) and carried as literals. Injected at the plan
 * level for inner equi-joins (see [[org.apache.spark.sql.sparkx.PlanHints]]).
 */
case class TargetedSaltHint(table: String, saltFactor: Int, hotKeys: Seq[String]) extends Hint {
  def render: String = s"TARGETED_SALT($table, $saltFactor, ${hotKeys.mkString("|")})"
  def key: String = s"skew:$table"
}

object Hint {
  private val Broadcast   = """(?i)\s*BROADCAST\s*\(([^)]*)\)\s*""".r
  private val Repartition = """(?i)\s*REPARTITION\s*\(\s*(\d+)\s*\)\s*""".r
  private val Coalesce    = """(?i)\s*COALESCE\s*\(\s*(\d+)\s*\)\s*""".r
  private val Rebalance   = """(?i)\s*REBALANCE\s*""".r
  private val SplitBcast  = """(?i)\s*SPLIT_BROADCAST\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)\s*""".r
  private val Salt        = """(?i)\s*SALT\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)\s*""".r
  private val TargetSalt  = """(?i)\s*TARGETED_SALT\s*\(\s*(.+?)\s*,\s*(\d+)\s*,\s*(.*?)\s*\)\s*""".r

  /** Parse a single rendered hint string (as stored in a fix profile) back into a [[Hint]]. */
  def parse(s: String): Option[Hint] = s match {
    case Broadcast(tables) =>
      val ts = tables.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      if (ts.isEmpty) None else Some(BroadcastHint(ts))
    case Repartition(n)    => Some(RepartitionHint(n.toInt))
    case Coalesce(n)       => Some(CoalesceHint(n.toInt))
    case SplitBcast(t, n)  => Some(SplitBroadcastHint(t.trim, n.toInt))
    case TargetSalt(t, n, keys) =>
      Some(TargetedSaltHint(t.trim, n.toInt, keys.split("\\|").map(_.trim).filter(_.nonEmpty).toSeq))
    case Salt(t, n)        => Some(SaltedJoinHint(t.trim, n.toInt))
    case Rebalance()       => Some(RebalanceHint)
    case _                 => None
  }

  /** Render a hint as the equivalent DataFrame/Dataset API call, for user-facing advice. */
  def dataframeOp(h: Hint): String = h match {
    case BroadcastHint(tables) => s"broadcast(${tables.mkString(", ")})"
    case RepartitionHint(n)    => s".repartition($n)"
    case CoalesceHint(n)       => s".coalesce($n)"
    case RebalanceHint         => """.hint("rebalance")"""
    case SplitBroadcastHint(t, n) => s"large.splitBroadcastJoin($t, keys, joinType, SplitBroadcastConfig(maxSplits = $n))"
    case SaltedJoinHint(t, n)     => s"skewed.autoSaltJoin($t, keys, joinType, AutoSaltJoinConfig(saltFactor = $n))"
    case TargetedSaltHint(t, n, _) => s"skewed.autoSaltJoin($t, keys, joinType, AutoSaltJoinConfig(saltFactor = $n))"
  }
}
