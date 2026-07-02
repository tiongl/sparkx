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

object Hint {
  private val Broadcast   = """(?i)\s*BROADCAST\s*\(([^)]*)\)\s*""".r
  private val Repartition = """(?i)\s*REPARTITION\s*\(\s*(\d+)\s*\)\s*""".r
  private val Coalesce    = """(?i)\s*COALESCE\s*\(\s*(\d+)\s*\)\s*""".r
  private val Rebalance   = """(?i)\s*REBALANCE\s*""".r

  /** Parse a single rendered hint string (as stored in a fix profile) back into a [[Hint]]. */
  def parse(s: String): Option[Hint] = s match {
    case Broadcast(tables) =>
      val ts = tables.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      if (ts.isEmpty) None else Some(BroadcastHint(ts))
    case Repartition(n) => Some(RepartitionHint(n.toInt))
    case Coalesce(n)    => Some(CoalesceHint(n.toInt))
    case Rebalance()    => Some(RebalanceHint)
    case _              => None
  }

  /** Render a hint as the equivalent DataFrame/Dataset API call, for user-facing advice. */
  def dataframeOp(h: Hint): String = h match {
    case BroadcastHint(tables) => s"broadcast(${tables.mkString(", ")})"
    case RepartitionHint(n)    => s".repartition($n)"
    case CoalesceHint(n)       => s".coalesce($n)"
    case RebalanceHint         => """.hint("rebalance")"""
  }
}
