package org.apache.spark.sql.sparkx

import com.sparkx.autofix.{Hint, SaltedJoinHint, SplitBroadcastHint, TargetedSaltHint}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Makes the sparkx skew-resolution pseudo-hints usable directly in SQL text, e.g.
 * {{{ SELECT /*+ SPLIT_BROADCAST(dim, 6) */ ... }}} or {{{ SELECT /*+ SALT(dim, 16) */ ... }}}.
 *
 * Spark's own SQL parser turns any hint it doesn't recognise into an [[UnresolvedHint]] and later
 * silently drops it (`ResolveHints.RemoveAllHints`). We inject this rule as an analyzer *resolution*
 * rule — which runs before that removal — so we can catch our hint names once their child subtree is
 * resolved and rewrite them into the same catalyst plan that the auto-fix loop injects
 * ([[PlanHints.applySplitBroadcast]] / [[PlanHints.applySalt]]). Unknown hints are left untouched for
 * Spark to handle. Because the rewrite is tagged [[PlanHints.Injected]], the auto-fix rule then bails
 * out (user intent wins) and the learner skips the manually-hinted run.
 */
class SparkXHintRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case h: UnresolvedHint if h.child.resolved =>
      hintFor(h.name, h.parameters) match {
        case Some(hint) => PlanHints.apply(h.child, Seq(hint)) // drops the wrapper, injects the rewrite
        case None       => h                                   // not ours — let Spark resolve/remove it
      }
  }

  /** Map a recognised sparkx hint name + parameters to the corresponding [[Hint]]. */
  private def hintFor(name: String, params: Seq[Any]): Option[Hint] =
    name.toUpperCase match {
      case "SPLIT_BROADCAST" =>
        for (t <- strArg(params, 0); n <- intArg(params, 1)) yield SplitBroadcastHint(t, n)
      case "SALT" =>
        for (t <- strArg(params, 0); n <- intArg(params, 1)) yield SaltedJoinHint(t, n)
      case "TARGETED_SALT" =>
        // TARGETED_SALT(table, saltFactor, hot1, hot2, ...) — trailing params are hot key values.
        for (t <- strArg(params, 0); n <- intArg(params, 1)) yield {
          val keys = params.drop(2).flatMap(p => strArg(Seq(p), 0)).toSeq
          TargetedSaltHint(t, n, keys)
        }
      case _ => None
    }

  /** A relation-name argument arrives as an `UnresolvedAttribute` (bare identifier) or a string. */
  private def strArg(params: Seq[Any], i: Int): Option[String] =
    params.lift(i).flatMap {
      case a: UnresolvedAttribute        => a.nameParts.lastOption
      case s: String                     => Some(s)
      case l: Literal if l.value != null => Some(l.value.toString)
      case null                          => None
      case other                         => Some(other.toString)
    }

  /** A numeric argument arrives as an integer `Literal`. */
  private def intArg(params: Seq[Any], i: Int): Option[Int] =
    params.lift(i).flatMap {
      case l: Literal if l.value != null => asInt(l.value.toString)
      case n: Int                        => Some(n)
      case n: Long                       => Some(n.toInt)
      case s: String                     => asInt(s)
      case other                         => asInt(String.valueOf(other))
    }

  private def asInt(s: String): Option[Int] =
    try Some(s.trim.toInt) catch { case _: Throwable => None }
}
