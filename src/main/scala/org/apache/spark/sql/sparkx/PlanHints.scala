package org.apache.spark.sql.sparkx

import com.sparkx.autofix._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

/**
 * Plan-level equivalents of SparkSQL hints. This is the heart of the unified (SQL + DataFrame)
 * auto-fix path: instead of rewriting SQL text, it injects the corresponding catalyst nodes
 * ([[Repartition]], [[RebalancePartitions]], join [[JoinHint]]s) directly into a resolved
 * [[LogicalPlan]], tags them as auto-fix-injected, and can later strip/detect them again.
 *
 * The tags let us (a) stay idempotent across analyzer fixed-point passes, (b) recover exactly
 * which hints were applied for a run, and (c) compute a *stable* fingerprint by stripping our
 * own nodes so a hinted plan and its un-hinted original share one identity.
 */
object PlanHints {

  /** Marks nodes that auto-fix injected, so they can be recognised and removed later. */
  val Injected = TreeNodeTag[Boolean]("sparkx.autofix.injected")

  /** True if the plan already carries auto-fix-injected nodes (used for idempotency). */
  def hasInjected(plan: LogicalPlan): Boolean =
    plan.find(_.getTagValue(Injected).contains(true)).isDefined

  // ── Injection ────────────────────────────────────────────────────────────────

  /** Apply the given hints to a resolved plan, tagging every injected node. */
  def apply(plan: LogicalPlan, hints: Seq[Hint]): LogicalPlan = {
    var p = plan
    hints.foreach {
      case BroadcastHint(tables) => tables.foreach(t => p = applyBroadcast(p, t))
      case _                     => // partitioning handled below
    }
    partitioningNode(p, hints).foreach { node =>
      node.setTagValue(Injected, value = true)
      p = node
    }
    p
  }

  private def partitioningNode(child: LogicalPlan, hints: Seq[Hint]): Option[LogicalPlan] =
    hints.collectFirst {
      case RepartitionHint(n) => Repartition(n, shuffle = true, child)
      case CoalesceHint(n)    => Repartition(n, shuffle = false, child)
      case RebalanceHint      => RebalancePartitions(Nil, child)
    }

  private def applyBroadcast(plan: LogicalPlan, table: String): LogicalPlan = {
    var done = false
    plan.transformUp {
      case j: Join if !done && j.hint == JoinHint.NONE =>
        val hinted =
          if (containsRelation(j.left, table))
            Some(j.copy(hint = JoinHint(Some(HintInfo(Some(BROADCAST))), None)))
          else if (containsRelation(j.right, table))
            Some(j.copy(hint = JoinHint(None, Some(HintInfo(Some(BROADCAST))))))
          else None
        hinted match {
          case Some(nj) => done = true; nj.setTagValue(Injected, value = true); nj
          case None     => j
        }
    }
  }

  // ── Stripping / detection ─────────────────────────────────────────────────────

  /**
   * Remove auto-fix-injected nodes, returning the pristine plan and the hints that had been
   * applied. Only nodes tagged [[Injected]] are touched, so user-authored repartitions and
   * hints are preserved.
   */
  def strip(plan: LogicalPlan): (LogicalPlan, Seq[Hint]) = {
    val detected = scala.collection.mutable.ArrayBuffer[Hint]()

    // Peel injected partitioning wrappers from the root.
    var p = plan
    var peeling = true
    while (peeling) {
      p match {
        case r: Repartition if r.getTagValue(Injected).contains(true) =>
          detected += (if (r.shuffle) RepartitionHint(r.numPartitions) else CoalesceHint(r.numPartitions))
          p = r.child
        case rb: RebalancePartitions if rb.getTagValue(Injected).contains(true) =>
          detected += RebalanceHint
          p = rb.child
        case _ => peeling = false
      }
    }

    // Clear injected join hints, recording the broadcast target.
    val cleared = p.transformUp {
      case j: Join if j.getTagValue(Injected).contains(true) =>
        val name =
          if (j.hint.leftHint.exists(_.strategy.contains(BROADCAST))) relationName(j.left)
          else if (j.hint.rightHint.exists(_.strategy.contains(BROADCAST))) relationName(j.right)
          else None
        name.foreach(n => detected += BroadcastHint(Seq(n)))
        j.copy(hint = JoinHint.NONE)
    }
    // `transformUp` re-copies tags from the original node onto rewritten ones, so strip our
    // marker from the resulting tree explicitly to keep the pristine plan tag-free.
    cleared.foreach(_.unsetTagValue(Injected))
    (cleared, detected.toSeq)
  }

  // ── Fingerprint ────────────────────────────────────────────────────────────────

  /**
   * A stable, literal-insensitive identity for a query, computed from its plan. Auto-fix nodes
   * are stripped first (so a hinted run and its original match), literal values are nulled (so
   * re-runs with different parameters match), and the result is canonicalized.
   */
  def fingerprintOf(plan: LogicalPlan): String = {
    val (pristine, _) = strip(plan)
    val normalized =
      try pristine.transformAllExpressions { case l: Literal => Literal(null, l.dataType) }
      catch { case _: Throwable => pristine }
    val canonical =
      try normalized.canonicalized.toString
      catch { case _: Throwable => normalized.toString }
    val md = java.security.MessageDigest.getInstance("SHA-256")
    md.digest(canonical.getBytes("UTF-8")).take(16).map(b => f"${b & 0xff}%02x").mkString
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def containsRelation(plan: LogicalPlan, table: String): Boolean =
    plan.collectFirst {
      case SubqueryAlias(id, _) if id.name.equalsIgnoreCase(table) => true
    }.isDefined

  private def relationName(plan: LogicalPlan): Option[String] =
    plan.collectFirst { case SubqueryAlias(id, _) => id.name }
}
