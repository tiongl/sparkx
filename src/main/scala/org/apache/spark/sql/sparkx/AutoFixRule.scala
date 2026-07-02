package org.apache.spark.sql.sparkx

import com.sparkx.SparkXConfig
import com.sparkx.autofix.FixProfileStore
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * The unified auto-fix injection point. Registered via `injectPostHocResolutionRule`, it runs
 * during analysis on every resolved plan — whether that plan came from SQL text or from the
 * DataFrame/Dataset API — which is what lets a single mechanism cover both.
 *
 * For each plan it computes a stable, literal-insensitive fingerprint ([[PlanHints.fingerprintOf]]),
 * looks up the learned [[com.sparkx.autofix.FixProfile]], and — when applying is enabled and not
 * in shadow mode — injects the profile's pending hints as catalyst nodes. In shadow mode it leaves
 * the plan untouched (the learner still records what *would* have been proposed). Idempotency is
 * guaranteed by tagging injected nodes and bailing out if any are already present.
 */
class AutoFixRule(config: SparkXConfig, store: FixProfileStore) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!config.autofixApplyEnabled) return plan
    if (PlanHints.hasInjected(plan)) return plan // already fixed this pass
    try {
      val fingerprint = PlanHints.fingerprintOf(plan)
      store.load(fingerprint) match {
        case Some(profile) if profile.pendingHints.nonEmpty =>
          PlanHints.apply(plan, profile.pendingHints)
        case _ => plan
      }
    } catch {
      case e: Throwable =>
        logWarning(s"[sparkx] AutoFixRule skipped: ${e.getClass.getName}: ${e.getMessage}")
        plan
    }
  }
}
