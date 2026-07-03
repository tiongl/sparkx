package org.apache.spark.sql.sparkx

import com.sparkx.SparkXConfig
import com.sparkx.autofix.{FixProfile, FixProfileStore, HintPolicy}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * A [[QueryExecutionListener]] that closes the auto-fix loop for both SQL and DataFrame queries.
 * On each successful run it fingerprints the analyzed plan the *same* way [[AutoFixRule]] does
 * ([[PlanHints.fingerprintOf]]) so learn-side and fix-side agree on identity, uses
 * [[PlanHints.strip]] to recover exactly which hints were injected this run, captures the plan
 * (baseline on the first un-hinted run, current every run) for observability, asks [[PlanAdvisor]]
 * which problems remain, and folds all of it into the persisted [[FixProfile]] via [[HintPolicy]].
 *
 * Failures are swallowed: learning must never affect the host job.
 */
class AutoFixLearner(config: SparkXConfig, store: FixProfileStore) extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (!config.autofixLearnEnabled) return
    try {
      val analyzed = qe.analyzed
      // A plan that carries injected nodes but no stamped identity was hinted manually via SQL
      // (SparkXHintRule), not by us — don't fold that run into the learned profile.
      val identityOpt = PlanHints.identityFrom(analyzed)
      if (identityOpt.isEmpty && PlanHints.hasInjected(analyzed)) return
      // Prefer the identity the fix side stamped (needed for complex skew rewrites that strip
      // can't structurally reverse); fall back to structural strip for un-stamped runs.
      val (fingerprint, appliedHints, pristine) = identityOpt match {
        case Some((fp, hints)) => (fp, hints, analyzed)
        case None =>
          val fp = PlanHints.fingerprintOf(analyzed)
          val (pris, hints) = PlanHints.strip(analyzed)
          (fp, hints, pris)
      }
      if (fingerprint.isEmpty) return

      // Capture the *executed* physical plan so the profile shows the real effect of the hints
      // (e.g. SortMergeJoin -> BroadcastHashJoin). The baseline is the un-hinted run's physical
      // plan; the current is refreshed every run. Falling back to the logical plan text on error.
      val planStr = try qe.executedPlan.treeString
                    catch { case _: Throwable => try pristine.treeString catch { case _: Throwable => "" } }
      val sample = try qe.logical.treeString catch { case _: Throwable => "" }
      val durationMs = math.max(0L, durationNs / 1000000L)
      val advice = PlanAdvisor.advise(qe, config)
      val recommendations = try PlanAdvisor.recommendations(qe, config) catch { case _: Throwable => Nil }

      fingerprint.intern().synchronized {
        val existing = store.load(fingerprint)
          .getOrElse(FixProfile.initial(fingerprint, sample))
        val withSample = if (existing.sample.isEmpty && sample.nonEmpty)
          existing.copy(sample = sample) else existing
        // Capture the baseline plan on the first (un-hinted) observation; refresh current every run.
        val withPlans = withSample.copy(
          baselinePlan = withSample.baselinePlan.orElse(
            if (appliedHints.isEmpty) Some(planStr) else None),
          currentPlan = Some(planStr),
          recommendations = if (recommendations.nonEmpty) recommendations else withSample.recommendations
        )
        val updated = HintPolicy.update(
          withPlans, appliedHints, durationMs, advice, config.autofixMaxIterations)
        store.save(updated)
      }
    } catch {
      case e: Throwable =>
        System.err.println(s"[sparkx] AutoFixLearner error: ${e.getClass.getName}: ${e.getMessage}")
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
}
