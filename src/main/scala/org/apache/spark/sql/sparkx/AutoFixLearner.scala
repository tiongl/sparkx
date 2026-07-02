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
      val fingerprint = PlanHints.fingerprintOf(analyzed)
      if (fingerprint.isEmpty) return

      val (pristine, appliedHints) = PlanHints.strip(analyzed)
      val planStr = try pristine.treeString catch { case _: Throwable => "" }
      val sample = try qe.logical.treeString catch { case _: Throwable => "" }
      val durationMs = math.max(0L, durationNs / 1000000L)
      val advice = PlanAdvisor.advise(qe, config)

      fingerprint.intern().synchronized {
        val existing = store.load(fingerprint)
          .getOrElse(FixProfile.initial(fingerprint, sample))
        val withSample = if (existing.sample.isEmpty && sample.nonEmpty)
          existing.copy(sample = sample) else existing
        // Capture the baseline plan on the first (un-hinted) observation; refresh current every run.
        val withPlans = withSample.copy(
          baselinePlan = withSample.baselinePlan.orElse(
            if (appliedHints.isEmpty) Some(planStr) else None),
          currentPlan = Some(planStr)
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
