package com.sparkx.autofix

/** One measured execution of a query under a specific hint set. */
case class FixAttempt(hints: Seq[Hint], durationMs: Long, ts: Long, improved: Boolean)

/**
 * The persisted learning state for a single query fingerprint: the baseline (un-hinted)
 * timing, the best hint set found so far, the hints to try on the next run, and the
 * history of attempts that drives iterative tuning.
 */
case class FixProfile(
  fingerprint:  String,
  sample:       String,          // a representative raw SQL, for display
  status:       String,          // learning | optimizing | converged
  baselineMs:   Option[Long],
  bestMs:       Option[Long],
  bestHints:    Seq[Hint],
  pendingHints: Seq[Hint],        // hints the parser applies on the next execution
  attempts:     Seq[FixAttempt],
  updatedTs:    Long,
  baselinePlan: Option[String] = None, // un-fixed plan (captured on the first, un-hinted run)
  currentPlan:  Option[String] = None, // plan from the most recent run
  recommendations: Seq[Hint] = Nil     // advisory skew-resolution strategies (applied via DataFrame API)
) {
  def iterations: Int = attempts.size
}

object FixProfile {
  val Learning   = "learning"
  val Optimizing = "optimizing"
  val Converged  = "converged"

  def initial(fingerprint: String, sample: String): FixProfile =
    FixProfile(fingerprint, sample, Learning, None, None, Nil, Nil, Nil, System.currentTimeMillis())
}
