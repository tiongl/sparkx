package com.sparkx.autofix

/**
 * The closed-loop tuning policy. Given the just-measured execution and fresh advice
 * from the plan analyzer, it records the attempt, tracks the best hint set found so
 * far, and decides which hints to apply on the *next* run — iterating until it runs
 * out of new candidates or hits the iteration budget, at which point it locks in the
 * best-known-good hints and marks the profile converged.
 */
object HintPolicy {

  /** A hinted run must be at least this much faster than the prior best to count as an improvement. */
  private val ImproveMargin = 0.98

  /**
   * Fold a completed execution into the profile and compute the next state.
   *
   * @param appliedHints hints that were actually applied this run (empty = baseline)
   * @param durationMs   measured wall-clock duration of this run
   * @param advice       candidate fixes derived from this run's plan (problems still present)
   * @param maxIterations tuning budget (number of hinted attempts before giving up)
   */
  def update(
    profile:       FixProfile,
    appliedHints:  Seq[Hint],
    durationMs:    Long,
    advice:        Seq[Hint],
    maxIterations: Int
  ): FixProfile = {
    val now = System.currentTimeMillis()
    val prevBest = profile.bestMs
    val improved = appliedHints.nonEmpty && prevBest.exists(b => durationMs < b * ImproveMargin)

    val attempts = profile.attempts :+ FixAttempt(appliedHints, durationMs, now, improved)
    val baselineMs = profile.baselineMs.orElse(Some(durationMs))

    // Strictly-fastest run wins, baseline included.
    val (bestMs, bestHints) =
      if (prevBest.isEmpty || durationMs < prevBest.get) (Some(durationMs), appliedHints)
      else (profile.bestMs, profile.bestHints)

    val triedSets: Set[Set[String]] = attempts.map(renderedSet).toSet
    val next =
      if (attempts.size > maxIterations) None
      else nextCandidate(bestHints, advice, triedSets)

    val (status, pending) = next match {
      case Some(cand) => (FixProfile.Optimizing, cand)
      case None       => (FixProfile.Converged, bestHints)
    }

    profile.copy(
      status       = status,
      baselineMs   = baselineMs,
      bestMs       = bestMs,
      bestHints    = bestHints,
      pendingHints = pending,
      attempts     = attempts,
      updatedTs    = now
    )
  }

  private def renderedSet(a: FixAttempt): Set[String] = a.hints.map(_.render).toSet

  /**
   * Produce the next untried hint set: first try applying each recommended fix on top of
   * the best-known-good set, then try tuning any numeric partitioning hint up/down.
   */
  private def nextCandidate(
    base:   Seq[Hint],
    advice: Seq[Hint],
    tried:  Set[Set[String]]
  ): Option[Seq[Hint]] = {
    val candidates = scala.collection.mutable.ArrayBuffer[Seq[Hint]]()

    // (a) Layer each recommended fix on top of the current best.
    advice.foreach(h => candidates += HintRewriter.dedupe(base :+ h))

    // (b) Tune an existing numeric partitioning hint by doubling / halving.
    base.collectFirst {
      case RepartitionHint(n) => n
      case CoalesceHint(n)    => n
    }.foreach { n =>
      Seq(n * 2, n / 2).filter(_ >= 1).foreach { m =>
        candidates += HintRewriter.dedupe(base.filterNot(_.key == "partitioning") :+ RepartitionHint(m))
      }
    }

    candidates.find { c =>
      val s = c.map(_.render).toSet
      s.nonEmpty && !tried.contains(s)
    }.map(_.toSeq)
  }
}
