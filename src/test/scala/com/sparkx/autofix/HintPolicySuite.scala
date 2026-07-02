package com.sparkx.autofix

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HintPolicySuite extends AnyFunSuite with Matchers {

  private val fp = "abc"

  test("baseline run with no problems converges immediately") {
    val p = HintPolicy.update(FixProfile.initial(fp, "SELECT 1"), Nil, 1000, advice = Nil, maxIterations = 5)
    p.baselineMs shouldBe Some(1000)
    p.bestMs shouldBe Some(1000)
    p.status shouldBe FixProfile.Converged
    p.pendingHints shouldBe empty
  }

  test("baseline run with advice queues the first fix to try next") {
    val p = HintPolicy.update(FixProfile.initial(fp, "q"), Nil, 1000,
      advice = Seq(RepartitionHint(50)), maxIterations = 5)
    p.status shouldBe FixProfile.Optimizing
    p.pendingHints shouldBe Seq(RepartitionHint(50))
    p.iterations shouldBe 1
  }

  test("an improving hinted run is recorded as the new best and tuning continues") {
    val base = HintPolicy.update(FixProfile.initial(fp, "q"), Nil, 1000,
      advice = Seq(RepartitionHint(50)), maxIterations = 5)
    val p = HintPolicy.update(base, Seq(RepartitionHint(50)), 500, advice = Nil, maxIterations = 5)
    p.bestMs shouldBe Some(500)
    p.bestHints shouldBe Seq(RepartitionHint(50))
    // next candidate tunes the partition count (double / halve)
    p.pendingHints should contain oneOf (RepartitionHint(100), RepartitionHint(25))
  }

  test("a regressing hinted run keeps the previous best") {
    val base = HintPolicy.update(FixProfile.initial(fp, "q"), Nil, 1000,
      advice = Seq(RepartitionHint(50)), maxIterations = 5)
    val better = HintPolicy.update(base, Seq(RepartitionHint(50)), 500, advice = Nil, maxIterations = 5)
    val worse = HintPolicy.update(better, better.pendingHints, 900, advice = Nil, maxIterations = 5)
    worse.bestMs shouldBe Some(500)
    worse.bestHints shouldBe Seq(RepartitionHint(50))
  }

  test("stops iterating and locks in the best hints once the budget is exhausted") {
    var p = HintPolicy.update(FixProfile.initial(fp, "q"), Nil, 1000,
      advice = Seq(RepartitionHint(50)), maxIterations = 1)
    p = HintPolicy.update(p, Seq(RepartitionHint(50)), 400, advice = Nil, maxIterations = 1)
    p.status shouldBe FixProfile.Converged
    p.bestHints shouldBe Seq(RepartitionHint(50))
    p.pendingHints shouldBe Seq(RepartitionHint(50)) // keep applying the winner
  }
}
