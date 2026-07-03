package org.apache.spark.sql.sparkx

import com.sparkx.autofix.{SaltedJoinHint, SplitBroadcastHint}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for [[PlanAdvisor]]'s pure skew-strategy selection. The [[PlanAdvisor.skewHints]]
 * entry point needs a live [[org.apache.spark.sql.execution.QueryExecution]]; the branching
 * logic is factored into [[PlanAdvisor.skewStrategy]] so it can be tested without a SparkSession.
 */
class PlanAdvisorSuite extends AnyFunSuite with Matchers {

  private val MB = 1024L * 1024L
  private val broadcastMax = BigInt(10 * MB)   // plain broadcast ceiling
  private val skewCeiling  = BigInt(100 * MB)  // artificial "double broadcast" ceiling
  private val skewFactor   = 10.0
  private val saltFactor   = 16

  private def strat(smallMB: Long, largeMB: Long, forced: Boolean = false) =
    PlanAdvisor.skewStrategy("t", BigInt(smallMB * MB), BigInt(largeMB * MB),
      broadcastMax, skewCeiling, skewFactor, saltFactor, forced)

  test("tiny build side is left to plain broadcast (no skew recommendation)") {
    strat(smallMB = 5, largeMB = 5000) shouldBe None
  }

  test("balanced join (not imbalanced enough) is not flagged as skew-prone") {
    // small = 40MB, large = 80MB, ratio 2 < skewFactor 10 => not skew-prone
    strat(smallMB = 40, largeMB = 80) shouldBe None
  }

  test("mid-sized build side on a skewed join => double broadcast (split-broadcast)") {
    // small = 40MB (> 10MB broadcast, <= 100MB ceiling), large = 4000MB (ratio 100 >= 10)
    // splits = ceil(40 / 10) = 4
    strat(smallMB = 40, largeMB = 4000) shouldBe Some(SplitBroadcastHint("t", 4))
  }

  test("split count rounds up for a non-multiple build side") {
    // small = 25MB => ceil(25 / 10) = 3
    strat(smallMB = 25, largeMB = 5000) shouldBe Some(SplitBroadcastHint("t", 3))
  }

  test("build side above the artificial ceiling => salting") {
    // small = 250MB (> 100MB ceiling), large = 5000MB
    strat(smallMB = 250, largeMB = 5000) shouldBe Some(SaltedJoinHint("t", 16))
  }

  test("forced skew flags an otherwise-balanced join") {
    // ratio 2 < 10, but forced=true bypasses the imbalance gate
    strat(smallMB = 40, largeMB = 80, forced = true) shouldBe Some(SplitBroadcastHint("t", 4))
  }

  test("forced skew still leaves a tiny build side to plain broadcast") {
    strat(smallMB = 5, largeMB = 5, forced = true) shouldBe None
  }
}
