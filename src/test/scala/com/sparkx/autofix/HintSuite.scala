package com.sparkx.autofix

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HintSuite extends AnyFunSuite with Matchers {

  test("dataframeOp renders the equivalent DataFrame API call") {
    Hint.dataframeOp(BroadcastHint(Seq("dim", "lookup"))) shouldBe "broadcast(dim, lookup)"
    Hint.dataframeOp(RepartitionHint(200))                shouldBe ".repartition(200)"
    Hint.dataframeOp(CoalesceHint(8))                     shouldBe ".coalesce(8)"
    Hint.dataframeOp(RebalanceHint)                       shouldBe """.hint("rebalance")"""
  }

  test("render / parse round-trip") {
    val hints = Seq(BroadcastHint(Seq("u")), RepartitionHint(64), CoalesceHint(4), RebalanceHint)
    hints.foreach(h => Hint.parse(h.render) shouldBe Some(h))
  }
}
