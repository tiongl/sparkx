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
    val hints = Seq(BroadcastHint(Seq("u")), RepartitionHint(64), CoalesceHint(4), RebalanceHint,
      SplitBroadcastHint("orders", 4), SaltedJoinHint("dim", 16),
      TargetedSaltHint("dim", 16, Seq("1", "42", "7")))
    hints.foreach(h => Hint.parse(h.render) shouldBe Some(h))
  }

  test("targeted salt renders hot keys and round-trips") {
    TargetedSaltHint("dim", 16, Seq("1", "42")).render shouldBe "TARGETED_SALT(dim, 16, 1|42)"
    TargetedSaltHint("dim", 16, Seq("1", "42")).key    shouldBe "skew:dim"
    Hint.parse("TARGETED_SALT(dim, 16, 1|42)") shouldBe Some(TargetedSaltHint("dim", 16, Seq("1", "42")))
    // TARGETED_SALT must not be mis-parsed as the shorter SALT hint.
    Hint.parse("TARGETED_SALT(dim, 16, 1|42)") should not be Some(SaltedJoinHint("dim", 16))
  }

  test("skew hints render as sparkx pseudo-hints keyed per table") {
    SplitBroadcastHint("orders", 4).render shouldBe "SPLIT_BROADCAST(orders, 4)"
    SaltedJoinHint("dim", 16).render       shouldBe "SALT(dim, 16)"
    // Both share a per-table 'skew' category so at most one is proposed per table at a time.
    SplitBroadcastHint("orders", 4).key shouldBe "skew:orders"
    SaltedJoinHint("dim", 16).key       shouldBe "skew:dim"
  }

  test("skew hints are injectable (not advisory)") {
    Seq(SplitBroadcastHint("orders", 4), SaltedJoinHint("dim", 16))
      .foreach(_.advisory shouldBe false)
  }

  test("no hints are advisory") {
    Seq(BroadcastHint(Seq("u")), RepartitionHint(10), CoalesceHint(2), RebalanceHint)
      .foreach(_.advisory shouldBe false)
  }

  test("skew hints map to the sparkx DataFrame join API") {
    Hint.dataframeOp(SplitBroadcastHint("orders", 4)) should include (".splitBroadcastJoin(")
    Hint.dataframeOp(SplitBroadcastHint("orders", 4)) should include ("maxSplits = 4")
    Hint.dataframeOp(SaltedJoinHint("dim", 16))       should include (".autoSaltJoin(")
    Hint.dataframeOp(SaltedJoinHint("dim", 16))       should include ("saltFactor = 16")
  }
}
