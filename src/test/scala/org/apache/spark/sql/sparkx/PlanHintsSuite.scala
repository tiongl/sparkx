package org.apache.spark.sql.sparkx

import com.sparkx.autofix._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PlanHintsSuite extends AnyFunSuite with Matchers {

  private val la = AttributeReference("k", IntegerType)()
  private val lb = AttributeReference("k", IntegerType)()

  private def aliasA = SubqueryAlias("a", LocalRelation(la))
  private def aliasB = SubqueryAlias("b", LocalRelation(lb))

  private def join = Join(aliasA, aliasB, Inner, Some(EqualTo(la, lb)), JoinHint.NONE)

  test("apply broadcast hint tags the matching join side, strip recovers it") {
    val hinted = PlanHints.apply(join, Seq(BroadcastHint(Seq("b"))))
    PlanHints.hasInjected(hinted) shouldBe true

    val theJoin = hinted.collectFirst { case j: Join => j }.get
    theJoin.hint.rightHint.flatMap(_.strategy) shouldBe Some(BROADCAST)
    theJoin.hint.leftHint shouldBe None

    val (stripped, hints) = PlanHints.strip(hinted)
    hints should contain (BroadcastHint(Seq("b")))
    PlanHints.hasInjected(stripped) shouldBe false
  }

  test("apply repartition hint wraps root, strip recovers it") {
    val hinted = PlanHints.apply(join, Seq(RepartitionHint(64)))
    hinted shouldBe a[Repartition]
    hinted.asInstanceOf[Repartition].shuffle shouldBe true
    hinted.asInstanceOf[Repartition].numPartitions shouldBe 64

    val (stripped, hints) = PlanHints.strip(hinted)
    hints shouldBe Seq(RepartitionHint(64))
    stripped shouldBe a[Join]
  }

  test("coalesce hint is a non-shuffle repartition") {
    val hinted = PlanHints.apply(join, Seq(CoalesceHint(8)))
    val r = hinted.asInstanceOf[Repartition]
    r.shuffle shouldBe false
    val (_, hints) = PlanHints.strip(hinted)
    hints shouldBe Seq(CoalesceHint(8))
  }

  test("rebalance hint becomes RebalancePartitions") {
    val hinted = PlanHints.apply(join, Seq(RebalanceHint))
    hinted shouldBe a[RebalancePartitions]
    val (_, hints) = PlanHints.strip(hinted)
    hints shouldBe Seq(RebalanceHint)
  }

  test("combined broadcast + repartition round-trips through strip") {
    val hinted = PlanHints.apply(join, Seq(BroadcastHint(Seq("a")), RepartitionHint(32)))
    val (stripped, hints) = PlanHints.strip(hinted)
    hints should contain (BroadcastHint(Seq("a")))
    hints should contain (RepartitionHint(32))
    stripped shouldBe a[Join]
  }

  test("fingerprint is literal-insensitive and stable across strip") {
    val f1 = Filter(EqualTo(la, Literal(1, IntegerType)), aliasA)
    val f2 = Filter(EqualTo(la, Literal(999, IntegerType)), aliasA)
    PlanHints.fingerprintOf(f1) shouldBe PlanHints.fingerprintOf(f2)

    // A hinted plan shares the fingerprint of its un-hinted original.
    val hinted = PlanHints.apply(join, Seq(RepartitionHint(50)))
    PlanHints.fingerprintOf(hinted) shouldBe PlanHints.fingerprintOf(join)
  }

  test("user-authored (untagged) repartition is not stripped") {
    val userRepartition = Repartition(10, shuffle = true, join)
    val (stripped, hints) = PlanHints.strip(userRepartition)
    hints shouldBe empty
    stripped shouldBe theSameInstanceAs(userRepartition)
  }

  // ── Skew-resolution rewrites ────────────────────────────────────────────────

  test("split-broadcast injects a union of N broadcast joins over hash-filtered chunks") {
    val hinted = PlanHints.apply(join, Seq(SplitBroadcastHint("b", 3)))
    PlanHints.hasInjected(hinted) shouldBe true
    hinted shouldBe a[Union]
    val u = hinted.asInstanceOf[Union]
    u.children.size shouldBe 3
    u.children.foreach { c =>
      val j = c.asInstanceOf[Join]
      j.hint.rightHint.flatMap(_.strategy) shouldBe Some(BROADCAST)
      j.right shouldBe a[Filter]
    }
    // The rewrite preserves the original schema.
    hinted.output.map(_.name) shouldBe join.output.map(_.name)
  }

  test("split-broadcast leaves unsupported (outer) joins untouched") {
    val outer = Join(aliasA, aliasB, LeftOuter, Some(EqualTo(la, lb)), JoinHint.NONE)
    val res = PlanHints.apply(outer, Seq(SplitBroadcastHint("b", 3)))
    PlanHints.hasInjected(res) shouldBe false
    res.asInstanceOf[Join].joinType shouldBe LeftOuter
  }

  test("salted join injects a top projection that preserves the original schema") {
    val hinted = PlanHints.apply(join, Seq(SaltedJoinHint("b", 4)))
    PlanHints.hasInjected(hinted) shouldBe true
    hinted shouldBe a[Project]
    hinted.output.map(_.name) shouldBe join.output.map(_.name)
    // A 4-row salt-range local relation is cross-joined in to replicate the build side.
    hinted.collectFirst { case lr: LocalRelation if lr.data.size == 4 => lr }.isDefined shouldBe true
    hinted.collectFirst { case j: Join if j.joinType == Cross => j }.isDefined shouldBe true
  }

  test("salted join leaves non-inner joins untouched") {
    val outer = Join(aliasA, aliasB, LeftOuter, Some(EqualTo(la, lb)), JoinHint.NONE)
    val res = PlanHints.apply(outer, Seq(SaltedJoinHint("b", 4)))
    PlanHints.hasInjected(res) shouldBe false
  }

  test("targeted salt injects a top projection over a Generate-replicated build side") {
    val hinted = PlanHints.apply(join, Seq(TargetedSaltHint("b", 4, Seq("1", "7"))))
    PlanHints.hasInjected(hinted) shouldBe true
    hinted shouldBe a[Project]
    hinted.output.map(_.name) shouldBe join.output.map(_.name)
    // Build side is replicated via an explode Generate (hot keys only), not a Cross to a range.
    hinted.collectFirst { case g: Generate => g }.isDefined shouldBe true
    hinted.collectFirst { case j: Join if j.joinType == Cross => j }.isDefined shouldBe false
  }

  test("targeted salt is a no-op with empty hot keys or non-inner joins") {
    PlanHints.hasInjected(PlanHints.apply(join, Seq(TargetedSaltHint("b", 4, Nil)))) shouldBe false
    val outer = Join(aliasA, aliasB, LeftOuter, Some(EqualTo(la, lb)), JoinHint.NONE)
    PlanHints.hasInjected(PlanHints.apply(outer, Seq(TargetedSaltHint("b", 4, Seq("1"))))) shouldBe false
  }

  test("stamped identity round-trips fingerprint and applied hints") {
    val fp = PlanHints.fingerprintOf(join)
    val hints = Seq(SplitBroadcastHint("b", 3))
    val hinted = PlanHints.apply(join, hints)
    PlanHints.stampIdentity(hinted, fp, hints)
    val recovered = PlanHints.identityFrom(hinted)
    recovered shouldBe defined
    recovered.get._1 shouldBe fp
    recovered.get._2 shouldBe hints
  }
}
