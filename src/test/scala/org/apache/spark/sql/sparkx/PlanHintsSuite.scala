package org.apache.spark.sql.sparkx

import com.sparkx.autofix._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
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
}
