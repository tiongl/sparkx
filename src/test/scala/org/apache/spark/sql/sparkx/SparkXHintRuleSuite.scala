package org.apache.spark.sql.sparkx

import com.sparkx.autofix._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests that [[SparkXHintRule]] turns the sparkx skew pseudo-hints written in SQL text
 * (parsed by Spark into an [[UnresolvedHint]]) into the corresponding plan rewrite.
 */
class SparkXHintRuleSuite extends AnyFunSuite with Matchers {

  private val la = AttributeReference("k", IntegerType)()
  private val lb = AttributeReference("k", IntegerType)()
  private def aliasA = SubqueryAlias("a", LocalRelation(la))
  private def aliasB = SubqueryAlias("b", LocalRelation(lb))
  private def join = Join(aliasA, aliasB, Inner, Some(EqualTo(la, lb)), JoinHint.NONE)

  private val rule = new SparkXHintRule

  test("SPLIT_BROADCAST hint in SQL text rewrites to a union of broadcast joins") {
    val hint = UnresolvedHint("SPLIT_BROADCAST", Seq(UnresolvedAttribute("b"), Literal(3)), join)
    val out = rule(hint)
    out shouldBe a[Union]
    out.asInstanceOf[Union].children.size shouldBe 3
    PlanHints.hasInjected(out) shouldBe true
  }

  test("SALT hint in SQL text rewrites to a salted projection") {
    val hint = UnresolvedHint("SALT", Seq(UnresolvedAttribute("b"), Literal(4)), join)
    val out = rule(hint)
    out shouldBe a[Project]
    PlanHints.hasInjected(out) shouldBe true
  }

  test("TARGETED_SALT hint in SQL text carries hot keys and rewrites to a Generate-replicated join") {
    val hint = UnresolvedHint("TARGETED_SALT",
      Seq(UnresolvedAttribute("b"), Literal(4), Literal(1), Literal(7)), join)
    val out = rule(hint)
    out shouldBe a[Project]
    out.collectFirst { case g: Generate => g }.isDefined shouldBe true
    PlanHints.hasInjected(out) shouldBe true
  }

  test("hint names are matched case-insensitively") {
    val hint = UnresolvedHint("split_broadcast", Seq(UnresolvedAttribute("b"), Literal(2)), join)
    rule(hint) shouldBe a[Union]
  }

  test("unknown hints are left untouched for Spark to handle") {
    val hint = UnresolvedHint("SOMETHING_ELSE", Seq(UnresolvedAttribute("b")), join)
    rule(hint) shouldBe theSameInstanceAs(hint)
  }

  test("recognised hint with missing arguments is a no-op rewrite (wrapper dropped)") {
    // Only a table, no split count => hintFor returns None => the hint is left for Spark.
    val hint = UnresolvedHint("SPLIT_BROADCAST", Seq(UnresolvedAttribute("b")), join)
    rule(hint) shouldBe theSameInstanceAs(hint)
  }
}
