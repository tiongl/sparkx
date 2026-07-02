package com.sparkx.autofix

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HintRewriterSuite extends AnyFunSuite with Matchers {

  test("inserts a hint block after the outer SELECT") {
    val out = HintRewriter.rewrite("SELECT a FROM t", Seq(RepartitionHint(200)))
    out shouldBe "SELECT /*+ REPARTITION(200) */ a FROM t"
  }

  test("renders multiple hints comma-separated") {
    val out = HintRewriter.rewrite("SELECT a FROM t JOIN u ON t.k = u.k",
      Seq(BroadcastHint(Seq("u")), RepartitionHint(64)))
    out should include ("/*+ BROADCAST(u), REPARTITION(64) */")
  }

  test("targets the outer SELECT, not a CTE or sub-select") {
    val sql = "WITH c AS (SELECT x FROM base) SELECT a FROM c"
    val out = HintRewriter.rewrite(sql, Seq(BroadcastHint(Seq("c"))))
    out shouldBe "WITH c AS (SELECT x FROM base) SELECT /*+ BROADCAST(c) */ a FROM c"
  }

  test("merges into an existing hint block, replacing same-category hints") {
    val sql = "SELECT /*+ COALESCE(10) */ a FROM t"
    val out = HintRewriter.rewrite(sql, Seq(RepartitionHint(200)))
    // COALESCE and REPARTITION share the 'partitioning' category, so COALESCE is replaced
    out shouldBe "SELECT /*+ REPARTITION(200) */ a FROM t"
  }

  test("preserves unrelated existing hints when merging") {
    val sql = "SELECT /*+ BROADCAST(t) */ a FROM t JOIN u ON t.k = u.k"
    val out = HintRewriter.rewrite(sql, Seq(RepartitionHint(50)))
    out should include ("BROADCAST(t)")
    out should include ("REPARTITION(50)")
  }

  test("de-duplicates within a hint set, last per category wins") {
    HintRewriter.dedupe(Seq(RepartitionHint(10), CoalesceHint(20))) shouldBe Seq(CoalesceHint(20))
  }

  test("empty hint list returns the query unchanged") {
    HintRewriter.rewrite("SELECT a FROM t", Nil) shouldBe "SELECT a FROM t"
  }

  test("no SELECT means no rewrite") {
    val sql = "INSERT INTO t VALUES (1)"
    HintRewriter.rewrite(sql, Seq(RepartitionHint(1))) shouldBe sql
  }
}
