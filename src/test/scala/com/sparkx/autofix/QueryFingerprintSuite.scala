package com.sparkx.autofix

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class QueryFingerprintSuite extends AnyFunSuite with Matchers {

  test("literal-insensitive: different parameter values share a fingerprint") {
    val a = "SELECT * FROM t WHERE dt = '2024-01-01' AND n > 5"
    val b = "SELECT * FROM t WHERE dt = '2024-02-15' AND n > 900"
    QueryFingerprint.compute(a) shouldBe QueryFingerprint.compute(b)
  }

  test("whitespace and case are normalized away") {
    val a = "select  a,   b\nfrom   t"
    val b = "SELECT a, b FROM t"
    QueryFingerprint.compute(a) shouldBe QueryFingerprint.compute(b)
  }

  test("existing hint blocks and comments do not fork the fingerprint") {
    val a = "SELECT /*+ BROADCAST(t) */ a FROM t -- trailing comment"
    val b = "SELECT a FROM t"
    QueryFingerprint.compute(a) shouldBe QueryFingerprint.compute(b)
  }

  test("different query shapes have different fingerprints") {
    QueryFingerprint.compute("SELECT a FROM t") should not be
      QueryFingerprint.compute("SELECT a FROM u")
  }

  test("identifiers with digits are not treated as literals") {
    // col1 vs col2 are genuinely different columns and must differ
    QueryFingerprint.compute("SELECT col1 FROM t") should not be
      QueryFingerprint.compute("SELECT col2 FROM t")
  }

  test("null and empty are handled") {
    QueryFingerprint.normalize(null) shouldBe ""
    QueryFingerprint.compute("").length shouldBe 32
  }
}
