package com.sparkx

import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkXConfigSuite extends AnyFunSuite with Matchers {

  test("skew-resolution keys default sensibly") {
    val c = SparkXConfig.fromConf(new SparkConf(false))
    c.autofixSkewFactor shouldBe 10.0
    c.autofixSkewBroadcastMaxBytes shouldBe (100L * 1024 * 1024)
    c.autofixSaltFactor shouldBe 16
    // The artificial skew-broadcast ceiling is larger than the plain broadcast ceiling.
    c.autofixSkewBroadcastMaxBytes should be > c.autofixBroadcastMaxBytes
  }

  test("skew-resolution keys are overridable") {
    val conf = new SparkConf(false)
      .set(SparkXConfig.AUTOFIX_SKEW_FACTOR, "1.0")
      .set(SparkXConfig.AUTOFIX_SKEW_BROADCAST_MAX_BYTES, "209715200")
      .set(SparkXConfig.AUTOFIX_SALT_FACTOR, "32")
    val c = SparkXConfig.fromConf(conf)
    c.autofixSkewFactor shouldBe 1.0
    c.autofixSkewBroadcastMaxBytes shouldBe 209715200L
    c.autofixSaltFactor shouldBe 32
  }
}
