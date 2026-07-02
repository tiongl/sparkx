package org.apache.spark.ui.sparkx

import com.sparkx.analysis._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConfigAdvisorSuite extends AnyFunSuite with Matchers {

  private def gc(ratio: Double, gcMs: Long) =
    GCPressureIssue(Some(1), "stage-gc", ratio, gcMs, (gcMs / ratio).toLong)
  private def spill(diskBytes: Long) =
    ShuffleSpillIssue(Some(2), "stage-spill", diskBytes, 0L)
  private def straggler() =
    StragglerIssue(Some(3), "stage-strag", 10000.0, 4000.0)
  private def fetchWait() =
    HighFetchWaitIssue(Some(4), "stage-fetch", 0.4, 2000L)
  private def diskRead() =
    DiskShuffleReadIssue(Some(5), "stage-disk", 300L * 1024 * 1024, 500L * 1024 * 1024)
  private def slowSer() =
    SlowResultSerializationIssue(Some(6), "stage-ser", 1500L, 8)
  private def failures() =
    TaskFailuresIssue(Some(7), "stage-fail", 3, 100, 2000L)

  private def keys(recs: Seq[ConfigRecommendation]) = recs.map(_.configKey).toSet

  test("no GC or spill issues yields no recommendations") {
    ConfigAdvisor.recommend(Nil, Map.empty) shouldBe empty
    ConfigAdvisor.recommend(Seq(DataSkewIssue(Some(1), "s", 100, 10, 10.0)), Map.empty) shouldBe empty
  }

  test("GC pressure recommends earlier spill, off-heap and Kryo") {
    val recs = ConfigAdvisor.recommend(Seq(gc(0.15, 5000)), Map.empty)
    keys(recs) should contain ("spark.shuffle.spill.numElementsForceSpillThreshold")
    keys(recs) should contain ("spark.memory.offHeap.enabled")
    keys(recs) should contain ("spark.serializer")
  }

  test("off-heap and Kryo are not recommended when already configured") {
    val props = Map(
      "spark.memory.offHeap.enabled" -> "true",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer")
    val recs = ConfigAdvisor.recommend(Seq(gc(0.15, 5000)), props)
    keys(recs) should not contain "spark.memory.offHeap.enabled"
    keys(recs) should not contain "spark.serializer"
    // The spill-threshold advice (the direct GC relief) still stands.
    keys(recs) should contain ("spark.shuffle.spill.numElementsForceSpillThreshold")
  }

  test("critical GC with no spill recommends more executor memory") {
    val recs = ConfigAdvisor.recommend(Seq(gc(0.25, 9000)), Map.empty)
    val mem = recs.find(_.configKey == "spark.executor.memory")
    mem shouldBe defined
    mem.get.severity shouldBe Critical
    mem.get.relatedIssue shouldBe "GC Pressure"
  }

  test("shuffle spill recommends more executor memory") {
    val recs = ConfigAdvisor.recommend(Seq(spill(200L * 1024 * 1024)), Map.empty)
    keys(recs) should contain ("spark.executor.memory")
    recs.find(_.configKey == "spark.executor.memory").get.relatedIssue shouldBe "Shuffle Spill"
  }

  test("low memory fraction is flagged when spilling") {
    val recs = ConfigAdvisor.recommend(Seq(spill(100L * 1024 * 1024)), Map("spark.memory.fraction" -> "0.4"))
    keys(recs) should contain ("spark.memory.fraction")
  }

  test("default memory fraction is not flagged") {
    val recs = ConfigAdvisor.recommend(Seq(spill(100L * 1024 * 1024)), Map("spark.memory.fraction" -> "0.6"))
    keys(recs) should not contain "spark.memory.fraction"
  }

  test("co-occurring GC + spill labels the spill-threshold recommendation accordingly") {
    val recs = ConfigAdvisor.recommend(Seq(gc(0.15, 5000), spill(100L * 1024 * 1024)), Map.empty)
    val forceSpill = recs.find(_.configKey == "spark.shuffle.spill.numElementsForceSpillThreshold").get
    forceSpill.relatedIssue shouldBe "GC Pressure + Shuffle Spill"
  }

  test("recommendations are sorted by severity, most urgent first") {
    val recs = ConfigAdvisor.recommend(Seq(gc(0.25, 9000), spill(100L * 1024 * 1024)), Map.empty)
    val ranks = recs.map(r => ConfigRecommendation.rank(r.severity))
    ranks shouldBe ranks.sorted(Ordering[Int].reverse)
  }

  test("stragglers recommend enabling speculation") {
    val recs = ConfigAdvisor.recommend(Seq(straggler()), Map.empty)
    val spec = recs.find(_.configKey == "spark.speculation")
    spec shouldBe defined
    spec.get.recommendedValue shouldBe "true"
    spec.get.relatedIssue shouldBe "Straggler Tasks"
  }

  test("speculation not recommended when already enabled") {
    val recs = ConfigAdvisor.recommend(Seq(straggler()), Map("spark.speculation" -> "true"))
    keys(recs) should not contain "spark.speculation"
  }

  test("high fetch wait recommends larger in-flight size and the shuffle service") {
    val recs = ConfigAdvisor.recommend(Seq(fetchWait()), Map.empty)
    keys(recs) should contain ("spark.reducer.maxSizeInFlight")
    keys(recs) should contain ("spark.shuffle.service.enabled")
  }

  test("shuffle service not recommended when already enabled") {
    val recs = ConfigAdvisor.recommend(Seq(fetchWait()), Map("spark.shuffle.service.enabled" -> "true"))
    keys(recs) should contain ("spark.reducer.maxSizeInFlight")
    keys(recs) should not contain "spark.shuffle.service.enabled"
  }

  test("disk shuffle read recommends keeping blocks in memory") {
    val recs = ConfigAdvisor.recommend(Seq(diskRead()), Map.empty)
    keys(recs) should contain ("spark.maxRemoteBlockSizeFetchToMem")
  }

  test("slow serialization recommends Kryo even without GC pressure") {
    val recs = ConfigAdvisor.recommend(Seq(slowSer()), Map.empty)
    val kryo = recs.find(_.configKey == "spark.serializer")
    kryo shouldBe defined
    kryo.get.severity shouldBe Warning
    kryo.get.relatedIssue shouldBe "Slow (De)serialization"
  }

  test("task failures recommend more executor memory") {
    val recs = ConfigAdvisor.recommend(Seq(failures()), Map.empty)
    val mem = recs.find(_.configKey == "spark.executor.memory")
    mem shouldBe defined
    mem.get.relatedIssue shouldBe "Task Failures"
  }
}
