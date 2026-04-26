package org.apache.spark.ui.sparkx

import com.sparkx.SparkXConfig
import com.sparkx.analysis._
import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SuggestionDetectorSuite extends AnyFunSuite with Matchers {

  private val defaultConfig = SparkXConfig.fromConf(new SparkConf())

  private def detect(
    planDescription:    String,
    stageIds:           Set[Int] = Set(0),
    stageShuffleWrites: Map[Int, Long] = Map.empty,
    stageInputBytes:    Map[Int, Long] = Map.empty,
    existingIssues:     Seq[PerformanceIssue] = Seq.empty,
    shufflePartitions:  Int = 200,
    config:             SparkXConfig = defaultConfig
  ): Seq[OptimizationSuggestion] = {
    SuggestionDetector.detectForExecution(
      execId = 1L,
      planDescription = planDescription,
      stageIds = stageIds,
      stageShuffleWrites = stageShuffleWrites,
      stageInputBytes = stageInputBytes,
      existingIssues = existingIssues,
      shufflePartitions = shufflePartitions,
      config = config
    )
  }

  // ── 1. Broadcast join candidate ──────────────────────────────────────────
  test("detects broadcast join candidate when small shuffle side exists") {
    val plan = "+- SortMergeJoin [id#5], [id#15], Inner"
    val result = detect(
      plan,
      stageIds = Set(0, 1),
      stageShuffleWrites = Map(0 -> (50L * 1024 * 1024), 1 -> (2L * 1024 * 1024))
    )
    result.collect { case s: BroadcastJoinSuggestion => s } should have size 1
  }

  test("no broadcast suggestion when shuffle sides exceed threshold") {
    val plan = "+- SortMergeJoin [id#5], [id#15], Inner"
    val result = detect(
      plan,
      stageIds = Set(0, 1),
      stageShuffleWrites = Map(0 -> (200L * 1024 * 1024), 1 -> (150L * 1024 * 1024))
    )
    result.collect { case s: BroadcastJoinSuggestion => s } shouldBe empty
  }

  test("no broadcast suggestion when no joins in plan") {
    val plan = "+- Exchange hashpartitioning(id#5, 200)"
    val result = detect(
      plan,
      stageShuffleWrites = Map(0 -> (1L * 1024 * 1024))
    )
    result.collect { case s: BroadcastJoinSuggestion => s } shouldBe empty
  }

  // ── 2. Excessive shuffles ───────────────────────────────────────────────
  test("detects excessive shuffles when exchange count exceeds threshold") {
    val plan =
      """|+- Exchange hashpartitioning(a, 200)
         |+- Exchange hashpartitioning(b, 200)
         |+- Exchange hashpartitioning(c, 200)
         |+- Exchange hashpartitioning(d, 200)
         |+- Exchange hashpartitioning(e, 200)""".stripMargin
    val result = detect(plan)
    result.collect { case s: UnnecessaryShuffleSuggestion => s } should have size 1
    result.collect { case s: UnnecessaryShuffleSuggestion => s }.head.exchangeCount shouldBe 5
  }

  test("no excessive shuffle suggestion at or below threshold") {
    val plan =
      """|+- Exchange hashpartitioning(a, 200)
         |+- Exchange hashpartitioning(b, 200)
         |+- Exchange hashpartitioning(c, 200)
         |+- Exchange hashpartitioning(d, 200)""".stripMargin
    val result = detect(plan)
    result.collect { case s: UnnecessaryShuffleSuggestion => s } shouldBe empty
  }

  // ── 3. Missing AQE ──────────────────────────────────────────────────────
  test("detects missing AQE when exchanges exist without AdaptiveSparkPlan") {
    val plan = "+- Exchange hashpartitioning(id#5, 200)"
    val result = detect(plan)
    result.collect { case s: MissingAQESuggestion => s } should have size 1
  }

  test("no missing AQE when AdaptiveSparkPlan is present") {
    val plan =
      """|AdaptiveSparkPlan isFinalPlan=true
         |+- Exchange hashpartitioning(id#5, 200)""".stripMargin
    val result = detect(plan)
    result.collect { case s: MissingAQESuggestion => s } shouldBe empty
  }

  test("no missing AQE when no exchanges exist") {
    val plan = "+- FileScan parquet [id#0] PushedFilters: [], DataFilters: [], PartitionFilters: []"
    val result = detect(plan)
    result.collect { case s: MissingAQESuggestion => s } shouldBe empty
  }

  test("missing AQE with co-occurring skew issue has Warning severity") {
    val plan = "+- Exchange hashpartitioning(id#5, 200)"
    val skewIssue = DataSkewIssue(
      stageId = Some(0), stageName = "stage-0",
      maxDurationMs = 10000, medianDurationMs = 100,
      ratio = 100.0
    )
    val result = detect(plan, existingIssues = Seq(skewIssue))
    val aqe = result.collect { case s: MissingAQESuggestion => s }
    aqe should have size 1
    aqe.head.hasCoOccurringIssues shouldBe true
    aqe.head.severity shouldBe Warning
  }

  // ── 4. Cartesian product ────────────────────────────────────────────────
  test("detects CartesianProduct") {
    val plan = "+- CartesianProduct"
    val result = detect(plan)
    result.collect { case s: CartesianProductSuggestion => s } should have size 1
  }

  test("detects BroadcastNestedLoopJoin as cartesian") {
    val plan = "+- BroadcastNestedLoopJoin BuildRight, Cross"
    val result = detect(plan)
    result.collect { case s: CartesianProductSuggestion => s } should have size 1
  }

  // ── 5. Suboptimal file format ───────────────────────────────────────────
  test("detects CSV as suboptimal format") {
    val plan = "+- FileScan csv [id#0] DataFilters: [], PartitionFilters: [], PushedFilters: []"
    val result = detect(plan)
    result.collect { case s: SuboptimalFormatSuggestion => s } should have size 1
    result.collect { case s: SuboptimalFormatSuggestion => s }.head.format shouldBe "csv"
  }

  test("detects JSON as suboptimal format") {
    val plan = "+- FileScan json [id#0] DataFilters: [], PartitionFilters: [], PushedFilters: []"
    val result = detect(plan)
    result.collect { case s: SuboptimalFormatSuggestion => s } should have size 1
  }

  test("parquet is not flagged as suboptimal") {
    val plan = "+- FileScan parquet [id#0] DataFilters: [], PartitionFilters: [], PushedFilters: []"
    val result = detect(plan)
    result.collect { case s: SuboptimalFormatSuggestion => s } shouldBe empty
  }

  // ── 6. Missing partition pruning ────────────────────────────────────────
  test("detects missing partition pruning when filters exist but no partition filters") {
    val plan = "+- FileScan csv [id#0] DataFilters: [isnotnull(id#0)], PartitionFilters: [], PushedFilters: [IsNotNull(id)]"
    val result = detect(plan)
    result.collect { case s: MissingPartitionPruningSuggestion => s } should have size 1
  }

  test("no missing partition pruning when partition filters exist") {
    val plan = "+- FileScan parquet [id#0] DataFilters: [], PartitionFilters: [isnotnull(date#1)], PushedFilters: []"
    val result = detect(plan)
    result.collect { case s: MissingPartitionPruningSuggestion => s } shouldBe empty
  }

  test("no missing partition pruning when no filters at all") {
    val plan = "+- FileScan parquet [id#0] DataFilters: [], PartitionFilters: [], PushedFilters: []"
    val result = detect(plan)
    result.collect { case s: MissingPartitionPruningSuggestion => s } shouldBe empty
  }

  // ── 7. Python UDFs ──────────────────────────────────────────────────────
  test("detects Python UDF nodes") {
    val plan =
      """|+- BatchEvalPython [my_udf(id#0)]
         |+- BatchEvalPython [my_other_udf(id#0)]""".stripMargin
    val result = detect(plan)
    val udfSuggestions = result.collect { case s: PythonUDFSuggestion => s }
    udfSuggestions should have size 1
    udfSuggestions.head.count shouldBe 2
  }

  test("no Python UDF suggestion when none present") {
    val plan = "+- FileScan parquet [id#0] DataFilters: [], PartitionFilters: [], PushedFilters: []"
    val result = detect(plan)
    result.collect { case s: PythonUDFSuggestion => s } shouldBe empty
  }

  // ── 8. Repeated table scans ─────────────────────────────────────────────
  test("detects repeated scans of same format") {
    val plan =
      """|+- FileScan csv [a#0] DataFilters: [], PartitionFilters: [], PushedFilters: []
         |+- FileScan csv [b#1] DataFilters: [], PartitionFilters: [], PushedFilters: []""".stripMargin
    val result = detect(plan)
    result.collect { case s: RepeatedScanSuggestion => s } should have size 1
    result.collect { case s: RepeatedScanSuggestion => s }.head.scanCount shouldBe 2
  }

  test("no repeated scan when each format appears once") {
    val plan =
      """|+- FileScan csv [a#0] DataFilters: [], PartitionFilters: [], PushedFilters: []
         |+- FileScan parquet [b#1] DataFilters: [], PartitionFilters: [], PushedFilters: []""".stripMargin
    val result = detect(plan)
    result.collect { case s: RepeatedScanSuggestion => s } shouldBe empty
  }

  // ── 9. Collect on large data ────────────────────────────────────────────
  test("detects collect on large dataset") {
    val plan = "CollectLimit 21"
    val result = detect(
      plan,
      stageInputBytes = Map(0 -> (200L * 1024 * 1024))
    )
    result.collect { case s: CollectLargeDataSuggestion => s } should have size 1
  }

  test("no collect suggestion when input is small") {
    val plan = "CollectLimit 21"
    val result = detect(
      plan,
      stageInputBytes = Map(0 -> (10L * 1024 * 1024))
    )
    result.collect { case s: CollectLargeDataSuggestion => s } shouldBe empty
  }

  test("no collect suggestion when no collect nodes in plan") {
    val plan = "+- Exchange hashpartitioning(id#5, 200)"
    val result = detect(
      plan,
      stageInputBytes = Map(0 -> (200L * 1024 * 1024))
    )
    result.collect { case s: CollectLargeDataSuggestion => s } shouldBe empty
  }

  // ── 10. Shuffle partition tuning ────────────────────────────────────────
  test("detects over-partitioned shuffles") {
    val plan = "+- Exchange hashpartitioning(id#5, 200)"
    // 1 MB shuffle with 200 partitions → suggested = 1, ratio = 200
    val result = detect(
      plan,
      stageShuffleWrites = Map(0 -> (1L * 1024 * 1024)),
      shufflePartitions = 200
    )
    result.collect { case s: DefaultShufflePartitionsSuggestion => s } should have size 1
    result.collect { case s: DefaultShufflePartitionsSuggestion => s }
      .head.suggestedPartitions shouldBe 1
  }

  test("no shuffle partition suggestion when AQE is enabled") {
    val plan =
      """|AdaptiveSparkPlan isFinalPlan=true
         |+- Exchange hashpartitioning(id#5, 200)""".stripMargin
    val result = detect(
      plan,
      stageShuffleWrites = Map(0 -> (1L * 1024 * 1024)),
      shufflePartitions = 200
    )
    result.collect { case s: DefaultShufflePartitionsSuggestion => s } shouldBe empty
  }

  test("no shuffle partition suggestion when ratio is within bounds") {
    val plan = "+- Exchange hashpartitioning(id#5, 200)"
    // 25 GB shuffle with 200 partitions → suggested = 200, ratio = 1.0
    val result = detect(
      plan,
      stageShuffleWrites = Map(0 -> (25600L * 1024 * 1024)),
      shufflePartitions = 200
    )
    result.collect { case s: DefaultShufflePartitionsSuggestion => s } shouldBe empty
  }

  // ── Combined detection ──────────────────────────────────────────────────
  test("multiple suggestion types detected from a single plan") {
    val plan =
      """|+- SortMergeJoin [id#5], [id#15], Inner
         |   :- Exchange hashpartitioning(id#5, 200)
         |   :  +- FileScan csv [id#0] DataFilters: [isnotnull(id#0)], PartitionFilters: [], PushedFilters: [IsNotNull(id)]
         |   +- Exchange hashpartitioning(id#15, 200)
         |      +- FileScan csv [id#1] DataFilters: [], PartitionFilters: [], PushedFilters: []""".stripMargin
    val result = detect(
      plan,
      stageIds = Set(0, 1),
      stageShuffleWrites = Map(0 -> (50L * 1024 * 1024), 1 -> (2L * 1024 * 1024))
    )
    // Should detect: broadcast candidate, suboptimal format (csv x2),
    // missing AQE, missing partition pruning, repeated scan (csv x2)
    result.collect { case s: BroadcastJoinSuggestion => s } should have size 1
    result.collect { case s: SuboptimalFormatSuggestion => s } should have size 2
    result.collect { case s: MissingAQESuggestion => s } should have size 1
    result.collect { case s: RepeatedScanSuggestion => s } should have size 1
  }

  test("empty plan produces no suggestions") {
    val result = detect("")
    result shouldBe empty
  }
}
