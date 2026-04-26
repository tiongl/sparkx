package com.sparkx.analysis

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OptimizationSuggestionSuite extends AnyFunSuite with Matchers {

  // ── BroadcastJoinSuggestion severity ─────────────────────────────────────
  test("BroadcastJoinSuggestion: < 10 MB is Critical") {
    val s = BroadcastJoinSuggestion(Some(1L), "SortMergeJoin", "[id]", 5 * 1024 * 1024, 100 * 1024 * 1024)
    s.severity shouldBe Critical
  }

  test("BroadcastJoinSuggestion: exactly 10 MB is Warning") {
    val s = BroadcastJoinSuggestion(Some(1L), "SortMergeJoin", "[id]", 10L * 1024 * 1024, 100 * 1024 * 1024)
    s.severity shouldBe Warning
  }

  test("BroadcastJoinSuggestion: > 10 MB is Warning") {
    val s = BroadcastJoinSuggestion(Some(1L), "SortMergeJoin", "[id]", 50L * 1024 * 1024, 100 * 1024 * 1024)
    s.severity shouldBe Warning
  }

  // ── CollectLargeDataSuggestion severity ──────────────────────────────────
  test("CollectLargeDataSuggestion: > 1024 MB is Critical") {
    val s = CollectLargeDataSuggestion(Some(1L), 2048)
    s.severity shouldBe Critical
  }

  test("CollectLargeDataSuggestion: exactly 1024 MB is Warning") {
    val s = CollectLargeDataSuggestion(Some(1L), 1024)
    s.severity shouldBe Warning
  }

  test("CollectLargeDataSuggestion: < 1024 MB is Warning") {
    val s = CollectLargeDataSuggestion(Some(1L), 500)
    s.severity shouldBe Warning
  }

  // ── MissingAQESuggestion severity ────────────────────────────────────────
  test("MissingAQESuggestion: with co-occurring issues is Warning") {
    val s = MissingAQESuggestion(Some(1L), 3, hasCoOccurringIssues = true)
    s.severity shouldBe Warning
  }

  test("MissingAQESuggestion: without co-occurring issues is Info") {
    val s = MissingAQESuggestion(Some(1L), 3, hasCoOccurringIssues = false)
    s.severity shouldBe Info
  }

  // ── Fixed-severity suggestions ───────────────────────────────────────────
  test("CartesianProductSuggestion is always Critical") {
    val s = CartesianProductSuggestion(Some(1L), "CartesianProduct", "CartesianProduct")
    s.severity shouldBe Critical
  }

  test("SuboptimalFormatSuggestion is always Info") {
    val s = SuboptimalFormatSuggestion(Some(1L), "csv", "FileScan csv ...")
    s.severity shouldBe Info
  }

  test("UnnecessaryShuffleSuggestion is always Warning") {
    val s = UnnecessaryShuffleSuggestion(Some(1L), 5, Some(1024L))
    s.severity shouldBe Warning
  }

  test("MissingPartitionPruningSuggestion is always Warning") {
    val s = MissingPartitionPruningSuggestion(Some(1L), "csv", "IsNotNull(id)", "FileScan csv ...")
    s.severity shouldBe Warning
  }

  test("PythonUDFSuggestion is always Warning") {
    val s = PythonUDFSuggestion(Some(1L), "BatchEvalPython", 2)
    s.severity shouldBe Warning
  }

  test("RepeatedScanSuggestion is always Warning") {
    val s = RepeatedScanSuggestion(Some(1L), "csv", 3)
    s.severity shouldBe Warning
  }

  test("DefaultShufflePartitionsSuggestion is always Warning") {
    val s = DefaultShufflePartitionsSuggestion(Some(1L), 200, 1024 * 1024, 1)
    s.severity shouldBe Warning
  }

  // ── UnnecessaryShuffleSuggestion estimated savings ───────────────────────
  test("UnnecessaryShuffleSuggestion: savings computed from shuffle bytes") {
    val s = UnnecessaryShuffleSuggestion(Some(1L), 5, Some(200L * 1024 * 1024))
    s.estimatedSavingsMs shouldBe defined
  }

  test("UnnecessaryShuffleSuggestion: no savings when shuffleBytesHint is None") {
    val s = UnnecessaryShuffleSuggestion(Some(1L), 5, None)
    s.estimatedSavingsMs shouldBe None
  }

  // ── All suggestions have required fields ─────────────────────────────────
  test("all suggestion types have non-empty title, description, recommendation") {
    val suggestions: Seq[OptimizationSuggestion] = Seq(
      BroadcastJoinSuggestion(Some(1L), "SortMergeJoin", "[id]", 5 * 1024 * 1024, 100 * 1024 * 1024),
      UnnecessaryShuffleSuggestion(Some(1L), 5, Some(1024L)),
      MissingAQESuggestion(Some(1L), 3, hasCoOccurringIssues = false),
      CartesianProductSuggestion(Some(1L), "CartesianProduct", "CartesianProduct"),
      SuboptimalFormatSuggestion(Some(1L), "csv", "FileScan csv ..."),
      MissingPartitionPruningSuggestion(Some(1L), "csv", "IsNotNull(id)", "FileScan csv ..."),
      PythonUDFSuggestion(Some(1L), "BatchEvalPython", 2),
      RepeatedScanSuggestion(Some(1L), "csv", 3),
      CollectLargeDataSuggestion(Some(1L), 500),
      DefaultShufflePartitionsSuggestion(Some(1L), 200, 1024 * 1024, 1)
    )
    suggestions.foreach { s =>
      s.title should not be empty
      s.description should not be empty
      s.recommendation should not be empty
      s.detailPath shouldBe "suggestions"
    }
  }
}
