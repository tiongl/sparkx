package com.sparkx.analysis

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PlanParserSuite extends AnyFunSuite with Matchers {

  test("null input returns empty ParsedPlan") {
    val plan = PlanParser.parse(null)
    plan.hasAQE shouldBe false
    plan.joinNodes shouldBe empty
    plan.exchangeCount shouldBe 0
    plan.scanNodes shouldBe empty
    plan.cartesianNodes shouldBe empty
    plan.pythonUDFs shouldBe empty
    plan.collectNodes shouldBe empty
    plan.sortNodes shouldBe empty
  }

  test("empty string returns empty ParsedPlan") {
    val plan = PlanParser.parse("")
    plan.exchangeCount shouldBe 0
    plan.scanNodes shouldBe empty
  }

  test("SortMergeJoin is parsed") {
    val input =
      """|+- SortMergeJoin [dept_id#10], [dept_id#20], Inner
         |   :- Sort [dept_id#10 ASC], false
         |   +- Sort [dept_id#20 ASC], false""".stripMargin
    val plan = PlanParser.parse(input)
    plan.joinNodes should have size 1
    plan.joinNodes.head.joinType shouldBe "SortMergeJoin"
    plan.joinNodes.head.keys should include("dept_id#10")
  }

  test("ShuffledHashJoin is parsed") {
    val input = "+- ShuffledHashJoin [id#5], [id#15], LeftOuter"
    val plan = PlanParser.parse(input)
    plan.joinNodes should have size 1
    plan.joinNodes.head.joinType shouldBe "ShuffledHashJoin"
  }

  test("BroadcastHashJoin is NOT parsed as a join candidate") {
    val input = "+- BroadcastHashJoin [id#5], [id#15], Inner, BuildRight"
    val plan = PlanParser.parse(input)
    plan.joinNodes shouldBe empty
  }

  test("Exchange nodes are counted with various types") {
    val input =
      """|+- Exchange hashpartitioning(id#5, 200)
         |   +- Exchange rangepartitioning(amount#10 ASC, 100)
         |      +- Exchange roundrobinpartitioning(8)
         |         +- Exchange SinglePartition""".stripMargin
    val plan = PlanParser.parse(input)
    plan.exchangeCount shouldBe 4
  }

  test("FileScan csv extracts format and filters") {
    val input =
      """|+- FileScan csv [id#0,name#1] Batched: false, DataFilters: [isnotnull(id#0)], Format: CSV, PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:string,name:string>""".stripMargin
    val plan = PlanParser.parse(input)
    plan.scanNodes should have size 1
    val scan = plan.scanNodes.head
    scan.format shouldBe "csv"
    scan.partitionFilters shouldBe ""
    scan.pushedFilters should include("IsNotNull")
    scan.dataFilters should include("isnotnull")
  }

  test("FileScan parquet with partition filters") {
    val input =
      """|+- FileScan parquet [id#0,date#1] PartitionFilters: [isnotnull(date#1), (date#1 = 2024-01-01)], PushedFilters: [], DataFilters: []""".stripMargin
    val plan = PlanParser.parse(input)
    plan.scanNodes should have size 1
    plan.scanNodes.head.format shouldBe "parquet"
    plan.scanNodes.head.partitionFilters should include("date#1")
  }

  test("FileScan json and text formats are recognized") {
    val input =
      """|+- FileScan json [val#0] PushedFilters: [], DataFilters: [], PartitionFilters: []
         |+- FileScan text [value#5] PushedFilters: [], DataFilters: [], PartitionFilters: []""".stripMargin
    val plan = PlanParser.parse(input)
    plan.scanNodes should have size 2
    plan.scanNodes.map(_.format) should contain allOf("json", "text")
  }

  test("repeated scans of same format are all captured") {
    val input =
      """|+- FileScan csv [a#0] PushedFilters: [], DataFilters: [], PartitionFilters: []
         |+- FileScan csv [b#1] PushedFilters: [], DataFilters: [], PartitionFilters: []""".stripMargin
    val plan = PlanParser.parse(input)
    plan.scanNodes should have size 2
    plan.scanNodes.map(_.format).distinct shouldBe Seq("csv")
  }

  test("CartesianProduct is detected") {
    val input = "+- CartesianProduct"
    val plan = PlanParser.parse(input)
    plan.cartesianNodes should have size 1
    plan.cartesianNodes.head.nodeType shouldBe "CartesianProduct"
  }

  test("BroadcastNestedLoopJoin is detected as cartesian") {
    val input = "+- BroadcastNestedLoopJoin BuildRight, Cross"
    val plan = PlanParser.parse(input)
    plan.cartesianNodes should have size 1
    plan.cartesianNodes.head.nodeType shouldBe "BroadcastNestedLoopJoin"
  }

  test("Python UDF node types are detected") {
    val input =
      """|+- BatchEvalPython [my_udf(id#0)]
         |+- ArrowEvalPython [my_pandas_udf(id#0)]""".stripMargin
    val plan = PlanParser.parse(input)
    plan.pythonUDFs should have size 2
    plan.pythonUDFs.map(_.nodeType) should contain allOf("BatchEvalPython", "ArrowEvalPython")
  }

  test("CollectLimit and GlobalLimit are detected") {
    val input =
      """|CollectLimit 21
         |+- GlobalLimit 21""".stripMargin
    val plan = PlanParser.parse(input)
    plan.collectNodes should have size 2
    plan.collectNodes.map(_.nodeType) should contain allOf("CollectLimit", "GlobalLimit")
  }

  test("Sort nodes parse global flag") {
    val input =
      """|+- Sort [amount#10 ASC], true
         |   +- Sort [id#5 DESC], false""".stripMargin
    val plan = PlanParser.parse(input)
    plan.sortNodes should have size 2
    plan.sortNodes.head.global shouldBe true
    plan.sortNodes(1).global shouldBe false
  }

  test("AdaptiveSparkPlan sets hasAQE to true") {
    val input =
      """|AdaptiveSparkPlan isFinalPlan=true
         |+- Exchange hashpartitioning(id#5, 200)""".stripMargin
    val plan = PlanParser.parse(input)
    plan.hasAQE shouldBe true
    plan.exchangeCount shouldBe 1
  }

  test("AQE plan format: Exchange with node ID is counted") {
    val input =
      """|AdaptiveSparkPlan (14)
         |+- == Final Plan ==
         |   * HashAggregate (8)
         |   +- ShuffleQueryStage (7)
         |      +- Exchange (6)
         |         +- * HashAggregate (5)""".stripMargin
    val plan = PlanParser.parse(input)
    plan.hasAQE shouldBe true
    plan.exchangeCount shouldBe 1
  }

  test("AQE plan format: SortMergeJoin with node ID is parsed") {
    val input =
      """|AdaptiveSparkPlan (18)
         |+- == Final Plan ==
         |   * SortMergeJoin (12)
         |   :- ShuffleQueryStage (5)
         |   +- ShuffleQueryStage (11)""".stripMargin
    val plan = PlanParser.parse(input)
    plan.joinNodes should have size 1
    plan.joinNodes.head.joinType shouldBe "SortMergeJoin"
  }

  test("AQE plan format: FileScan in detailed section is parsed") {
    val input =
      """|AdaptiveSparkPlan (14)
         |+- == Final Plan ==
         |   * HashAggregate (8)
         |   +- ShuffleQueryStage (7)
         |
         |(9) FileScan csv [id#0,name#1] DataFilters: [isnotnull(id#0)], PartitionFilters: [], PushedFilters: [IsNotNull(id)]""".stripMargin
    val plan = PlanParser.parse(input)
    plan.hasAQE shouldBe true
    plan.scanNodes should have size 1
    plan.scanNodes.head.format shouldBe "csv"
  }

  test("plan without AdaptiveSparkPlan has hasAQE false") {
    val input = "+- Exchange hashpartitioning(id#5, 200)"
    val plan = PlanParser.parse(input)
    plan.hasAQE shouldBe false
  }

  test("realistic mixed plan parses all node types") {
    val input =
      """|== Physical Plan ==
         |*(3) HashAggregate(keys=[dept_name#25], functions=[sum(amount#12L)])
         |+- Exchange hashpartitioning(dept_name#25, 200)
         |   +- *(2) HashAggregate(keys=[dept_name#25], functions=[partial_sum(amount#12L)])
         |      +- *(2) SortMergeJoin [dept_id#10], [dept_id#20], Inner
         |         :- *(1) Sort [dept_id#10 ASC], false
         |         :  +- Exchange hashpartitioning(dept_id#10, 200)
         |         :     +- FileScan csv [id#0,dept_id#10,amount#12] DataFilters: [isnotnull(dept_id#10)], Format: CSV, PartitionFilters: [], PushedFilters: [IsNotNull(dept_id)], ReadSchema: struct<>
         |         +- *(1) Sort [dept_id#20 ASC], false
         |            +- Exchange hashpartitioning(dept_id#20, 200)
         |               +- FileScan parquet [dept_id#20,dept_name#25] PartitionFilters: [], PushedFilters: [], DataFilters: []""".stripMargin
    val plan = PlanParser.parse(input)
    plan.hasAQE shouldBe false
    plan.joinNodes should have size 1
    plan.joinNodes.head.joinType shouldBe "SortMergeJoin"
    plan.exchangeCount shouldBe 3
    plan.scanNodes should have size 2
    plan.scanNodes.map(_.format).toSet shouldBe Set("csv", "parquet")
    plan.sortNodes should have size 2
  }
}
