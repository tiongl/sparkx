package com.sparkx.diff

/**
 * Tests that apply to every [[DiffStrategy]] implementation.
 *
 * Each concrete strategy suite extends this and plugs in its own instance
 * via [[strategy]].  Tests cover:
 *   - basic added / removed / changed detection
 *   - identical datasets (no diff)
 *   - selective diff-column specification
 *   - limit enforcement
 *   - null handling
 *   - multi-column composite keys
 *   - schema validation errors
 */
trait DiffStrategyBehaviors { self: DiffSuiteBase =>

  def strategy: DiffStrategy
  def strategyName: String

  // ── Core correctness ──────────────────────────────────────────────────

  test(s"$strategyName: detects added, removed, and changed rows") {
    val result = strategy.diff(leftDf, rightDf, defaultConfig)

    result.summary.addedCount   shouldBe 1
    result.summary.removedCount shouldBe 1
    result.summary.changedCount shouldBe 1

    val addedRows = result.added.collect()
    addedRows.length shouldBe 1
    addedRows(0).getAs[Int]("id") shouldBe 5

    val removedRows = result.removed.collect()
    removedRows.length shouldBe 1
    removedRows(0).getAs[Int]("id") shouldBe 4

    val changedRows = result.changed.collect()
    changedRows.length shouldBe 1
    changedRows(0).getAs[Int]("id") shouldBe 2
    changedRows(0).getAs[Double]("left_value") shouldBe 20.0
    changedRows(0).getAs[Double]("right_value") shouldBe 25.0
  }

  test(s"$strategyName: identical datasets produce empty diff") {
    val result = strategy.diff(leftDf, leftDf, defaultConfig)

    result.summary.addedCount   shouldBe 0
    result.summary.removedCount shouldBe 0
    result.summary.changedCount shouldBe 0
    result.added.count()   shouldBe 0
    result.removed.count() shouldBe 0
    result.changed.count() shouldBe 0
  }

  test(s"$strategyName: respects explicit diffColumns") {
    val s = spark
    import s.implicits._
    // Change only 'name' for id=2 — value stays the same
    val right = Seq(
      (1, "alice", 10.0),
      (2, "BOB",   20.0),   // name changed, value same
      (3, "carol", 30.0),
      (4, "dave",  40.0)
    ).toDF("id", "name", "value")

    // Diff only 'value' column — should see no changes
    val config = DiffConfig(keyColumns = Seq("id"), diffColumns = Seq("value"))
    val result = strategy.diff(leftDf, right, config)
    result.summary.changedCount shouldBe 0

    // Diff only 'name' column — should see 1 change
    val config2 = DiffConfig(keyColumns = Seq("id"), diffColumns = Seq("name"))
    val result2 = strategy.diff(leftDf, right, config2)
    result2.summary.changedCount shouldBe 1
  }

  test(s"$strategyName: limit caps output rows") {
    val s = spark
    import s.implicits._
    // Create many added rows
    val left = Seq((1, "a")).toDF("id", "name")
    val right = (1 to 50).toList.map(i => (i, s"name_$i")).toDF("id", "name")

    val config = DiffConfig(keyColumns = Seq("id"), limit = 5)
    val result = strategy.diff(left, right, config)

    result.summary.addedCount shouldBe 49  // total
    result.added.count() shouldBe 5        // limited
  }

  test(s"$strategyName: handles null values correctly") {
    val s = spark
    import s.implicits._
    val left = Seq(
      (1, Option("alice"), Option(10.0)),
      (2, Option.empty[String], Option(20.0)),
      (3, Option("carol"), Option.empty[Double])
    ).toDF("id", "name", "value")

    val right = Seq(
      (1, Option("alice"), Option(10.0)),  // same
      (2, Option.empty[String], Option(25.0)),  // value changed, name stays null
      (3, Option("carol"), Option.empty[Double])         // same (both null)
    ).toDF("id", "name", "value")

    val config = DiffConfig(keyColumns = Seq("id"))
    val result = strategy.diff(left, right, config)

    result.summary.changedCount shouldBe 1
    result.summary.addedCount   shouldBe 0
    result.summary.removedCount shouldBe 0

    val changed = result.changed.collect()
    changed(0).getAs[Int]("id") shouldBe 2
  }

  test(s"$strategyName: composite key columns") {
    val s = spark
    import s.implicits._
    val left = Seq(
      (1, "A", 100),
      (1, "B", 200),
      (2, "A", 300)
    ).toDF("id", "region", "value")

    val right = Seq(
      (1, "A", 100),
      (1, "B", 999),   // changed
      (2, "B", 400)    // added (2,B); (2,A) removed
    ).toDF("id", "region", "value")

    val config = DiffConfig(keyColumns = Seq("id", "region"))
    val result = strategy.diff(left, right, config)

    result.summary.changedCount shouldBe 1
    result.summary.addedCount   shouldBe 1
    result.summary.removedCount shouldBe 1
  }

  test(s"$strategyName: changed_columns array is accurate") {
    val s = spark
    import s.implicits._
    val left  = Seq((1, "alice", 10.0, "x")).toDF("id", "name", "value", "tag")
    val right = Seq((1, "ALICE", 10.0, "y")).toDF("id", "name", "value", "tag")

    val config = DiffConfig(keyColumns = Seq("id"))
    val result = strategy.diff(left, right, config)

    result.summary.changedCount shouldBe 1
    val row = result.changed.collect()(0)
    val changedCols = row.getAs[scala.collection.mutable.WrappedArray[String]]("changed_columns").toSet
    changedCols should contain ("name")
    changedCols should contain ("tag")
    changedCols should not contain "value"
  }

  // ── Validation ────────────────────────────────────────────────────────

  test(s"$strategyName: rejects missing key column") {
    val config = DiffConfig(keyColumns = Seq("nonexistent"))
    an[IllegalArgumentException] should be thrownBy {
      strategy.diff(leftDf, rightDf, config)
    }
  }

  test(s"$strategyName: rejects missing diff column") {
    val config = DiffConfig(keyColumns = Seq("id"), diffColumns = Seq("nope"))
    an[IllegalArgumentException] should be thrownBy {
      strategy.diff(leftDf, rightDf, config)
    }
  }
}
