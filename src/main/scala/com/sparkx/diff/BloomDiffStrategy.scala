package com.sparkx.diff

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.util.sketch.BloomFilter

/**
 * Diff strategy that uses a Bloom filter to pre-filter candidates before
 * performing precise comparisons.
 *
 * Workflow:
 *  1. Determine which dataset is smaller (via logical-plan stats or exact count).
 *  2. Build a Bloom filter on the smaller dataset's key columns.
 *  3. Broadcast the filter and use it to pre-filter the larger dataset,
 *     eliminating rows whose keys definitely do not exist on the other side.
 *  4. Perform anti-joins (added / removed) and an inner join + column-wise
 *     comparison (changed) on the filtered data.
 *
 * @param expectedNumItems Expected number of items in the Bloom filter
 *                         (used for sizing). Defaults to 10 million.
 * @param fpp              False-positive probability. Defaults to 0.01 (1 %).
 */
class BloomDiffStrategy(
    expectedNumItems: Long = 10000000L,
    fpp: Double = 0.01
) extends DiffStrategy {

  override def diff(left: DataFrame, right: DataFrame,
                    config: DiffConfig): DiffResult = {
    DiffUtils.validateSchemas(left, right, config)
    val startMs = System.currentTimeMillis()
    val diffCols = DiffUtils.resolveDiffColumns(left, config)
    val spark = left.sparkSession

    // ── 1. Determine smaller side using logical-plan stats ──────────────
    val leftSize  = planSize(left)
    val rightSize = planSize(right)
    val (smaller, larger, smallerIsLeft) =
      if (leftSize <= rightSize) (left, right, true)
      else (right, left, false)

    // ── 2. Build Bloom filter on smaller dataset's composite key ───────
    val keyExpr = concat_ws("\u0000", config.keyColumns.map(col): _*)
    val bloomFilter = smaller
      .select(keyExpr.as("_bloom_key"))
      .stat
      .bloomFilter("_bloom_key", expectedNumItems, fpp)

    // ── 3. Broadcast filter and pre-filter the larger dataset ──────────
    val bfBroadcast = spark.sparkContext.broadcast(bloomFilter)
    val mightExistUdf = udf((key: String) =>
      key != null && bfBroadcast.value.mightContain(key)
    )

    val largerKeyCol = concat_ws("\u0000", config.keyColumns.map(col): _*)
    val largerFiltered = larger.filter(mightExistUdf(largerKeyCol))

    // Reassign left/right after filtering
    val (filteredLeft, filteredRight) =
      if (smallerIsLeft) (smaller, largerFiltered)
      else (largerFiltered, smaller)

    // ── 4. Added / Removed via anti-joins ──────────────────────────────
    // Broadcast only the key columns of each side (much smaller than
    // full-width rows) to avoid shuffling the larger dataset.
    val leftKeys  = left.select(config.keyColumns.map(col): _*)
    val rightKeys = right.select(config.keyColumns.map(col): _*)
    val removedFull = left.join(broadcast(rightKeys), config.keyColumns, "left_anti")
    val addedFull   = right.join(broadcast(leftKeys), config.keyColumns, "left_anti")

    // ── 5. Changed via inner join + column comparison ──────────────────
    // Bloom-filtered data reduces the inner join to rows whose keys exist
    // on both sides (with possible false positives).
    val joinCond = DiffUtils.keyJoinCondition(filteredLeft, filteredRight,
      config.keyColumns)
    val joined = filteredLeft.alias("l").join(
      filteredRight.alias("r"), joinCond, "inner"
    )
    val changedFull = DiffUtils.buildChangedDf(
      joined,
      filteredLeft.alias("l"), filteredRight.alias("r"),
      config.keyColumns, diffCols
    )

    // ── 6. Counts (total) then limit ───────────────────────────────────
    val addedCount   = addedFull.count()
    val removedCount = removedFull.count()
    val changedCount = changedFull.count()

    val elapsedMs = System.currentTimeMillis() - startMs

    // Clean up broadcast
    bfBroadcast.unpersist()

    DiffResult(
      added   = addedFull.limit(config.limit),
      removed = removedFull.limit(config.limit),
      changed = changedFull.limit(config.limit),
      summary = DiffSummary(addedCount, removedCount, changedCount, elapsedMs)
    )
  }

  /** Estimate dataset size from logical-plan statistics; fall back to Long.MaxValue. */
  private def planSize(df: DataFrame): Long = {
    val stats = df.queryExecution.optimizedPlan.stats
    stats.sizeInBytes.toLong
  }
}
