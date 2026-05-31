package com.sparkx.diff

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Diff strategy that uses MD5 hashing to quickly identify rows whose
 * content has changed, then performs exact column-wise comparison on
 * the mismatched subset.
 *
 * Workflow:
 *  1. Compute an MD5 hash of the diff columns for every row in both datasets.
 *  2. Join on key columns and compare hashes to find candidate changed rows.
 *  3. For those candidates, do null-safe column comparisons to produce the
 *     precise per-column diff (guards against hash collisions).
 *  4. Anti-joins for added / removed rows.
 *
 * The hash step narrows the data that needs expensive column-wise comparison
 * while the verification step guarantees correctness.
 */
class MD5DiffStrategy extends DiffStrategy {

  private val HashCol = "_sparkx_md5"

  override def diff(left: DataFrame, right: DataFrame,
                    config: DiffConfig): DiffResult = {
    DiffUtils.validateSchemas(left, right, config)
    val startMs = System.currentTimeMillis()
    val diffCols = DiffUtils.resolveDiffColumns(left, config)

    // ── 1. Add MD5 hash column ─────────────────────────────────────────
    // Use to_json(struct(...)) for stable serialization that handles nulls,
    // nested types, and avoids ambiguous concat boundaries.
    val hashExpr = md5(to_json(struct(diffCols.map(col): _*)))
    val leftHashed  = left.withColumn(HashCol, hashExpr)
    val rightHashed = right.withColumn(HashCol, hashExpr)

    // ── 2. Added / Removed via anti-joins on key columns ───────────────
    val removedFull = leftHashed.join(rightHashed, config.keyColumns, "left_anti")
      .drop(HashCol)
    val addedFull = rightHashed.join(leftHashed, config.keyColumns, "left_anti")
      .drop(HashCol)

    // ── 3. Find candidate changed rows via narrow (key + hash) join ────
    // Only shuffle the key columns + 32-char hash instead of all 20+ columns.
    val keyCols = config.keyColumns.map(col)
    val leftNarrow  = leftHashed.select((keyCols :+ col(HashCol)): _*)
    val rightNarrow = rightHashed.select((keyCols :+ col(HashCol)): _*)

    val narrowJoinCond = DiffUtils.keyJoinCondition(leftNarrow, rightNarrow,
      config.keyColumns)
    val joinedNarrow = leftNarrow.alias("l").join(
      rightNarrow.alias("r"), narrowJoinCond, "inner"
    )
    val mismatchKeys = joinedNarrow
      .filter(not(col(s"l.$HashCol") <=> col(s"r.$HashCol")))
      .select(config.keyColumns.map(k => col(s"l.$k").as(k)): _*)

    // ── 4. Fetch full rows only for mismatched keys, then verify ────────
    val leftMismatched  = left.join(broadcast(mismatchKeys), config.keyColumns, "inner")
    val rightMismatched = right.join(broadcast(mismatchKeys), config.keyColumns, "inner")
    val fullJoinCond = DiffUtils.keyJoinCondition(leftMismatched, rightMismatched,
      config.keyColumns)
    val hashMismatch = leftMismatched.alias("l").join(
      rightMismatched.alias("r"), fullJoinCond, "inner"
    )

    // ── 5. Exact column-wise verification on candidates ────────────────
    val changedFull = DiffUtils.buildChangedDf(
      hashMismatch,
      leftMismatched.alias("l"), rightMismatched.alias("r"),
      config.keyColumns, diffCols
    )

    // ── 6. Counts (total) then limit ───────────────────────────────────
    val addedCount   = addedFull.count()
    val removedCount = removedFull.count()
    val changedCount = changedFull.count()

    val elapsedMs = System.currentTimeMillis() - startMs

    DiffResult(
      added   = addedFull.limit(config.limit),
      removed = removedFull.limit(config.limit),
      changed = changedFull.limit(config.limit),
      summary = DiffSummary(addedCount, removedCount, changedCount, elapsedMs)
    )
  }
}
