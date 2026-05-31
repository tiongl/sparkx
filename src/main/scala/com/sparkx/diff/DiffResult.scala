package com.sparkx.diff

import org.apache.spark.sql.DataFrame

/**
 * Result of a dataset diff operation.
 *
 * @param added   Rows present in `right` but not in `left` (limited by [[DiffConfig.limit]]).
 * @param removed Rows present in `left` but not in `right` (limited by [[DiffConfig.limit]]).
 * @param changed Rows where keys match but diff-columns differ.
 *                Contains key columns plus `left_<col>` / `right_<col>` pairs
 *                and a `changed_columns` array (limited by [[DiffConfig.limit]]).
 * @param summary Aggregate counts (totals before limit) and timing.
 */
case class DiffResult(
    added: DataFrame,
    removed: DataFrame,
    changed: DataFrame,
    summary: DiffSummary
)

/**
 * Aggregate statistics for a diff.
 *
 * Counts reflect the *total* number of differences before any limit is applied.
 *
 * @param addedCount       Total rows added.
 * @param removedCount     Total rows removed.
 * @param changedCount     Total rows changed.
 * @param comparisonTimeMs Wall-clock time spent computing the diff.
 */
case class DiffSummary(
    addedCount: Long,
    removedCount: Long,
    changedCount: Long,
    comparisonTimeMs: Long
)
