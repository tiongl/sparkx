package com.sparkx.diff

import org.apache.spark.sql.DataFrame

/**
 * Common trait for dataset diff strategies.
 *
 * Implementations compare two DataFrames by key columns and report
 * which rows were added, removed, or changed.
 */
trait DiffStrategy {

  /** Compare `left` and `right` according to `config` and return the differences. */
  def diff(left: DataFrame, right: DataFrame, config: DiffConfig): DiffResult
}
