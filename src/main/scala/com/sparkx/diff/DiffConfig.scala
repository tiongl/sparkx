package com.sparkx.diff

/**
 * Configuration for a dataset diff operation.
 *
 * @param keyColumns   Columns that uniquely identify a row (used for joining).
 * @param diffColumns  Columns to compare for changes. If empty, all non-key columns are compared.
 * @param limit        Maximum number of rows to return per category (added / removed / changed).
 */
case class DiffConfig(
    keyColumns: Seq[String],
    diffColumns: Seq[String] = Seq.empty,
    limit: Int = 100
) {
  require(keyColumns.nonEmpty, "keyColumns must not be empty")
  require(limit > 0, "limit must be positive")
}
