package com.sparkx.diff

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/** Shared helpers used by diff strategy implementations. */
private[diff] object DiffUtils {

  /** Resolve which columns to compare: explicit list or all non-key columns. */
  def resolveDiffColumns(df: DataFrame, config: DiffConfig): Seq[String] = {
    if (config.diffColumns.nonEmpty) config.diffColumns
    else df.columns.filterNot(config.keyColumns.contains).toSeq
  }

  /** Validate that key and diff columns exist on both sides with compatible types. */
  def validateSchemas(left: DataFrame, right: DataFrame, config: DiffConfig): Unit = {
    val allCols = config.keyColumns ++ config.diffColumns
    val leftFields = left.schema.fieldNames.toSet
    val rightFields = right.schema.fieldNames.toSet

    allCols.foreach { c =>
      require(leftFields.contains(c), s"Column '$c' not found in left DataFrame")
      require(rightFields.contains(c), s"Column '$c' not found in right DataFrame")
    }

    // Check type compatibility for key columns
    config.keyColumns.foreach { c =>
      val lt = left.schema(c).dataType
      val rt = right.schema(c).dataType
      require(lt == rt,
        s"Key column '$c' type mismatch: left=$lt, right=$rt")
    }
  }

  /** Build a null-safe key equality condition for joining. */
  def keyJoinCondition(left: DataFrame, right: DataFrame,
                       keyColumns: Seq[String]): Column = {
    keyColumns
      .map(k => left(k) <=> right(k))
      .reduce(_ && _)
  }

  /**
   * Build a standard equality join condition (for anti-joins where Spark
   * requires equi-join semantics rather than expression-based conditions).
   */
  def keyEquiJoinColumns(keyColumns: Seq[String]): Seq[String] = keyColumns

  /**
   * Build changed-rows DataFrame with key cols, left_/right_ value pairs,
   * and a `changed_columns` array listing which columns actually differ.
   */
  def buildChangedDf(joined: DataFrame,
                     left: DataFrame,
                     right: DataFrame,
                     keyColumns: Seq[String],
                     diffColumns: Seq[String]): DataFrame = {
    // Only keep rows where at least one diff column differs (null-safe)
    val diffCondition = diffColumns
      .map(c => not(left(c) <=> right(c)))
      .reduce(_ || _)

    val keyCols = keyColumns.map(k => left(k).as(k))

    val valueCols = diffColumns.flatMap { c =>
      Seq(left(c).as(s"left_$c"), right(c).as(s"right_$c"))
    }

    // Build array of changed column names, using when/otherwise to mark
    // unchanged columns as empty string, then filter them out.
    val changedColNames = diffColumns.map { c =>
      when(not(left(c) <=> right(c)), lit(c)).otherwise(lit(""))
    }
    val changedColsRaw = array(changedColNames: _*)
    val changedColumnsCol = array_remove(changedColsRaw, "")

    joined
      .filter(diffCondition)
      .select((keyCols :+ changedColumnsCol.as("changed_columns")) ++ valueCols: _*)
  }
}
