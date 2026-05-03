package com.sparkx.analysis

import com.sparkx.Utils

sealed trait OptimizationSuggestion {
  def severity:           Severity
  def executionId:        Option[Long]   // SQL execution ID
  def title:              String
  def description:        String
  def recommendation:     String         // actionable advice
  def detailPath:         String
  def estimatedSavingsMs: Option[Long]
  def savingsType:        String
}

case class BroadcastJoinSuggestion(
  executionId:     Option[Long],
  joinType:        String,
  joinKeys:        String,
  smallSideBytes:  Long,
  thresholdBytes:  Long
) extends OptimizationSuggestion {
  val severity: Severity =
    if (smallSideBytes < 10L * 1024 * 1024) Critical  // < 10 MB = Spark default threshold, likely missing stats
    else Warning
  val detailPath = "suggestions"
  val title = "Broadcast Join Candidate"
  val description =
    s"$joinType on $joinKeys has a side of only ${Utils.formatBytes(smallSideBytes)}, " +
    s"but was not broadcast (threshold: ${Utils.formatBytes(thresholdBytes)})"
  val recommendation =
    "Add a broadcast hint: `df.join(broadcast(smallDf), ...)` or " +
    "ensure table statistics are computed with `ANALYZE TABLE ... COMPUTE STATISTICS` " +
    "so Spark can auto-broadcast."
  val estimatedSavingsMs: Option[Long] = None
  val savingsType = "I/O"
}

case class UnnecessaryShuffleSuggestion(
  executionId:      Option[Long],
  exchangeCount:    Int,
  shuffleBytesHint: Option[Long]
) extends OptimizationSuggestion {
  val severity: Severity = Warning
  val detailPath = "suggestions"
  val title = "Excessive Shuffles"
  val description =
    s"SQL execution has $exchangeCount Exchange (shuffle) nodes. " +
    "Some may be redundant if data is already partitioned correctly."
  val recommendation =
    "Review the query plan for unnecessary repartitions. " +
    "Consider using `repartition()` once before joins rather than multiple times, " +
    "or enable AQE which can coalesce redundant shuffles."
  val estimatedSavingsMs: Option[Long] = shuffleBytesHint.map(_ / (100 * 1024 * 1024) * 1000)
  val savingsType = "I/O"
}

case class MissingAQESuggestion(
  executionId:  Option[Long],
  exchangeCount: Int,
  hasCoOccurringIssues: Boolean
) extends OptimizationSuggestion {
  val severity: Severity = if (hasCoOccurringIssues) Warning else Info
  val detailPath = "suggestions"
  val title = "AQE Not Enabled"
  val description =
    s"Adaptive Query Execution is disabled for this execution with $exchangeCount shuffle(s). " +
    "AQE dynamically optimizes joins, partition sizes, and skew at runtime."
  val recommendation =
    "Set `spark.sql.adaptive.enabled=true` (default since Spark 3.2). " +
    "AQE can auto-convert sort-merge joins to broadcast, coalesce small partitions, " +
    "and handle skew joins at runtime."
  val estimatedSavingsMs: Option[Long] = None
  val savingsType = ""
}

case class CartesianProductSuggestion(
  executionId: Option[Long],
  nodeType:    String,
  line:        String
) extends OptimizationSuggestion {
  val severity: Severity = Critical
  val detailPath = "suggestions"
  val title = "Cartesian Product"
  val description =
    s"Query uses $nodeType which produces a cross-product of all rows. " +
    "This is O(N×M) and usually unintentional."
  val recommendation =
    "Add a join condition (`ON a.key = b.key`) to convert this to an equi-join. " +
    "If the cross-product is intentional on small data, use `crossJoin()` explicitly."
  val estimatedSavingsMs: Option[Long] = None
  val savingsType = ""
}

case class SuboptimalFormatSuggestion(
  executionId: Option[Long],
  format:      String,
  scanLine:    String
) extends OptimizationSuggestion {
  val severity: Severity = Info
  val detailPath = "suggestions"
  val title = "Suboptimal File Format"
  val description =
    s"Reading data in ${format.toUpperCase} format. Row-based formats lack " +
    "columnar compression, predicate pushdown, and column pruning."
  val recommendation =
    s"Convert data to Parquet or ORC for 2–5× faster reads and smaller storage: " +
    s"""`df.write.parquet("path")` or `CREATE TABLE ... USING PARQUET`."""
  val estimatedSavingsMs: Option[Long] = None
  val savingsType = "I/O"
}

case class MissingPartitionPruningSuggestion(
  executionId:     Option[Long],
  format:          String,
  pushedFilters:   String,
  scanLine:        String
) extends OptimizationSuggestion {
  val severity: Severity = Warning
  val detailPath = "suggestions"
  val title = "Missing Partition Pruning"
  val description =
    s"Scan on ${format.toUpperCase} has filters ($pushedFilters) " +
    "but no partition pruning. Full partition listing is performed."
  val recommendation =
    "Partition your data by frequently filtered columns: " +
    """`df.write.partitionBy("date").parquet("path")` or """ +
    "`PARTITIONED BY (date)` in DDL. " +
    "This lets Spark skip reading irrelevant partitions entirely."
  val estimatedSavingsMs: Option[Long] = None
  val savingsType = "I/O"
}

case class PythonUDFSuggestion(
  executionId: Option[Long],
  nodeType:    String,
  count:       Int
) extends OptimizationSuggestion {
  val severity: Severity = Warning
  val detailPath = "suggestions"
  val title = "Python UDF Detected"
  val description =
    s"Query uses $count $nodeType node(s). Python UDFs serialize rows to Python and back, " +
    "breaking Spark's whole-stage code generation and adding significant overhead."
  val recommendation =
    "Replace Python UDFs with native Spark SQL functions or " +
    "Pandas UDFs (`@pandas_udf`) which operate on Arrow batches. " +
    "For complex logic, consider `mapInArrow()` (Spark 3.3+)."
  val estimatedSavingsMs: Option[Long] = None
  val savingsType = "Compute"
}

case class RepeatedScanSuggestion(
  executionId: Option[Long],
  format:      String,
  scanCount:   Int
) extends OptimizationSuggestion {
  val severity: Severity = Warning
  val detailPath = "suggestions"
  val title = "Repeated Table Scan"
  val description =
    s"The same ${format.toUpperCase} data source is scanned $scanCount times in one execution. " +
    "Each scan re-reads data from storage, wasting I/O."
  val recommendation =
    "Cache the shared DataFrame with `.persist()` or `.cache()` before reuse: " +
    "`val cached = df.cache(); cached.count()` (materialize), then use `cached` in subsequent operations. " +
    "Unpersist when done."
  val estimatedSavingsMs: Option[Long] = None
  val savingsType = "I/O"
}

case class CollectLargeDataSuggestion(
  executionId:   Option[Long],
  totalInputMB:  Long
) extends OptimizationSuggestion {
  val severity: Severity = if (totalInputMB > 1024) Critical else Warning
  val detailPath = "suggestions"
  val title = "Collect on Large Dataset"
  val description =
    s"A collect/take operation pulls data to the driver on stages processing ~${totalInputMB} MB. " +
    "Large collects risk driver OOM and defeat the purpose of distributed processing."
  val recommendation =
    "Avoid `collect()` on large datasets. Use `.show()`, `.take(n)`, or `.toLocalIterator()` for sampling. " +
    "For exports, write directly to storage: `df.write.parquet(\"path\")`."
  val estimatedSavingsMs: Option[Long] = None
  val savingsType = ""
}

case class DefaultShufflePartitionsSuggestion(
  executionId:         Option[Long],
  currentPartitions:   Int,
  totalShuffleBytes:   Long,
  suggestedPartitions: Int
) extends OptimizationSuggestion {
  val severity: Severity = Warning
  val detailPath = "suggestions"
  val title = "Shuffle Partition Tuning"
  val description = {
    val totalMB = totalShuffleBytes / (1024 * 1024)
    s"Using $currentPartitions shuffle partitions (Spark default: 200) " +
    s"for ~${totalMB} MB of shuffle data. " +
    (if (suggestedPartitions < currentPartitions)
       s"Too many partitions create scheduling overhead and tiny tasks."
     else
       s"Too few partitions may cause large tasks and spill.")
  }
  val recommendation =
    s"Set `spark.sql.shuffle.partitions=$suggestedPartitions` based on data volume, " +
    "or enable AQE (`spark.sql.adaptive.enabled=true`) which auto-coalesces partitions at runtime."
  val estimatedSavingsMs: Option[Long] = None
  val savingsType = "Scheduling overhead"
}
