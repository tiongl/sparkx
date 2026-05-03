package org.apache.spark.ui.sparkx

import com.sparkx.SparkXConfig
import com.sparkx.analysis._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.StageStatus

import java.util.Arrays

/**
 * Detects missed optimization opportunities by analysing SQL physical
 * plans from `SQLAppStatusStore` and cross-referencing with stage metrics.
 */
object SuggestionDetector {

  private val CACHE_TTL_MS = 30000L
  private case class CacheEntry(result: Any, ts: Long)
  private val cache = new java.util.concurrent.ConcurrentHashMap[String, CacheEntry]()

  private def cached[T](key: String)(compute: => T): T = {
    val e = cache.get(key)
    if (e != null && System.currentTimeMillis() - e.ts < CACHE_TTL_MS)
      e.result.asInstanceOf[T]
    else {
      val r = compute
      cache.put(key, CacheEntry(r, System.currentTimeMillis()))
      r
    }
  }

  def detect(store: AppStatusStore,
             config: SparkXConfig): Seq[OptimizationSuggestion] = cached("suggestions") {
    try {
      val sqlStore = new org.apache.spark.sql.execution.ui.SQLAppStatusStore(store.store)
      val executions = sqlStore.executionsList()
      val stageShuffleWrites = buildStageShuffleWrites(store)
      val stageInputBytes = buildStageInputBytes(store)
      val existingIssues = IssueDetector.detect(store, config)
      val shufflePartitions = try {
        val conf = store.environmentInfo().sparkProperties
        conf.find(_._1 == "spark.sql.shuffle.partitions")
          .map(_._2.toInt).getOrElse(200)
      } catch { case _: Throwable => 200 }

      executions.flatMap { exec =>
        detectForExecution(
          exec.executionId,
          exec.physicalPlanDescription,
          exec.stages,
          stageShuffleWrites,
          stageInputBytes,
          existingIssues,
          shufflePartitions,
          config
        )
      }
    } catch {
      case e: Throwable =>
        System.err.println(s"[sparkx] SuggestionDetector.detect failed: ${e.getClass.getName}: ${e.getMessage}")
        e.printStackTrace(System.err)
        Seq.empty
    }
  }

  private[sparkx] def detectForExecution(
    execId:              Long,
    planDescription:     String,
    stageIds:            Set[Int],
    stageShuffleWrites:  Map[Int, Long],
    stageInputBytes:     Map[Int, Long],
    existingIssues:      Seq[PerformanceIssue],
    shufflePartitions:   Int,
    config:              SparkXConfig
  ): Seq[OptimizationSuggestion] = {
    val parsed = PlanParser.parse(planDescription)
    val suggestions = scala.collection.mutable.ArrayBuffer[OptimizationSuggestion]()

    // 1) Broadcast join candidates
    if (parsed.joinNodes.nonEmpty) {
      val shuffleSizes = stageIds.toSeq.flatMap(stageShuffleWrites.get).sorted
      if (shuffleSizes.nonEmpty) {
        val smallestShuffle = shuffleSizes.head
        if (smallestShuffle > 0 && smallestShuffle <= config.broadcastThresholdBytes) {
          val join = parsed.joinNodes.head
          suggestions += BroadcastJoinSuggestion(
            executionId    = Some(execId),
            joinType       = join.joinType,
            joinKeys       = join.keys,
            smallSideBytes = smallestShuffle,
            thresholdBytes = config.broadcastThresholdBytes
          )
        }
      }
    }

    // 2) Excessive shuffles
    if (parsed.exchangeCount > config.excessiveShuffleCount) {
      val totalShuffleBytes = stageIds.toSeq
        .flatMap(stageShuffleWrites.get).sum
      suggestions += UnnecessaryShuffleSuggestion(
        executionId      = Some(execId),
        exchangeCount    = parsed.exchangeCount,
        shuffleBytesHint = if (totalShuffleBytes > 0) Some(totalShuffleBytes) else None
      )
    }

    // 3) Missing AQE
    if (!parsed.hasAQE && parsed.exchangeCount > 0) {
      val hasCoOccurring = existingIssues.exists { i =>
        i.stageId.exists(stageIds.contains) && (i.isInstanceOf[DataSkewIssue] ||
          i.isInstanceOf[SmallTasksIssue] || i.isInstanceOf[UnderPartitionedIssue])
      }
      suggestions += MissingAQESuggestion(
        executionId          = Some(execId),
        exchangeCount        = parsed.exchangeCount,
        hasCoOccurringIssues = hasCoOccurring
      )
    }

    // 4) Cartesian products
    parsed.cartesianNodes.foreach { cn =>
      suggestions += CartesianProductSuggestion(
        executionId = Some(execId),
        nodeType    = cn.nodeType,
        line        = cn.line
      )
    }

    // 5) Suboptimal file format
    val suboptimalFormats = Set("csv", "json", "text")
    parsed.scanNodes
      .filter(s => suboptimalFormats.contains(s.format))
      .foreach { scan =>
        suggestions += SuboptimalFormatSuggestion(
          executionId = Some(execId),
          format      = scan.format,
          scanLine    = scan.line
        )
      }

    // 6) Missing partition pruning
    parsed.scanNodes
      .filter { s =>
        s.partitionFilters.trim.isEmpty &&
          (s.pushedFilters.trim.nonEmpty || s.dataFilters.trim.nonEmpty)
      }
      .foreach { scan =>
        val filterDesc = if (scan.pushedFilters.nonEmpty) scan.pushedFilters else scan.dataFilters
        suggestions += MissingPartitionPruningSuggestion(
          executionId   = Some(execId),
          format        = scan.format,
          pushedFilters = filterDesc,
          scanLine      = scan.line
        )
      }

    // 7) Python UDFs
    if (parsed.pythonUDFs.nonEmpty) {
      val grouped = parsed.pythonUDFs.groupBy(_.nodeType)
      grouped.foreach { case (nodeType, nodes) =>
        suggestions += PythonUDFSuggestion(
          executionId = Some(execId),
          nodeType    = nodeType,
          count       = nodes.size
        )
      }
    }

    // 8) Repeated table scans — same format scanned 2+ times
    val scansByFormat = parsed.scanNodes.groupBy(_.format)
    scansByFormat.foreach { case (format, scans) =>
      if (scans.size >= 2) {
        suggestions += RepeatedScanSuggestion(
          executionId = Some(execId),
          format      = format,
          scanCount   = scans.size
        )
      }
    }

    // 9) Collect on large data
    if (parsed.collectNodes.nonEmpty) {
      val totalInputMB = stageIds.toSeq
        .flatMap(stageInputBytes.get).sum / (1024 * 1024)
      if (totalInputMB >= config.collectLargeDataMinMB) {
        suggestions += CollectLargeDataSuggestion(
          executionId  = Some(execId),
          totalInputMB = totalInputMB
        )
      }
    }

    // 10) Shuffle partition tuning
    if (parsed.exchangeCount > 0 && !parsed.hasAQE) {
      val totalShuffleBytes = stageIds.toSeq.flatMap(stageShuffleWrites.get).sum
      if (totalShuffleBytes > 0) {
        val targetPartSizeMB = 128L // 128 MB per partition is a good target
        val suggested = math.max(1,
          (totalShuffleBytes / (targetPartSizeMB * 1024 * 1024)).toInt)
        val ratio = if (suggested > 0) shufflePartitions.toDouble / suggested else 1.0
        // Flag if partitions are 3× too many or too few
        if (ratio >= 3.0 || ratio <= 0.33) {
          suggestions += DefaultShufflePartitionsSuggestion(
            executionId         = Some(execId),
            currentPartitions   = shufflePartitions,
            totalShuffleBytes   = totalShuffleBytes,
            suggestedPartitions = suggested
          )
        }
      }
    }

    suggestions.toSeq
  }

  /** Build a map of stageId → total shuffle write bytes from completed stages. */
  private def buildStageShuffleWrites(store: AppStatusStore): Map[Int, Long] = {
    val completed = Arrays.asList(StageStatus.COMPLETE)
    try {
      store.stageList(completed).collect {
        case s if s.shuffleWriteBytes > 0 =>
          s.stageId -> s.shuffleWriteBytes
      }.toMap
    } catch {
      case _: Throwable => Map.empty
    }
  }

  /** Build a map of stageId → total input bytes from completed stages. */
  private def buildStageInputBytes(store: AppStatusStore): Map[Int, Long] = {
    val completed = Arrays.asList(StageStatus.COMPLETE)
    try {
      store.stageList(completed).collect {
        case s if s.inputBytes > 0 =>
          s.stageId -> s.inputBytes
      }.toMap
    } catch {
      case _: Throwable => Map.empty
    }
  }
}
