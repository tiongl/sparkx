package org.apache.spark.ui.sparkx

import com.sparkx.SparkXConfig
import com.sparkx.analysis._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{StageData, StageStatus}

import java.util.Arrays

object IssueDetector {
  private val QUANTILES = Array(0.0, 0.25, 0.5, 0.75, 0.95, 1.0)
  private val Q_MIN = 0; private val Q_Q1 = 1; private val Q_MED = 2
  private val Q_Q3  = 3; private val Q_P95 = 4; private val Q_MAX = 5

  private def activeAndComplete = Arrays.asList(StageStatus.COMPLETE, StageStatus.ACTIVE)

  // Lightweight TTL cache — avoids recomputing heavy per-stage taskSummary
  // queries on every page load.  30 s is short enough to pick up new stages.
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

  def detect(store: AppStatusStore, config: SparkXConfig): Seq[PerformanceIssue] = cached("detect") {
    val issues = scala.collection.mutable.ArrayBuffer[PerformanceIssue]()
    val stages = {
      // Deduplicate by stageId — keep latest attempt per stage to avoid duplicate issues
      val byId = scala.collection.mutable.LinkedHashMap[Int, StageData]()
      store.stageList(activeAndComplete).foreach { s =>
        byId.get(s.stageId) match {
          case Some(prev) if prev.attemptId >= s.attemptId => // keep prev
          case _ => byId(s.stageId) = s
        }
      }
      byId.values.toSeq
    }

    val activeCores = store.executorList(activeOnly = false)
      .filter(_.isActive).map(_.totalCores).sum

    for (stage <- stages) {
      val sid      = Some(stage.stageId)
      val name     = stage.name
      val avgTaskMs = if (stage.numCompleteTasks > 0)
        stage.executorRunTime / stage.numCompleteTasks else 0L

      // ── Existing detections ─────────────────────────────────────────────────
      if (stage.executorRunTime > 0) {
        val gcRatio = stage.jvmGcTime.toDouble / stage.executorRunTime.toDouble
        if (gcRatio >= config.gcRatioThreshold)
          issues += GCPressureIssue(sid, name, gcRatio, stage.jvmGcTime, stage.executorRunTime)
      }

      if (stage.diskBytesSpilled > 0)
        issues += ShuffleSpillIssue(sid, name, stage.diskBytesSpilled, stage.memoryBytesSpilled)

      val numSpeculative = stage.speculationSummary.map(_.numTasks).getOrElse(0)
      if (numSpeculative > 0)
        issues += SpeculativeTasksIssue(sid, name, numSpeculative, stage.numTasks, avgTaskMs)

      // Under-partitioned (check before taskSummary — uses stage-level numTasks)
      val coresForRatio = if (activeCores > 0) activeCores else 1
      if (stage.numTasks > 0 && stage.numTasks < (coresForRatio * config.underPartitionRatio).toInt.max(2))
        issues += UnderPartitionedIssue(sid, name, stage.numTasks, coresForRatio)

      // Shuffle amplification
      if (stage.inputBytes > 0 && stage.shuffleWriteBytes > 0) {
        val ratio = stage.shuffleWriteBytes.toDouble / stage.inputBytes.toDouble
        if (ratio >= config.shuffleAmplifyRatio)
          issues += ShuffleAmplificationIssue(
            sid, name, stage.inputBytes / (1024 * 1024),
            stage.shuffleWriteBytes / (1024 * 1024), ratio)
      }

      // ── Phase 2: Low CPU utilization ────────────────────────────────────────
      if (stage.executorRunTime >= config.lowCpuMinRunTimeMs && stage.executorCpuTime > 0) {
        val cpuRatio = stage.executorCpuTime.toDouble / stage.executorRunTime.toDouble
        if (cpuRatio < config.lowCpuRatioThreshold)
          issues += LowCpuUtilizationIssue(sid, name, cpuRatio,
            stage.executorCpuTime, stage.executorRunTime)
      }

      // ── Phase 2: Disk shuffle read (reader-side spill) ─────────────────────
      val diskShuffleThreshold = config.diskShuffleReadMinMB * 1024L * 1024L
      if (stage.shuffleReadBytes > 0) {
        val remoteToDisk = stage.shuffleRemoteBytesReadToDisk
        if (remoteToDisk >= diskShuffleThreshold)
          issues += DiskShuffleReadIssue(sid, name, remoteToDisk, stage.shuffleReadBytes)
      }

      store.taskSummary(stage.stageId, stage.attemptId, QUANTILES).foreach { dist =>
        val rt = dist.executorRunTime
        if (rt.length == QUANTILES.length) {
          val q1 = rt(Q_Q1); val med = rt(Q_MED); val q3 = rt(Q_Q3); val max = rt(Q_MAX)

          // Data skew
          if (med > 0 && max / med >= config.skewMultiplier)
            issues += DataSkewIssue(sid, name, max.toLong, med.toLong, max / med)

          // Stragglers
          val iqr = q3 - q1
          val upperFence = q3 + config.stragglerIQRFactor * iqr
          if (iqr > 0 && max > upperFence)
            issues += StragglerIssue(sid, name, max, upperFence)

          // Small tasks (over-partitioned) — use median scheduler delay * numTasks for savings
          if (stage.numTasks >= config.smallTaskMinCount && med < config.smallTaskMedianMs) {
            val totalSchedDelayMs = (dist.schedulerDelay(Q_MED) * stage.numTasks).toLong
            issues += SmallTasksIssue(sid, name, stage.numTasks, med.toLong, totalSchedDelayMs)
          }
        }

        // Large task result returned to driver
        val p95Result = dist.resultSize(Q_P95) / (1024.0 * 1024.0)
        if (p95Result >= config.largeResultMB)
          issues += LargeResultIssue(sid, name, p95Result.toLong)

        // High task deserialization time
        val p95Deser = dist.executorDeserializeTime(Q_P95)
        if (p95Deser >= config.highDeserMs)
          issues += HighDeserializationIssue(sid, name, p95Deser.toLong, stage.numTasks)

        // High shuffle fetch wait
        val p50FetchWait = dist.shuffleReadMetrics.fetchWaitTime(Q_MED)
        val p50RunTime   = dist.executorRunTime(Q_MED)
        if (p50RunTime > 0 && p50FetchWait / p50RunTime >= config.fetchWaitRatioThreshold)
          issues += HighFetchWaitIssue(sid, name, p50FetchWait / p50RunTime, p50FetchWait.toLong)

        // ── Phase 2: High scheduler delay ───────────────────────────────────────
        val p95SchedDelay = dist.schedulerDelay(Q_P95)
        val p50Run        = dist.executorRunTime(Q_MED)
        if (p95SchedDelay >= config.highSchedulerDelayMs &&
            p50Run > 0 && p95SchedDelay / p50Run >= config.schedulerDelayRatio)
          issues += HighSchedulerDelayIssue(sid, name, p95SchedDelay.toLong, p50Run.toLong, stage.numTasks)

        // ── Phase 2: Slow result serialization ──────────────────────────────────
        val p95ResultSer = dist.resultSerializationTime(Q_P95)
        if (p95ResultSer >= config.resultSerializationMs)
          issues += SlowResultSerializationIssue(sid, name, p95ResultSer.toLong, stage.numTasks)
      }
    }

    // Task failures — scan FAILED stages too so aborted jobs show data.
    // Deduplicate by stageId: keep the attempt with the most failed tasks.
    val allStatuses = Arrays.asList(StageStatus.COMPLETE, StageStatus.ACTIVE, StageStatus.FAILED)
    val failedByStage = scala.collection.mutable.LinkedHashMap[Int, StageData]()
    store.stageList(allStatuses).filter(_.numFailedTasks > 0).foreach { s =>
      failedByStage.get(s.stageId) match {
        case Some(prev) if prev.numFailedTasks >= s.numFailedTasks => // keep prev
        case _ => failedByStage(s.stageId) = s
      }
    }
    failedByStage.values.foreach { s =>
      val avg = if (s.numCompleteTasks > 0) s.executorRunTime / s.numCompleteTasks else 0L
      issues += TaskFailuresIssue(Some(s.stageId), s.name, s.numFailedTasks, s.numTasks, avg)
    }

    val broadcastThreshold = config.broadcastSizeMB * 1024L * 1024L
    store.rddList(cachedOnly = false).foreach { rdd =>
      if (rdd.name.startsWith("broadcast_") && rdd.memoryUsed >= broadcastThreshold)
        issues += BroadcastSizeIssue(rdd.name, rdd.memoryUsed / (1024 * 1024))
    }

    // ── Phase 2: Stage retries ────────────────────────────────────────────────
    {
      val allStatuses2 = Arrays.asList(StageStatus.COMPLETE, StageStatus.ACTIVE, StageStatus.FAILED)
      val attemptCounts = scala.collection.mutable.LinkedHashMap[Int, (String, Int, Long, Int)]()
      store.stageList(allStatuses2).foreach { s =>
        val (_, currentCount, _, _) = attemptCounts.getOrElse(s.stageId, (s.name, 0, 0L, s.numTasks))
        val avg = if (s.numCompleteTasks > 0) s.executorRunTime / s.numCompleteTasks else 0L
        attemptCounts(s.stageId) = (s.name, currentCount + 1, avg.max(attemptCounts.get(s.stageId).map(_._3).getOrElse(0L)), s.numTasks)
      }
      attemptCounts.foreach { case (sid, (sname, count, avgMs, numTasks)) =>
        if (count > 1)
          issues += StageRetryIssue(Some(sid), sname, count - 1, avgMs, numTasks)
      }
    }

    // ── Phase 2: Executor memory skew (app-wide) ─────────────────────────────
    {
      val executors = store.executorList(activeOnly = false).filter(_.isActive)
      if (executors.length >= config.executorMemoryMinCount) {
        val memValues = executors.flatMap(_.peakMemoryMetrics)
          .map(_.getMetricValue("JVMHeapMemory")).filter(_ > 0)
        if (memValues.length >= config.executorMemoryMinCount) {
          val mean = memValues.sum.toDouble / memValues.length
          if (mean > 0) {
            val variance = memValues.map(v => math.pow(v - mean, 2)).sum / memValues.length
            val cov = math.sqrt(variance) / mean
            val maxMB = memValues.max / (1024 * 1024)
            val minMB = memValues.min / (1024 * 1024)
            if (cov >= config.executorMemoryCov)
              issues += ExecutorMemorySkewIssue(None, "(all stages)", maxMB, minMB, cov, memValues.length)
          }
        }
      }
    }

    issues.toSeq
  }

  // ── Per-page query methods ──────────────────────────────────────────────────

  def skewStages(store: AppStatusStore, config: SparkXConfig): Seq[(Int, String, Double, Double, Double)] = cached("skewStages") {
    store.stageList(activeAndComplete).flatMap { stage =>
      store.taskSummary(stage.stageId, stage.attemptId, QUANTILES).flatMap { dist =>
        val rt = dist.executorRunTime
        if (rt.length == QUANTILES.length) {
          val med = rt(Q_MED); val max = rt(Q_MAX)
          Some((stage.stageId, stage.name, med, max, if (med > 0) max / med else 0.0))
        } else None
      }
    }
  }

  def gcStages(store: AppStatusStore): Seq[(Int, String, Long, Long, Double)] = cached("gcStages") {
    store.stageList(activeAndComplete).map { s =>
      val ratio = if (s.executorRunTime > 0) s.jvmGcTime.toDouble / s.executorRunTime else 0.0
      (s.stageId, s.name, s.jvmGcTime, s.executorRunTime, ratio)
    }.filter(_._5 > 0).sortBy(-_._3)
  }

  def spillStages(store: AppStatusStore): Seq[(Int, String, Long, Long)] = cached("spillStages") {
    store.stageList(activeAndComplete)
      .filter(_.diskBytesSpilled > 0)
      .map(s => (s.stageId, s.name, s.diskBytesSpilled, s.memoryBytesSpilled))
      .sortBy(-_._3)
  }

  def stragglerStages(store: AppStatusStore, config: SparkXConfig): Seq[(Int, String, Double, Double, Double, Double)] = cached("stragglerStages") {
    store.stageList(activeAndComplete).flatMap { stage =>
      store.taskSummary(stage.stageId, stage.attemptId, QUANTILES).flatMap { dist =>
        val rt = dist.executorRunTime
        if (rt.length == QUANTILES.length) {
          val q1 = rt(Q_Q1); val q3 = rt(Q_Q3); val max = rt(Q_MAX); val med = rt(Q_MED)
          val iqr = q3 - q1
          val fence = q3 + config.stragglerIQRFactor * iqr
          if (iqr > 0 && max > fence) Some((stage.stageId, stage.name, med, q3, max, fence))
          else None
        } else None
      }
    }.sortBy(r => -(r._5 - r._6).toLong)
  }

  def broadcastRDDs(store: AppStatusStore, config: SparkXConfig): Seq[(String, Long)] = cached("broadcastRDDs") {
    val threshold = config.broadcastSizeMB * 1024L * 1024L
    store.rddList(cachedOnly = false)
      .filter(r => r.name.startsWith("broadcast_") && r.memoryUsed >= threshold)
      .map(r => (r.name, r.memoryUsed / (1024 * 1024)))
      .sortBy(-_._2)
  }

  // ── Partitioning page ───────────────────────────────────────────────────────

  def smallTaskStages(store: AppStatusStore, config: SparkXConfig)
      : Seq[(Int, String, Int, Double, Long)] = cached("smallTaskStages") {
    store.stageList(activeAndComplete).flatMap { stage =>
      if (stage.numTasks < config.smallTaskMinCount) None
      else store.taskSummary(stage.stageId, stage.attemptId, QUANTILES).flatMap { dist =>
        val med             = dist.executorRunTime(Q_MED)
        val totalSchedDelay = (dist.schedulerDelay(Q_MED) * stage.numTasks).toLong
        Some((stage.stageId, stage.name, stage.numTasks, med, totalSchedDelay))
      }
    }.sortBy(-_._5) // highest savings (scheduling overhead) first
  }

  def underPartitionedStages(store: AppStatusStore, config: SparkXConfig)
      : Seq[(Int, String, Int, Int)] = cached("underPartitionedStages") {
    val cores = store.executorList(activeOnly = false).filter(_.isActive).map(_.totalCores).sum
    val effectiveCores = if (cores > 0) cores else 1
    val minTasks = (effectiveCores * config.underPartitionRatio).toInt.max(2)
    store.stageList(activeAndComplete)
      .filter(s => s.numTasks > 0 && s.numTasks < minTasks)
      .map(s => (s.stageId, s.name, s.numTasks, effectiveCores))
      .sortBy(_._3)
  }

  def shuffleAmplifiedStages(store: AppStatusStore, config: SparkXConfig)
      : Seq[(Int, String, Long, Long, Double)] = cached("shuffleAmplifiedStages") {
    store.stageList(activeAndComplete).flatMap { s =>
      if (s.inputBytes <= 0 || s.shuffleWriteBytes <= 0) None
      else {
        val ratio = s.shuffleWriteBytes.toDouble / s.inputBytes.toDouble
        if (ratio >= config.shuffleAmplifyRatio)
          Some((s.stageId, s.name, s.inputBytes / (1024*1024), s.shuffleWriteBytes / (1024*1024), ratio))
        else None
      }
    }.sortBy(-_._5)
  }

  // ── Stability page ──────────────────────────────────────────────────────────

  def failedTaskStages(store: AppStatusStore): Seq[(Int, String, Int, Int, Long)] = cached("failedTaskStages") {
    val statuses = Arrays.asList(StageStatus.COMPLETE, StageStatus.ACTIVE, StageStatus.FAILED)
    val byStage = scala.collection.mutable.LinkedHashMap[Int, StageData]()
    store.stageList(statuses).filter(_.numFailedTasks > 0).foreach { s =>
      byStage.get(s.stageId) match {
        case Some(prev) if prev.numFailedTasks >= s.numFailedTasks => // keep prev
        case _ => byStage(s.stageId) = s
      }
    }
    byStage.values.map { s =>
      val avg = if (s.numCompleteTasks > 0) s.executorRunTime / s.numCompleteTasks else 0L
      (s.stageId, s.name, s.numFailedTasks, s.numTasks, avg)
    }.toSeq.sortBy(r => -(r._3.toLong * r._5))
  }

  def speculativeStages(store: AppStatusStore): Seq[(Int, String, Int, Int, Long)] = cached("speculativeStages") {
    store.stageList(activeAndComplete)
      .filter(_.speculationSummary.exists(_.numTasks > 0))
      .map { s =>
        val avg = if (s.numCompleteTasks > 0) s.executorRunTime / s.numCompleteTasks else 0L
        (s.stageId, s.name, s.speculationSummary.map(_.numTasks).getOrElse(0), s.numTasks, avg)
      }
      .sortBy(r => -(r._3.toLong * r._5))
  }

  def largeResultStages(store: AppStatusStore, config: SparkXConfig)
      : Seq[(Int, String, Long)] = cached("largeResultStages") {
    store.stageList(activeAndComplete).flatMap { stage =>
      store.taskSummary(stage.stageId, stage.attemptId, QUANTILES).flatMap { dist =>
        val p95MB = dist.resultSize(Q_P95) / (1024.0 * 1024.0)
        if (p95MB >= config.largeResultMB) Some((stage.stageId, stage.name, p95MB.toLong))
        else None
      }
    }.sortBy(-_._3)
  }

  def highFetchWaitStages(store: AppStatusStore, config: SparkXConfig)
      : Seq[(Int, String, Double, Long, Long)] = cached("highFetchWaitStages") {
    store.stageList(activeAndComplete).flatMap { stage =>
      store.taskSummary(stage.stageId, stage.attemptId, QUANTILES).flatMap { dist =>
        val p50Run   = dist.executorRunTime(Q_MED)
        val p50Fetch = dist.shuffleReadMetrics.fetchWaitTime(Q_MED)
        if (p50Run > 0) {
          val ratio = p50Fetch / p50Run
          if (ratio >= config.fetchWaitRatioThreshold)
            Some((stage.stageId, stage.name, ratio, p50Fetch.toLong, p50Run.toLong))
          else None
        } else None
      }
    }.sortBy(-_._4)
  }

  def highDeserStages(store: AppStatusStore, config: SparkXConfig)
      : Seq[(Int, String, Long, Int)] = cached("highDeserStages") {
    store.stageList(activeAndComplete).flatMap { stage =>
      store.taskSummary(stage.stageId, stage.attemptId, QUANTILES).flatMap { dist =>
        val p95Deser = dist.executorDeserializeTime(Q_P95)
        if (p95Deser >= config.highDeserMs)
          Some((stage.stageId, stage.name, p95Deser.toLong, stage.numTasks))
        else None
      }
    }.sortBy(r => -(r._3 * r._4))
  }

  // ── Phase 2 per-page query methods ─────────────────────────────────────────

  def lowCpuStages(store: AppStatusStore, config: SparkXConfig)
      : Seq[(Int, String, Double, Long, Long)] = cached("lowCpuStages") {
    store.stageList(activeAndComplete).flatMap { s =>
      if (s.executorRunTime >= config.lowCpuMinRunTimeMs && s.executorCpuTime > 0) {
        val ratio = s.executorCpuTime.toDouble / s.executorRunTime.toDouble
        Some((s.stageId, s.name, ratio, s.executorCpuTime, s.executorRunTime))
      } else None
    }.sortBy(_._3)
  }

  def diskShuffleReadStages(store: AppStatusStore, config: SparkXConfig)
      : Seq[(Int, String, Long, Long)] = cached("diskShuffleReadStages") {
    val threshold = config.diskShuffleReadMinMB * 1024L * 1024L
    store.stageList(activeAndComplete).flatMap { s =>
      if (s.shuffleReadBytes > 0) {
        val remoteToDisk = s.shuffleRemoteBytesReadToDisk
        if (remoteToDisk >= threshold)
          Some((s.stageId, s.name, remoteToDisk, s.shuffleReadBytes))
        else None
      } else None
    }.sortBy(-_._3)
  }

  def highSchedulerDelayStages(store: AppStatusStore, config: SparkXConfig)
      : Seq[(Int, String, Long, Long, Int)] = cached("highSchedulerDelayStages") {
    store.stageList(activeAndComplete).flatMap { stage =>
      store.taskSummary(stage.stageId, stage.attemptId, QUANTILES).flatMap { dist =>
        val p95Delay = dist.schedulerDelay(Q_P95)
        val p50Run   = dist.executorRunTime(Q_MED)
        if (p95Delay >= config.highSchedulerDelayMs &&
            p50Run > 0 && p95Delay / p50Run >= config.schedulerDelayRatio)
          Some((stage.stageId, stage.name, p95Delay.toLong, p50Run.toLong, stage.numTasks))
        else None
      }
    }.sortBy(-_._3)
  }

  def stageRetries(store: AppStatusStore)
      : Seq[(Int, String, Int, Long, Int)] = cached("stageRetries") {
    val allStatuses = Arrays.asList(StageStatus.COMPLETE, StageStatus.ACTIVE, StageStatus.FAILED)
    val attemptData = scala.collection.mutable.LinkedHashMap[Int, (String, Int, Long, Int)]()
    store.stageList(allStatuses).foreach { s =>
      val (_, count, prevAvg, _) = attemptData.getOrElse(s.stageId, (s.name, 0, 0L, s.numTasks))
      val avg = if (s.numCompleteTasks > 0) s.executorRunTime / s.numCompleteTasks else 0L
      attemptData(s.stageId) = (s.name, count + 1, avg.max(prevAvg), s.numTasks)
    }
    attemptData.filter(_._2._2 > 1).map { case (sid, (name, count, avgMs, numTasks)) =>
      (sid, name, count - 1, avgMs, numTasks)
    }.toSeq.sortBy(r => -(r._3.toLong * r._4 * r._5))
  }

  def slowResultSerStages(store: AppStatusStore, config: SparkXConfig)
      : Seq[(Int, String, Long, Int)] = cached("slowResultSerStages") {
    store.stageList(activeAndComplete).flatMap { stage =>
      store.taskSummary(stage.stageId, stage.attemptId, QUANTILES).flatMap { dist =>
        val p95Ser = dist.resultSerializationTime(Q_P95)
        if (p95Ser >= config.resultSerializationMs)
          Some((stage.stageId, stage.name, p95Ser.toLong, stage.numTasks))
        else None
      }
    }.sortBy(-_._3)
  }

  def executorMemoryDistribution(store: AppStatusStore)
      : Seq[(String, Long, Long)] = cached("executorMemoryDist") {
    store.executorList(activeOnly = false).filter(_.isActive).flatMap { e =>
      e.peakMemoryMetrics.map(_.getMetricValue("JVMHeapMemory")).filter(_ > 0).map { peak =>
        (e.id, peak / (1024 * 1024), e.maxMemory / (1024 * 1024))
      }
    }.sortBy(-_._2)
  }

  // ── Cross-referencing helpers ───────────────────────────────────────────────

  /** Stage ID → Job ID (latest job that owns the stage). */
  def stageJobMap(store: AppStatusStore): Map[Int, Int] = cached("stageJobMap") {
    store.jobsList(null).flatMap(j => j.stageIds.map(sid => sid -> j.jobId)).toMap
  }

  /** Stage ID → SQL execution ID (for DAG link). Gracefully empty if Spark SQL is absent. */
  def stageSqlExecMap(store: AppStatusStore): Map[Int, Long] = cached("stageSqlExecMap") {
    try {
      val sqlStore = new org.apache.spark.sql.execution.ui.SQLAppStatusStore(store.store)
      sqlStore.executionsList().flatMap { exec =>
        exec.stages.map(sid => sid -> exec.executionId)
      }.toMap
    } catch {
      case _: Throwable => Map.empty[Int, Long]
    }
  }
}
