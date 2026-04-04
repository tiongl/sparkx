package org.apache.spark.ui.xpark

import com.xpark.XParkConfig
import com.xpark.analysis._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.StageStatus

import java.util.Arrays

object IssueDetector {
  private val QUANTILES = Array(0.0, 0.25, 0.5, 0.75, 0.95, 1.0)
  private val Q_MIN = 0; private val Q_Q1 = 1; private val Q_MED = 2
  private val Q_Q3  = 3; private val Q_MAX = 5

  private def activeAndComplete = Arrays.asList(StageStatus.COMPLETE, StageStatus.ACTIVE)

  def detect(store: AppStatusStore, config: XParkConfig): Seq[PerformanceIssue] = {
    val issues = scala.collection.mutable.ArrayBuffer[PerformanceIssue]()
    val stages = store.stageList(activeAndComplete)

    for (stage <- stages) {
      val stageIdOpt = Some(stage.stageId)
      val name       = stage.name

      if (stage.executorRunTime > 0) {
        val gcRatio = stage.jvmGcTime.toDouble / stage.executorRunTime.toDouble
        if (gcRatio >= config.gcRatioThreshold)
          issues += GCPressureIssue(stageIdOpt, name, gcRatio, stage.jvmGcTime, stage.executorRunTime)
      }

      if (stage.diskBytesSpilled > 0)
        issues += ShuffleSpillIssue(stageIdOpt, name, stage.diskBytesSpilled, stage.memoryBytesSpilled)

      store.taskSummary(stage.stageId, stage.attemptId, QUANTILES).foreach { dist =>
        val rt = dist.executorRunTime
        if (rt.length == QUANTILES.length) {
          val q1 = rt(Q_Q1); val med = rt(Q_MED); val q3 = rt(Q_Q3); val max = rt(Q_MAX)
          if (med > 0 && max / med >= config.skewMultiplier)
            issues += DataSkewIssue(stageIdOpt, name, max.toLong, med.toLong, max / med)
          val iqr = q3 - q1
          val upperFence = q3 + config.stragglerIQRFactor * iqr
          if (iqr > 0 && max > upperFence)
            issues += StragglerIssue(stageIdOpt, name, max, upperFence)
        }
      }
    }

    val broadcastThreshold = config.broadcastSizeMB * 1024L * 1024L
    store.rddList(cachedOnly = false).foreach { rdd =>
      if (rdd.name.startsWith("broadcast_") && rdd.memoryUsed >= broadcastThreshold)
        issues += BroadcastSizeIssue(rdd.name, rdd.memoryUsed / (1024 * 1024))
    }

    issues.toSeq
  }

  def skewStages(store: AppStatusStore, config: XParkConfig): Seq[(Int, String, Double, Double, Double)] = {
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

  def gcStages(store: AppStatusStore): Seq[(Int, String, Long, Long, Double)] = {
    store.stageList(activeAndComplete).map { s =>
      val ratio = if (s.executorRunTime > 0) s.jvmGcTime.toDouble / s.executorRunTime else 0.0
      (s.stageId, s.name, s.jvmGcTime, s.executorRunTime, ratio)
    }.filter(_._5 > 0).sortBy(-_._5)
  }

  def spillStages(store: AppStatusStore): Seq[(Int, String, Long, Long)] = {
    store.stageList(activeAndComplete)
      .filter(_.diskBytesSpilled > 0)
      .map(s => (s.stageId, s.name, s.diskBytesSpilled, s.memoryBytesSpilled))
      .sortBy(-_._3)
  }

  def stragglerStages(store: AppStatusStore, config: XParkConfig): Seq[(Int, String, Double, Double, Double, Double)] = {
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
    }
  }

  def broadcastRDDs(store: AppStatusStore, config: XParkConfig): Seq[(String, Long)] = {
    val threshold = config.broadcastSizeMB * 1024L * 1024L
    store.rddList(cachedOnly = false)
      .filter(r => r.name.startsWith("broadcast_") && r.memoryUsed >= threshold)
      .map(r => (r.name, r.memoryUsed / (1024 * 1024)))
      .sortBy(-_._2)
  }
}
