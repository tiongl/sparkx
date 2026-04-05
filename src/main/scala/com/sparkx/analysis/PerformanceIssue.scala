package com.sparkx.analysis

import com.sparkx.Utils

sealed trait Severity
case object Critical extends Severity { override def toString = "Critical" }
case object Warning  extends Severity { override def toString = "Warning" }
case object Info     extends Severity { override def toString = "Info" }

sealed trait PerformanceIssue {
  def severity:           Severity
  def stageId:            Option[Int]
  def title:              String
  def description:        String
  def detailPath:         String          // sparkx sub-page ("skew", "gc", etc.)
  def estimatedSavingsMs: Option[Long]    // estimated time that could be saved
  def savingsType:        String          // "Wall-clock" | "Compute" | ""
}

case class DataSkewIssue(
  stageId:          Option[Int],
  stageName:        String,
  maxDurationMs:    Long,
  medianDurationMs: Long,
  ratio:            Double
) extends PerformanceIssue {
  val severity: Severity = if (ratio >= 5.0) Critical else Warning
  val detailPath = "skew"
  val title = "Data Skew"
  val description =
    s"Max task (${Utils.formatDuration(maxDurationMs)}) is ${f"$ratio%.1f"}× the median " +
    s"(${Utils.formatDuration(medianDurationMs)}) in stage '$stageName'"
  // Stage finishes as fast as its slowest task — fix skew and it finishes in ~median time
  val estimatedSavingsMs: Option[Long] = Some((maxDurationMs - medianDurationMs).max(0))
  val savingsType = "Wall-clock"
}

case class GCPressureIssue(
  stageId:   Option[Int],
  stageName: String,
  gcRatio:   Double,
  gcTimeMs:  Long,
  runTimeMs: Long
) extends PerformanceIssue {
  val severity: Severity = if (gcRatio >= 0.20) Critical else Warning
  val detailPath = "gc"
  val title = "GC Pressure"
  val description =
    s"GC is ${f"${gcRatio * 100}%.1f"}% of executor run time in stage '$stageName' " +
    s"(${Utils.formatDuration(gcTimeMs)} GC / ${Utils.formatDuration(runTimeMs)} run)"
  // GC time is directly measured wasted executor CPU time
  val estimatedSavingsMs: Option[Long] = Some(gcTimeMs.max(0))
  val savingsType = "Compute"
}

case class ShuffleSpillIssue(
  stageId:        Option[Int],
  stageName:      String,
  diskSpillBytes: Long,
  memSpillBytes:  Long
) extends PerformanceIssue {
  val severity: Severity = Warning
  val detailPath = "spill"
  val title = "Shuffle Spill"
  val description =
    s"Stage '$stageName' spilled ${Utils.formatBytes(diskSpillBytes)} to disk" +
    (if (memSpillBytes > 0) s" and ${Utils.formatBytes(memSpillBytes)} to memory" else "")
  // Disk I/O time: read + write back at ~50 MB/s effective throughput
  val estimatedSavingsMs: Option[Long] = Some((diskSpillBytes * 2L / (50L * 1024 * 1024) * 1000).max(0))
  val savingsType = "I/O"
}

case class StragglerIssue(
  stageId:       Option[Int],
  stageName:     String,
  maxDurationMs: Double,
  thresholdMs:   Double
) extends PerformanceIssue {
  val severity: Severity = Warning
  val detailPath = "stragglers"
  val title = "Straggler Tasks"
  val description =
    s"Stage '$stageName' has stragglers: max ${Utils.formatDuration(maxDurationMs.toLong)} " +
    s"exceeds IQR threshold ${Utils.formatDuration(thresholdMs.toLong)}"
  // Stage is held up until the straggler finishes; eliminating stragglers caps duration at the fence
  val estimatedSavingsMs: Option[Long] = Some((maxDurationMs - thresholdMs).toLong.max(0))
  val savingsType = "Wall-clock"
}

case class BroadcastSizeIssue(
  rddName: String,
  sizeMB:  Long
) extends PerformanceIssue {
  val stageId:            Option[Int]  = None
  val severity:           Severity     = Warning
  val detailPath         = "broadcast"
  val title              = "Large Broadcast"
  val description        = s"Broadcast '$rddName' is ${sizeMB} MB, consider filtering before broadcasting"
  val estimatedSavingsMs: Option[Long] = None
  val savingsType                      = ""
}

// ── New detections ────────────────────────────────────────────────────────────

case class SmallTasksIssue(
  stageId:              Option[Int],
  stageName:            String,
  numTasks:             Int,
  medianMs:             Long,
  totalSchedulerDelayMs: Long   // sum of scheduler delay across all tasks (= overhead)
) extends PerformanceIssue {
  val severity   = Warning
  val detailPath = "partitioning"
  val title      = "Small Tasks (Over-partitioned)"
  val description =
    s"Stage '$stageName' has $numTasks tasks with median ${Utils.formatDuration(medianMs)} each. " +
    s"Scheduling overhead dominates useful work — consider coalesce() or increasing input split size."
  // Scheduler delay = time each task waits before actually starting — that time is pure overhead
  val estimatedSavingsMs: Option[Long] = Some(totalSchedulerDelayMs.max(0))
  val savingsType = "Scheduling overhead"
}

case class UnderPartitionedIssue(
  stageId:       Option[Int],
  stageName:     String,
  numTasks:      Int,
  availableCores: Int
) extends PerformanceIssue {
  val severity   = Warning
  val detailPath = "partitioning"
  val title      = "Under-partitioned Stage"
  val description =
    s"Stage '$stageName' has only $numTasks task(s) but $availableCores core(s) are available. " +
    s"Most cores will sit idle — consider repartition() or increasing spark.default.parallelism."
  val estimatedSavingsMs: Option[Long] = None
  val savingsType                      = ""
}

case class ShuffleAmplificationIssue(
  stageId:        Option[Int],
  stageName:      String,
  inputMB:        Long,
  shuffleWriteMB: Long,
  ratio:          Double
) extends PerformanceIssue {
  val severity   = if (ratio >= 20.0) Critical else Warning
  val detailPath = "partitioning"
  val title      = "Shuffle Amplification"
  val description =
    s"Stage '$stageName' writes ${shuffleWriteMB} MB to shuffle but only read ${inputMB} MB input " +
    s"(${f"$ratio%.1f"}× amplification). Check for cartesian joins, explode(), or missing filters."
  val estimatedSavingsMs: Option[Long] = None
  val savingsType                      = ""
}

case class TaskFailuresIssue(
  stageId:    Option[Int],
  stageName:  String,
  numFailed:  Int,
  numTotal:   Int,
  avgTaskMs:  Long   // average task duration = wasted per failed retry
) extends PerformanceIssue {
  val severity   = Critical
  val detailPath = "stability"
  val title      = "Task Failures"
  val description =
    s"Stage '$stageName' had $numFailed failed task(s) out of $numTotal. " +
    s"Failures waste resources and may cause incorrect results."
  // Each failed task wastes avgTaskMs before the retry (or stage abort)
  val estimatedSavingsMs: Option[Long] = Some((numFailed.toLong * avgTaskMs).max(0))
  val savingsType = "Compute"
}

case class SpeculativeTasksIssue(
  stageId:        Option[Int],
  stageName:      String,
  numSpeculative: Int,
  numTotal:       Int,
  avgTaskMs:      Long   // average task duration used to estimate speculative cost
) extends PerformanceIssue {
  val severity   = Warning
  val detailPath = "stability"
  val title      = "Speculative Tasks"
  val description =
    s"Stage '$stageName' launched $numSpeculative speculative task(s) (Spark re-ran slow tasks). " +
    s"Root cause is typically data skew, GC pressure, or node degradation."
  // Each speculative re-run wastes ~avgTaskMs of additional compute
  val estimatedSavingsMs: Option[Long] = Some((numSpeculative.toLong * avgTaskMs).max(0))
  val savingsType = "Compute"
}

case class LargeResultIssue(
  stageId:     Option[Int],
  stageName:   String,
  p95ResultMB: Long
) extends PerformanceIssue {
  val severity   = Warning
  val detailPath = "stability"
  val title      = "Large Task Result"
  val description =
    s"Stage '$stageName' tasks return up to ~${p95ResultMB} MB to the driver (P95). " +
    s"Avoid collect() on large datasets — write to storage instead."
  val estimatedSavingsMs: Option[Long] = None
  val savingsType                      = ""
}

case class HighFetchWaitIssue(
  stageId:        Option[Int],
  stageName:      String,
  fetchWaitRatio: Double,
  p50FetchWaitMs: Long
) extends PerformanceIssue {
  val severity   = Warning
  val detailPath = "stability"
  val title      = "High Shuffle Fetch Wait"
  val description =
    s"Stage '$stageName' spends ${f"${fetchWaitRatio * 100}%.1f"}% of task time waiting for " +
    s"shuffle data (median ${Utils.formatDuration(p50FetchWaitMs)}). " +
    s"Suggests network bottleneck or shuffle service overload."
  // Tasks are blocked for p50FetchWaitMs — that's wall-clock time lost per wave of tasks
  val estimatedSavingsMs: Option[Long] = Some(p50FetchWaitMs.max(0))
  val savingsType = "Wall-clock"
}

case class HighDeserializationIssue(
  stageId:    Option[Int],
  stageName:  String,
  p95DeserMs: Long,
  numTasks:   Int
) extends PerformanceIssue {
  val severity   = Warning
  val detailPath = "stability"
  val title      = "High Task Deserialization"
  val description =
    s"Stage '$stageName' tasks take up to ~${Utils.formatDuration(p95DeserMs)} to deserialize (P95). " +
    s"Large task closures or complex broadcast variables are common causes."
  // Total compute wasted on deserializing tasks that don't need such large payloads
  val estimatedSavingsMs: Option[Long] = Some((p95DeserMs * numTasks).max(0))
  val savingsType = "Compute"
}

