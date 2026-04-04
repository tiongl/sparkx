package com.xpark.analysis

import com.xpark.Utils

sealed trait Severity
case object Critical extends Severity { override def toString = "Critical" }
case object Warning  extends Severity { override def toString = "Warning" }
case object Info     extends Severity { override def toString = "Info" }

sealed trait PerformanceIssue {
  def severity:    Severity
  def stageId:     Option[Int]
  def title:       String
  def description: String
}

case class DataSkewIssue(
  stageId:          Option[Int],
  stageName:        String,
  maxDurationMs:    Long,
  medianDurationMs: Long,
  ratio:            Double
) extends PerformanceIssue {
  val severity: Severity = if (ratio >= 5.0) Critical else Warning
  val title = "Data Skew"
  val description =
    s"Max task (${Utils.formatDuration(maxDurationMs)}) is ${f"$ratio%.1f"}× the median " +
    s"(${Utils.formatDuration(medianDurationMs)}) in stage '$stageName'"
}

case class GCPressureIssue(
  stageId:   Option[Int],
  stageName: String,
  gcRatio:   Double,
  gcTimeMs:  Long,
  runTimeMs: Long
) extends PerformanceIssue {
  val severity: Severity = if (gcRatio >= 0.20) Critical else Warning
  val title = "GC Pressure"
  val description =
    s"GC is ${f"${gcRatio * 100}%.1f"}% of executor run time in stage '$stageName' " +
    s"(${Utils.formatDuration(gcTimeMs)} GC / ${Utils.formatDuration(runTimeMs)} run)"
}

case class ShuffleSpillIssue(
  stageId:        Option[Int],
  stageName:      String,
  diskSpillBytes: Long,
  memSpillBytes:  Long
) extends PerformanceIssue {
  val severity: Severity = Warning
  val title = "Shuffle Spill"
  val description =
    s"Stage '$stageName' spilled ${Utils.formatBytes(diskSpillBytes)} to disk" +
    (if (memSpillBytes > 0) s" and ${Utils.formatBytes(memSpillBytes)} to memory" else "")
}

case class StragglerIssue(
  stageId:       Option[Int],
  stageName:     String,
  maxDurationMs: Double,
  thresholdMs:   Double
) extends PerformanceIssue {
  val severity: Severity = Warning
  val title = "Straggler Tasks"
  val description =
    s"Stage '$stageName' has stragglers: max ${Utils.formatDuration(maxDurationMs.toLong)} " +
    s"exceeds IQR threshold ${Utils.formatDuration(thresholdMs.toLong)}"
}

case class BroadcastSizeIssue(
  rddName: String,
  sizeMB:  Long
) extends PerformanceIssue {
  val stageId:     Option[Int] = None
  val severity:    Severity    = Warning
  val title       = "Large Broadcast"
  val description = s"Broadcast '$rddName' is ${sizeMB} MB, consider filtering before broadcasting"
}
