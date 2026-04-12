package com.sparkx.analysis

/**
 * Groups co-occurring PerformanceIssues into root-cause clusters.
 *
 * A root cause is identified when 2+ symptoms from a known cluster
 * co-occur on the same stage (or at application scope for global signals).
 * Issues that don't match any cluster are returned as unclustered.
 */
object RootCauseAnalyzer {

  case class RootCause(
    name:            String,
    recommendation:  String,
    affectedStages:  Set[Int],
    issues:          Seq[PerformanceIssue],
    wallClockSavMs:  Long,
    computeSavMs:    Long
  )

  case class AnalysisResult(
    rootCauses:       Seq[RootCause],
    unclusteredIssues: Seq[PerformanceIssue]
  )

  private case class ClusterDef(
    name:           String,
    recommendation: String,
    primary:        Set[String],   // at least one primary must be present
    secondary:      Set[String],   // at least one secondary must co-occur
    appWide:        Boolean = false // if true, match across all stages
  )

  private val clusters = Seq(
    ClusterDef(
      name           = "Uneven Data Distribution",
      recommendation = "Salt skewed keys, repartition by a more uniform column, or use Spark AQE " +
                       "(spark.sql.adaptive.skewJoin.enabled=true) to split oversized partitions.",
      primary        = Set("Data Skew"),
      secondary      = Set("Straggler Tasks", "Shuffle Spill", "Executor Memory Skew", "GC Pressure")
    ),
    ClusterDef(
      name           = "Memory Pressure",
      recommendation = "Increase spark.executor.memory, reduce partition size with repartition(), " +
                       "or filter data earlier in the pipeline to reduce per-task memory footprint.",
      primary        = Set("GC Pressure"),
      secondary      = Set("Shuffle Spill", "Large Broadcast", "Disk Shuffle Read")
    ),
    ClusterDef(
      name           = "Over-partitioned Workload",
      recommendation = "Use coalesce() to reduce partition count, increase " +
                       "spark.sql.files.maxPartitionBytes, or raise spark.default.parallelism.",
      primary        = Set("Small Tasks (Over-partitioned)"),
      secondary      = Set("High Scheduler Delay")
    ),
    ClusterDef(
      name           = "Under-parallelized Workload",
      recommendation = "Increase partition count with repartition(), raise " +
                       "spark.sql.shuffle.partitions, or tune spark.default.parallelism " +
                       "to match available cores.",
      primary        = Set("Under-partitioned Stage"),
      secondary      = Set("Low CPU Utilization", "Straggler Tasks")
    ),
    ClusterDef(
      name           = "Serialization Overhead",
      recommendation = "Enable Kryo serialization (spark.serializer=org.apache.spark.serializer.KryoSerializer), " +
                       "reduce closure size, avoid capturing large driver objects in lambdas, " +
                       "and prefer write() over collect().",
      primary        = Set("High Task Deserialization"),
      secondary      = Set("Large Task Result", "Slow Result Serialization")
    ),
    ClusterDef(
      name           = "Node / Executor Instability",
      recommendation = "Investigate executor logs for OOM kills, disk failures, or network issues. " +
                       "Consider enabling spark.speculation and increasing executor memory headroom.",
      primary        = Set("Task Failures"),
      secondary      = Set("Speculative Tasks", "Stage Retry")
    )
  )

  def analyze(issues: Seq[PerformanceIssue]): AnalysisResult = {
    val claimed = scala.collection.mutable.Set[PerformanceIssue]()
    val rootCauses = scala.collection.mutable.ArrayBuffer[RootCause]()

    // Group issues by stage (None → app-wide bucket -1)
    val byStage: Map[Int, Seq[PerformanceIssue]] =
      issues.groupBy(_.stageId.getOrElse(-1))

    for (clusterDef <- clusters) {
      val matchedIssues = scala.collection.mutable.ArrayBuffer[PerformanceIssue]()

      for ((_, stageIssues) <- byStage) {
        val titles = stageIssues.map(_.title).toSet
        val hasPrimary   = clusterDef.primary.exists(titles.contains)
        val hasSecondary = clusterDef.secondary.exists(titles.contains)

        if (hasPrimary && hasSecondary) {
          val relevant = stageIssues.filter(i =>
            clusterDef.primary.contains(i.title) || clusterDef.secondary.contains(i.title))
          matchedIssues ++= relevant
        }
      }

      // Also check app-wide: primary in one stage, secondary in another or app-wide
      if (matchedIssues.isEmpty) {
        val allTitles = issues.map(_.title).toSet
        val hasPrimary   = clusterDef.primary.exists(allTitles.contains)
        val hasSecondary = clusterDef.secondary.exists(allTitles.contains)

        // For app-wide signals (broadcasts, executor memory skew) allow cross-stage matching
        val hasAppWideSecondary = clusterDef.secondary.exists { t =>
          issues.exists(i => i.title == t && i.stageId.isEmpty)
        }

        if (hasPrimary && (hasSecondary || hasAppWideSecondary)) {
          val relevant = issues.filter(i =>
            clusterDef.primary.contains(i.title) || clusterDef.secondary.contains(i.title))
          matchedIssues ++= relevant
        }
      }

      if (matchedIssues.nonEmpty) {
        val deduplicated = matchedIssues.distinct.filterNot(claimed.contains)
        if (deduplicated.nonEmpty) {
          claimed ++= deduplicated
          val stages = deduplicated.flatMap(_.stageId).toSet

          // Aggregate savings conservatively: max for wall-clock (avoid double-counting),
          // max for compute within same cluster
          val wallClock = deduplicated
            .filter(_.savingsType == "Wall-clock")
            .flatMap(_.estimatedSavingsMs)
          val compute = deduplicated
            .filter(i => i.savingsType != "Wall-clock" && i.savingsType.nonEmpty)
            .flatMap(_.estimatedSavingsMs)

          rootCauses += RootCause(
            name           = clusterDef.name,
            recommendation = clusterDef.recommendation,
            affectedStages = stages,
            issues         = deduplicated.toSeq,
            wallClockSavMs = if (wallClock.nonEmpty) wallClock.max else 0L,
            computeSavMs   = if (compute.nonEmpty) compute.max else 0L
          )
        }
      }
    }

    val unclustered = issues.filterNot(claimed.contains)

    AnalysisResult(
      rootCauses        = rootCauses.toSeq.sortBy(rc => -(rc.wallClockSavMs + rc.computeSavMs)),
      unclusteredIssues = unclustered
    )
  }
}
