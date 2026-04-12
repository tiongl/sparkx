package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import com.sparkx.analysis.PerformanceIssue
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

/**
 * Stage-centric view: lists every stage with the issues detected on it and
 * estimated wall-clock / compute savings side-by-side, so users can quickly
 * see which stages have the highest optimisation potential.
 */
class StagesSummaryPage(parent: SparkXTab) extends WebUIPage("stages") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val allIssues = IssueDetector.detect(parent.sparkUI.store, parent.config)
    val stageJobs    = IssueDetector.stageJobMap(parent.sparkUI.store)
    val stageSqlExec = IssueDetector.stageSqlExecMap(parent.sparkUI.store)

    // Group issues by stageId (issues with no stageId go under a special -1 bucket)
    val byStage: Map[Int, Seq[PerformanceIssue]] =
      allIssues.groupBy(_.stageId.getOrElse(-1))

    // Build one row per stage that actually has issues, sorted by total savings desc
    case class StageRow(
      stageId:        Int,
      stageName:      String,
      wallClockSavMs: Long,
      computeSavMs:   Long,
      totalSavMs:     Long,
      issues:         Seq[PerformanceIssue]
    )

    val rows: Seq[StageRow] = byStage
      .collect { case (sid, issues) if sid >= 0 => (sid, issues) }
      .flatMap { case (sid, issues) =>
        parent.sparkUI.store.stageList(
          java.util.Arrays.asList(
            org.apache.spark.status.api.v1.StageStatus.COMPLETE,
            org.apache.spark.status.api.v1.StageStatus.ACTIVE,
            org.apache.spark.status.api.v1.StageStatus.FAILED
          )
        ).find(_.stageId == sid).map { stage =>
          val wallMs = issues.collect {
            case i if i.savingsType == "Wall-clock" => i.estimatedSavingsMs.getOrElse(0L)
          }.sum
          val compMs = issues.collect {
            case i if i.savingsType != "Wall-clock" && i.savingsType.nonEmpty =>
              i.estimatedSavingsMs.getOrElse(0L)
          }.sum
          StageRow(sid, stage.name, wallMs, compMs, wallMs + compMs, issues)
        }
      }
      .toSeq
      .sortBy(-_.totalSavMs)

    // Issues with no stageId (e.g. Broadcast)
    val globalIssues = byStage.getOrElse(-1, Seq.empty)

    val content =
      <div>
        <p>
          Every stage that sparkx has flagged, ranked by <strong>estimated total savings</strong>.
          Fix higher-ranked stages first for the biggest impact.
        </p>
        <p style="font-size:12px;color:#888">
          ⚡ <strong>Wall-clock savings</strong> directly reduce job completion time.
          🔄 <strong>Compute savings</strong> reduce CPU / I/O cost (may run in parallel).
        </p>

        {if (rows.isEmpty && globalIssues.isEmpty)
          <div class="alert alert-success">No performance issues detected in any stage.</div>
        }

        {if (rows.nonEmpty)
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th>
                <th>Job</th>
                <th>Stage Name</th>
                <th>Issues Found</th>
                <th>⚡ Wall-clock Savings</th>
                <th>🔄 Compute Savings</th>
                <th>Total Est. Savings</th>
                <th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {rows.map { row =>
                val issueBadges = row.issues.map { i =>
                  val cls = i.severity.toString match {
                    case "Critical" => "label-danger"
                    case "Warning"  => "label-warning"
                    case _          => "label-info"
                  }
                  <span class={s"label $cls"} style="margin-right:3px">
                    <a href={SparkXPageUtils.sparkxSubUrl(request, i.detailPath)}
                       style="color:white">{i.title}</a>
                  </span>
                }
                val rowClass = if (row.totalSavMs > 60000) "danger"
                              else if (row.totalSavMs > 5000) "warning"
                              else ""
                <tr class={rowClass}>
                  <td><a href={SparkXPageUtils.stageUrl(request, row.stageId)}>{row.stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, row.stageId, stageJobs)}
                  <td>{row.stageName}</td>
                  <td>{issueBadges}</td>
                  <td sorttable_customkey={row.wallClockSavMs.toString}>{if (row.wallClockSavMs > 0)
                        <strong>⚡ {Utils.formatDuration(row.wallClockSavMs)}</strong>
                      else <span style="color:#ccc">—</span>}</td>
                  <td sorttable_customkey={row.computeSavMs.toString}>{if (row.computeSavMs > 0)
                        <strong>🔄 {Utils.formatDuration(row.computeSavMs)}</strong>
                      else <span style="color:#ccc">—</span>}</td>
                  <td sorttable_customkey={row.totalSavMs.toString}><strong>{Utils.formatDuration(row.totalSavMs)}</strong></td>
                  {SparkXPageUtils.dagTd(request, row.stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }

        {if (globalIssues.nonEmpty)
          <div>
            <h4>Application-wide Issues</h4>
            <table class="table table-bordered table-condensed table-striped">
              <thead>
                <tr><th>Issue</th><th>Description</th></tr>
              </thead>
              <tbody>
                {globalIssues.map { i =>
                  <tr>
                    <td><a href={SparkXPageUtils.sparkxSubUrl(request, i.detailPath)}>{i.title}</a></td>
                    <td>{i.description}</td>
                  </tr>
                }}
              </tbody>
            </table>
          </div>
        }
      </div>

    UIUtils.headerSparkPage(request, "sparkx — Stages with Savings Potential",
      SparkXPageUtils.subNavBar(request, "stages") ++ content, parent)
  }
}
