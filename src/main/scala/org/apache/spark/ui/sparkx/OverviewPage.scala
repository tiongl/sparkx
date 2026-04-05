package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import com.sparkx.analysis.{Critical, Warning}
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class OverviewPage(parent: SparkXTab) extends WebUIPage("") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val issues = IssueDetector.detect(parent.sparkUI.store, parent.config)
      .sortBy(i => -i.estimatedSavingsMs.getOrElse(0L))
    val stageJobs    = IssueDetector.stageJobMap(parent.sparkUI.store)
    val stageSqlExec = IssueDetector.stageSqlExecMap(parent.sparkUI.store)

    val content = if (issues.isEmpty) {
      <div class="alert alert-success">
        <strong>No performance issues detected.</strong> Your Spark job looks healthy.
      </div>
    } else {
      // Separate wall-clock and compute savings for the banner
      val wallClockSavingsMs = issues.collect {
        case i if i.savingsType == "Wall-clock" => i.estimatedSavingsMs.getOrElse(0L)
      }.sum
      val computeSavingsMs = issues.collect {
        case i if i.savingsType != "Wall-clock" && i.savingsType.nonEmpty =>
          i.estimatedSavingsMs.getOrElse(0L)
      }.sum
      val hasSavings = wallClockSavingsMs > 0 || computeSavingsMs > 0

      <div>
        {if (hasSavings)
          <div class="alert alert-warning" style="margin-bottom:16px">
            <strong>💡 Estimated potential savings if all issues are fixed:</strong>
            <span style="margin-left:12px">
              {if (wallClockSavingsMs > 0)
                <span>⚡ <strong>{Utils.formatDuration(wallClockSavingsMs)}</strong> faster job wall-clock time</span>}
              {if (wallClockSavingsMs > 0 && computeSavingsMs > 0) <span>&nbsp;·&nbsp;</span>}
              {if (computeSavingsMs > 0)
                <span>🔄 <strong>{Utils.formatDuration(computeSavingsMs)}</strong> compute/I/O time saved</span>}
              <span style="margin-left:16px">
                <a href={SparkXPageUtils.sparkxSubUrl(request, "stages")}>View by stage →</a>
              </span>
            </span>
            <div style="font-size:11px;margin-top:4px;color:#888">
              Wall-clock savings reduce job completion time directly.
              Compute savings reduce CPU/I/O cost (amortized across all cores).
              Estimates are conservative lower bounds.
            </div>
          </div>
        }
        <p>Found <strong>{issues.length}</strong> performance issue(s).
          Use the sub-tabs above for detailed analysis.</p>
        <table class="table table-bordered table-condensed table-striped sortable">
          <thead>
            <tr>
              <th>Severity</th><th>Stage</th><th>Job</th><th>Issue</th>
              <th>Description</th><th>Est. Savings</th><th>DAG</th>
            </tr>
          </thead>
          <tbody>
            {issues.map { issue =>
              val labelClass = issue.severity match {
                case Critical => "label-danger"
                case Warning  => "label-warning"
                case _        => "label-info"
              }
              val savingsCell = issue.estimatedSavingsMs match {
                case Some(ms) if ms > 0 =>
                  val icon = if (issue.savingsType == "Wall-clock") "⚡" else "🔄"
                  <span title={s"${issue.savingsType} savings"}>
                    {icon} {Utils.formatDuration(ms)}
                    <small style="color:#888"> ({issue.savingsType})</small>
                  </span>
                case _ => <span style="color:#ccc">—</span>
              }
              <tr>
                <td><span class={s"label $labelClass"}>{issue.severity.toString}</span></td>
                <td>{issue.stageId match {
                  case Some(id) => <a href={SparkXPageUtils.stageUrl(request, id)}>{id}</a>
                  case None     => scala.xml.Text("—")
                }}</td>
                {SparkXPageUtils.jobTd(request, issue.stageId, stageJobs)}
                <td><a href={SparkXPageUtils.sparkxSubUrl(request, issue.detailPath)}><strong>{issue.title}</strong></a></td>
                <td>{issue.description}</td>
                <td sorttable_customkey={issue.estimatedSavingsMs.getOrElse(0L).toString}>{savingsCell}</td>
                {SparkXPageUtils.dagTd(request, issue.stageId, stageSqlExec)}
              </tr>
            }}
          </tbody>
        </table>
      </div>
    }

    UIUtils.headerSparkPage(request, "sparkx — Performance Analysis", content, parent)
  }
}
