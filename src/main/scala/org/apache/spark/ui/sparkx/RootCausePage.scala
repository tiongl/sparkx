package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import com.sparkx.analysis.{Critical, RootCauseAnalyzer, Warning}
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class RootCausePage(parent: SparkXTab) extends WebUIPage("rootcause") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val allIssues  = IssueDetector.detect(parent.sparkUI.store, parent.config)
    val result     = RootCauseAnalyzer.analyze(allIssues)
    val stageJobs  = IssueDetector.stageJobMap(parent.sparkUI.store)
    val stageSqlExec = IssueDetector.stageSqlExecMap(parent.sparkUI.store)

    val content =
      <div>
        <p>
          Issues that frequently co-occur on the same stage are grouped into
          <strong>root causes</strong>. Fixing the root cause typically resolves all
          associated symptoms at once. Issues that don't match any known pattern are
          listed separately below.
        </p>

        {if (result.rootCauses.isEmpty && result.unclusteredIssues.isEmpty)
          <div class="alert alert-success">
            <strong>No performance issues detected.</strong> Your Spark job looks healthy.
          </div>
        else <div>
          {if (result.rootCauses.nonEmpty)
            <div>
              <h4>Root Causes ({result.rootCauses.length} identified)</h4>
              {result.rootCauses.zipWithIndex.map { case (rc, idx) =>
                val hasSavings = rc.wallClockSavMs > 0 || rc.computeSavMs > 0
                val maxSeverity = rc.issues.map(_.severity).collectFirst { case Critical => Critical }
                  .getOrElse(Warning)
                val borderColor = maxSeverity match {
                  case Critical => "#d9534f"
                  case _ => "#f0ad4e"
                }
                <div style={s"border-left:4px solid $borderColor;padding:12px 16px;margin-bottom:16px;background:#fafafa"}>
                  <h5 style="margin-top:0">
                    <span class={s"label ${if (maxSeverity == Critical) "label-danger" else "label-warning"}"}>
                      {maxSeverity.toString}
                    </span>
                    {" "}{rc.name}
                    {if (rc.affectedStages.nonEmpty)
                      <small style="color:#888"> — Stage(s): {rc.affectedStages.toSeq.sorted.mkString(", ")}</small>
                    }
                  </h5>

                  {if (hasSavings)
                    <div style="margin-bottom:8px">
                      {if (rc.wallClockSavMs > 0)
                        <span>⚡ Up to <strong>{Utils.formatDuration(rc.wallClockSavMs)}</strong> wall-clock savings</span>}
                      {if (rc.wallClockSavMs > 0 && rc.computeSavMs > 0) <span> · </span>}
                      {if (rc.computeSavMs > 0)
                        <span>🔄 Up to <strong>{Utils.formatDuration(rc.computeSavMs)}</strong> compute savings</span>}
                    </div>
                  }

                  <div style="margin-bottom:8px">
                    <strong>Contributing symptoms:</strong>
                    <ul style="margin-top:4px;margin-bottom:4px">
                      {rc.issues.map { issue =>
                        <li>
                          <a href={SparkXPageUtils.sparkxSubUrl(request, issue.detailPath)}>
                            {issue.title}
                          </a>
                          {issue.stageId.map(id =>
                            <span> (Stage <a href={SparkXPageUtils.stageUrl(request, id)}>{id}</a>)</span>
                          ).getOrElse(scala.xml.NodeSeq.Empty)}
                          {" — "}{issue.description}
                        </li>
                      }}
                    </ul>
                  </div>

                  <div style="background:#fff;padding:8px 12px;border:1px solid #ddd;border-radius:4px">
                    <strong>💡 Recommendation:</strong> {rc.recommendation}
                  </div>
                </div>
              }}
            </div>
          }

          {if (result.unclusteredIssues.nonEmpty)
            <div>
              <h4>Other Issues ({result.unclusteredIssues.length})</h4>
              <p style="font-size:12px;color:#888">
                These issues don't co-occur with known symptom patterns. They may still
                warrant investigation individually.
              </p>
              <table class="table table-bordered table-condensed table-striped sortable">
                <thead>
                  <tr>
                    <th>Severity</th><th>Stage</th><th>Job</th><th>Issue</th>
                    <th>Description</th><th>Est. Savings</th><th>Detail</th>
                  </tr>
                </thead>
                <tbody>
                  {result.unclusteredIssues.sortBy(i => -i.estimatedSavingsMs.getOrElse(0L)).map { issue =>
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
                      <td><strong>{issue.title}</strong></td>
                      <td>{issue.description}</td>
                      <td sorttable_customkey={issue.estimatedSavingsMs.getOrElse(0L).toString}>{savingsCell}</td>
                      <td><a href={SparkXPageUtils.sparkxSubUrl(request, issue.detailPath)}>View →</a></td>
                    </tr>
                  }}
                </tbody>
              </table>
            </div>
          }
        </div>
        }
      </div>

    UIUtils.headerSparkPage(request, "sparkx — Root Cause Analysis",
      SparkXPageUtils.subNavBar(request, "rootcause") ++ content, parent)
  }
}
