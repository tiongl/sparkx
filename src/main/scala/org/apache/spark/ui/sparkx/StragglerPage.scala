package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class StragglerPage(parent: SparkXTab) extends WebUIPage("stragglers") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val rows = IssueDetector.stragglerStages(parent.sparkUI.store, parent.config)
    val stageJobs    = IssueDetector.stageJobMap(parent.sparkUI.store)
    val stageSqlExec = IssueDetector.stageSqlExecMap(parent.sparkUI.store)

    val content =
      <div>
        <p>
          A task is a <strong>straggler</strong> when its duration exceeds
          <code>Q3 + {parent.config.stragglerIQRFactor} × IQR</code>.
          Stragglers slow down the entire stage. Common causes: data skew, GC, network issues.
        </p>
        <p style="font-size:12px;color:#888">
          ⚡ <strong>Est. Savings</strong> = max duration − IQR upper fence.
          Eliminating the straggler (e.g., via speculation tuning, skew fix, or node repair)
          allows the stage to finish at the fence threshold rather than waiting for the outlier.
        </p>
        {if (rows.isEmpty)
          <div class="alert alert-success">No straggler tasks detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th><th>Median</th>
                <th>P75</th><th>Max</th><th>IQR Threshold</th><th>⚡ Est. Savings</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {rows.map { case (stageId, name, med, q3, max, fence) =>
                val savings = (max - fence).toLong.max(0)
                <tr class="warning">
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td sorttable_customkey={med.toLong.toString}>{Utils.formatDuration(med.toLong)}</td>
                  <td sorttable_customkey={q3.toLong.toString}>{Utils.formatDuration(q3.toLong)}</td>
                  <td sorttable_customkey={max.toLong.toString}><strong>{Utils.formatDuration(max.toLong)}</strong></td>
                  <td sorttable_customkey={fence.toLong.toString}>{Utils.formatDuration(fence.toLong)}</td>
                  <td sorttable_customkey={savings.toString}><strong>⚡ {Utils.formatDuration(savings)}</strong> <small style="color:#888">(wall-clock)</small></td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }
      </div>

    UIUtils.headerSparkPage(request, "sparkx — Straggler Task Analysis",
      SparkXPageUtils.subNavBar(request, "stragglers") ++ content, parent)
  }
}
