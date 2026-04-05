package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class SkewPage(parent: SparkXTab) extends WebUIPage("skew") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val rows = IssueDetector.skewStages(parent.sparkUI.store, parent.config)
      .sortBy(r => -(r._4 - r._3).toLong) // sort by est. savings (max − median) descending
    val stageJobs    = IssueDetector.stageJobMap(parent.sparkUI.store)
    val stageSqlExec = IssueDetector.stageSqlExecMap(parent.sparkUI.store)

    val content =
      <div>
        <p>
          Stages where <strong>max task duration / median task duration</strong> exceeds
          <strong>{parent.config.skewMultiplier}×</strong> are flagged as skewed.
          Data skew means some tasks receive significantly more data than others.
        </p>
        <p style="font-size:12px;color:#888">
          ⚡ <strong>Est. Savings</strong> = max − median task duration.
          Fixing skew (e.g., with salting or repartition) allows the stage to complete in ~median time,
          saving that difference as wall-clock time.
        </p>
        {if (rows.isEmpty)
          <div class="alert alert-success">No skewed stages detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>Median Duration</th><th>Max Duration</th>
                <th>Ratio (Max/Median)</th><th>⚡ Est. Savings</th><th>Status</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {rows.map { case (stageId, stageName, med, max, ratio) =>
                val isSkewed = ratio >= parent.config.skewMultiplier
                val savings  = (max - med).toLong.max(0)
                <tr class={if (isSkewed) "danger" else ""}>
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{stageName}</td>
                  <td sorttable_customkey={med.toLong.toString}>{Utils.formatDuration(med.toLong)}</td>
                  <td sorttable_customkey={max.toLong.toString}>{Utils.formatDuration(max.toLong)}</td>
                  <td>{f"$ratio%.2f"}×</td>
                  <td sorttable_customkey={savings.toString}><strong>⚡ {Utils.formatDuration(savings)}</strong> <small style="color:#888">(wall-clock)</small></td>
                  <td>{if (isSkewed) <span class="label label-danger">Skewed</span>
                       else <span class="label label-success">OK</span>}</td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }
      </div>

    UIUtils.headerSparkPage(request, "sparkx — Data Skew Analysis", content, parent)
  }
}
