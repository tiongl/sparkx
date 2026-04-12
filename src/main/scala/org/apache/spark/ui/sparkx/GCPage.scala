package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class GCPage(parent: SparkXTab) extends WebUIPage("gc") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val gcStages    = IssueDetector.gcStages(parent.sparkUI.store)
    val executors   = parent.sparkUI.store.executorList(activeOnly = false)
    val threshold   = parent.config.gcRatioThreshold
    val stageJobs    = IssueDetector.stageJobMap(parent.sparkUI.store)
    val stageSqlExec = IssueDetector.stageSqlExecMap(parent.sparkUI.store)

    val content =
      <div>
        <p>
          Stages and executors with <strong>GC time / executor run time</strong> above
          <strong>{f"${threshold * 100}%.0f"}%</strong> are flagged.
          High GC pressure typically indicates heap pressure — consider increasing executor memory.
        </p>

        <h4>Per-Stage GC Summary</h4>
        <p style="font-size:12px;color:#888">
          🔄 <strong>Est. Savings</strong> = total GC time for the stage.
          GC time is directly wasted executor CPU — every millisecond of GC is a millisecond tasks
          could have been computing instead.
        </p>
        {if (gcStages.isEmpty)
          <div class="alert alert-success">No GC pressure detected in any stage.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>GC Time</th><th>Executor Run Time</th>
                <th>GC Ratio</th><th>🔄 Est. Savings</th><th>Status</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {gcStages.map { case (stageId, name, gcMs, runMs, ratio) =>
                val isHigh = ratio >= threshold
                <tr class={if (isHigh) "warning" else ""}>
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td sorttable_customkey={gcMs.toString}>{Utils.formatDuration(gcMs)}</td>
                  <td sorttable_customkey={runMs.toString}>{Utils.formatDuration(runMs)}</td>
                  <td>{f"${ratio * 100}%.1f"}%</td>
                  <td sorttable_customkey={gcMs.toString}><strong>🔄 {Utils.formatDuration(gcMs)}</strong> <small style="color:#888">(compute)</small></td>
                  <td>{if (isHigh) <span class="label label-warning">High GC</span>
                       else <span class="label label-success">OK</span>}</td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }

        <h4>Per-Executor GC Summary</h4>
        <table class="table table-bordered table-condensed table-striped sortable">
          <thead>
            <tr>
              <th>Executor ID</th><th>Host</th><th>Status</th>
              <th>Total GC Time</th><th>Total Duration</th>
              <th>GC Ratio</th>
            </tr>
          </thead>
          <tbody>
            {executors.map { ex =>
              val ratio = if (ex.totalDuration > 0)
                ex.totalGCTime.toDouble / ex.totalDuration else 0.0
              val isHigh = ratio >= threshold
              <tr class={if (isHigh) "warning" else ""}>
                <td>{ex.id}</td>
                <td>{ex.hostPort}</td>
                <td>{if (ex.isActive) "Active" else "Dead"}</td>
                <td>{Utils.formatDuration(ex.totalGCTime)}</td>
                <td>{Utils.formatDuration(ex.totalDuration)}</td>
                <td>{f"${ratio * 100}%.1f"}%</td>
              </tr>
            }}
          </tbody>
        </table>
      </div>

    UIUtils.headerSparkPage(request, "sparkx — GC Pressure Analysis",
      SparkXPageUtils.subNavBar(request, "gc") ++ content, parent)
  }
}
