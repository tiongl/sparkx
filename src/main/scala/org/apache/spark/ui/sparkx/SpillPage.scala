package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class SpillPage(parent: SparkXTab) extends WebUIPage("spill") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val spillStages = IssueDetector.spillStages(parent.sparkUI.store)
    val stageJobs    = IssueDetector.stageJobMap(parent.sparkUI.store)
    val stageSqlExec = IssueDetector.stageSqlExecMap(parent.sparkUI.store)

    val content =
      <div>
        <p>
          Stages with <strong>disk spill &gt; 0</strong> are listed below.
          Shuffle spill occurs when executor memory is insufficient to hold intermediate shuffle data.
          Consider increasing <code>spark.executor.memory</code> or reducing partition size.
        </p>
        <p style="font-size:12px;color:#888">
          🔄 <strong>Est. Savings</strong> = estimated disk I/O time eliminated (disk bytes × 2 ÷ 50 MB/s).
          Spill requires reading and writing to disk, doubling the I/O cost.
        </p>
        {if (spillStages.isEmpty)
          <div class="alert alert-success">No shuffle spill detected in any stage.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>Disk Spill</th><th>Memory Spill</th><th>🔄 Est. Savings</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {spillStages.map { case (stageId, name, disk, mem) =>
                val ioMs = disk * 2L / (50L * 1024 * 1024) * 1000
                <tr class="warning">
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td sorttable_customkey={disk.toString}><strong>{Utils.formatBytes(disk)}</strong></td>
                  <td sorttable_customkey={mem.toString}>{Utils.formatBytes(mem)}</td>
                  <td sorttable_customkey={ioMs.toString}><strong>🔄 {Utils.formatDuration(ioMs)}</strong> <small style="color:#888">(I/O)</small></td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }
      </div>

    UIUtils.headerSparkPage(request, "sparkx — Shuffle Spill Analysis", content, parent)
  }
}
