package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class PartitioningPage(parent: SparkXTab) extends WebUIPage("partitioning") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val smallTasks     = IssueDetector.smallTaskStages(parent.sparkUI.store, parent.config)
    val underPart      = IssueDetector.underPartitionedStages(parent.sparkUI.store, parent.config)
    val shuffleAmpStgs = IssueDetector.shuffleAmplifiedStages(parent.sparkUI.store, parent.config)
    val stageJobs      = IssueDetector.stageJobMap(parent.sparkUI.store)
    val stageSqlExec   = IssueDetector.stageSqlExecMap(parent.sparkUI.store)

    val content =
      <div>
        <p>
          Partition count directly impacts parallelism, scheduling overhead, and memory pressure.
          Too many tiny tasks waste CPU on scheduling; too few leave cores idle; amplified
          shuffles signal missing filters or problematic join strategies.
        </p>

        <h4>Over-partitioned Stages (Small Tasks)</h4>
        <p>
          Stages with ≥ <strong>{parent.config.smallTaskMinCount}</strong> tasks whose
          median task duration is below <strong>{parent.config.smallTaskMedianMs} ms</strong>.
          Scheduling overhead likely exceeds useful work.
          Fix: <code>coalesce()</code>, increase <code>spark.sql.files.maxPartitionBytes</code>,
          or raise <code>spark.default.parallelism</code>.
        </p>
        <p style="font-size:12px;color:#888">
          🔄 <strong>Est. Savings</strong> = median scheduler delay × number of tasks (total scheduling overhead).
        </p>
        {if (smallTasks.isEmpty)
          <div class="alert alert-success">No over-partitioned stages detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>Num Tasks</th><th>Median Task Duration</th><th>🔄 Est. Savings</th><th>Status</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {smallTasks.map { case (stageId, name, numTasks, medMs, totalSchedMs) =>
                val flagged = medMs < parent.config.smallTaskMedianMs
                <tr class={if (flagged) "warning" else ""}>
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td>{numTasks}</td>
                  <td sorttable_customkey={medMs.toLong.toString}>{Utils.formatDuration(medMs.toLong)}</td>
                  <td sorttable_customkey={totalSchedMs.toString}>{if (totalSchedMs > 0)
                        <span><strong>🔄 {Utils.formatDuration(totalSchedMs)}</strong> <small style="color:#888">(scheduling)</small></span>
                      else <span style="color:#ccc">—</span>}</td>
                  <td>{if (flagged) <span class="label label-warning">Over-partitioned</span>
                       else <span class="label label-success">OK</span>}</td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }

        <h4>Under-partitioned Stages (Too Few Tasks)</h4>
        <p>
          Stages where task count is less than
          <strong>{(parent.config.underPartitionRatio * 100).toInt}%</strong> of available executor
          cores — most cores will sit idle for the duration of this stage.
          Fix: <code>repartition(N)</code> or tune <code>spark.sql.shuffle.partitions</code>.
        </p>
        {if (underPart.isEmpty)
          <div class="alert alert-success">No under-partitioned stages detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>Num Tasks</th><th>Available Cores</th><th>Utilization</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {underPart.map { case (stageId, name, numTasks, cores) =>
                val util = if (cores > 0) numTasks.toDouble / cores else 0.0
                <tr class="warning">
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td>{numTasks}</td>
                  <td>{cores}</td>
                  <td><span class="label label-warning">{f"${util * 100}%.0f"}% core utilization</span></td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }

        <h4>Shuffle Amplification</h4>
        <p>
          Stages where shuffle write bytes exceed input bytes by more than
          <strong>{f"${parent.config.shuffleAmplifyRatio}%.0f"}×</strong>.
          High amplification suggests a cartesian join, <code>explode()</code> on large arrays,
          or missing upstream filters.
        </p>
        {if (shuffleAmpStgs.isEmpty)
          <div class="alert alert-success">No shuffle amplification detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>Input (MB)</th><th>Shuffle Write (MB)</th>
                <th>Amplification</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {shuffleAmpStgs.map { case (stageId, name, inputMB, writeMB, ratio) =>
                val cls = if (ratio >= 20.0) "danger" else "warning"
                <tr class={cls}>
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td>{inputMB}</td>
                  <td><strong>{writeMB}</strong></td>
                  <td><span class={s"label label-$cls"}>{f"$ratio%.1f"}×</span></td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }
      </div>

    UIUtils.headerSparkPage(request, "sparkx — Partitioning Analysis", content, parent)
  }
}
