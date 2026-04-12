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

        <h4>High Scheduler Delay</h4>
        <p>
          Stages where the P95 scheduler delay exceeds
          <strong>{parent.config.highSchedulerDelayMs} ms</strong> and is more than
          <strong>{f"${parent.config.schedulerDelayRatio * 100}%.0f"}%</strong> of median task run time.
          Tasks are waiting a long time to be assigned to an executor, indicating cluster
          resource contention or insufficient executors.
        </p>
        <p style="font-size:12px;color:#888">
          🔄 <strong>Est. Savings</strong> = P95 scheduler delay × number of tasks (total scheduling overhead).
        </p>
        {val schedDelayStages = IssueDetector.highSchedulerDelayStages(parent.sparkUI.store, parent.config)
        if (schedDelayStages.isEmpty)
          <div class="alert alert-success">No high scheduler delays detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>P95 Sched Delay</th><th>P50 Run Time</th><th>Delay / Run</th>
                <th>Tasks</th><th>🔄 Est. Savings</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {schedDelayStages.map { case (stageId, name, delayMs, runMs, numTasks) =>
                val ratio   = if (runMs > 0) delayMs.toDouble / runMs * 100 else 0.0
                val savings = delayMs * numTasks
                <tr class="warning">
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td sorttable_customkey={delayMs.toString}><strong>{Utils.formatDuration(delayMs)}</strong></td>
                  <td sorttable_customkey={runMs.toString}>{Utils.formatDuration(runMs)}</td>
                  <td><span class="label label-warning">{f"$ratio%.0f"}%</span></td>
                  <td>{numTasks}</td>
                  <td sorttable_customkey={savings.toString}><strong>🔄 {Utils.formatDuration(savings)}</strong> <small style="color:#888">(scheduling)</small></td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }

        <h4>Low CPU Utilization</h4>
        <p>
          Stages where executor CPU time is less than
          <strong>{f"${parent.config.lowCpuRatioThreshold * 100}%.0f"}%</strong> of executor run time
          (minimum <strong>{Utils.formatDuration(parent.config.lowCpuMinRunTimeMs)}</strong> run time).
          Low CPU usage means tasks are spending time on I/O waits, lock contention, sleeping,
          or waiting on external systems rather than computing.
        </p>
        <p style="font-size:12px;color:#888">
          🔄 <strong>Est. Savings</strong> = run time − CPU time (total idle compute time).
        </p>
        {val cpuStages = IssueDetector.lowCpuStages(parent.sparkUI.store, parent.config)
          .filter(_._3 < parent.config.lowCpuRatioThreshold)
        if (cpuStages.isEmpty)
          <div class="alert alert-success">No low CPU utilization stages detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>CPU Time</th><th>Run Time</th><th>CPU Ratio</th>
                <th>🔄 Est. Savings</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {cpuStages.map { case (stageId, name, ratio, cpuMs, runMs) =>
                val cls     = if (ratio < 0.25) "danger" else "warning"
                val savings = (runMs - cpuMs).max(0)
                <tr class={cls}>
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td sorttable_customkey={cpuMs.toString}>{Utils.formatDuration(cpuMs)}</td>
                  <td sorttable_customkey={runMs.toString}>{Utils.formatDuration(runMs)}</td>
                  <td><span class={s"label label-$cls"}>{f"${ratio * 100}%.1f"}%</span></td>
                  <td sorttable_customkey={savings.toString}><strong>🔄 {Utils.formatDuration(savings)}</strong> <small style="color:#888">(compute)</small></td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }
      </div>

    UIUtils.headerSparkPage(request, "sparkx — Partitioning Analysis",
      SparkXPageUtils.subNavBar(request, "partitioning") ++ content, parent)
  }
}
