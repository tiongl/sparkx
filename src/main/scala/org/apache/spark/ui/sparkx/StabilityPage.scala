package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class StabilityPage(parent: SparkXTab) extends WebUIPage("stability") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val failures    = IssueDetector.failedTaskStages(parent.sparkUI.store)
    val speculative = IssueDetector.speculativeStages(parent.sparkUI.store)
    val largeResult = IssueDetector.largeResultStages(parent.sparkUI.store, parent.config)
    val fetchWait   = IssueDetector.highFetchWaitStages(parent.sparkUI.store, parent.config)
    val highDeser   = IssueDetector.highDeserStages(parent.sparkUI.store, parent.config)
    val stageJobs    = IssueDetector.stageJobMap(parent.sparkUI.store)
    val stageSqlExec = IssueDetector.stageSqlExecMap(parent.sparkUI.store)

    val content =
      <div>
        <p>
          Task-level health indicators: failures waste compute resources, speculative tasks
          signal severe stragglers, large results risk driver OOM, high fetch wait indicates
          network bottlenecks, and high deserialization points to oversized task payloads.
        </p>

        <h4>Task Failures</h4>
        <p>
          Stages with at least one failed task attempt. Repeated failures cause retries
          that inflate wall-clock time. Check the stage detail page for the failure reason.
        </p>
        <p style="font-size:12px;color:#888">
          🔄 <strong>Est. Savings</strong> = failed tasks × average task duration (wasted compute on retries).
        </p>
        {if (failures.isEmpty)
          <div class="alert alert-success">No task failures detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>Failed Tasks</th><th>Total Tasks</th><th>Failure Rate</th><th>🔄 Est. Savings</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {failures.map { case (stageId, name, numFailed, numTotal, avgMs) =>
                val rate    = if (numTotal > 0) numFailed.toDouble / numTotal * 100 else 0.0
                val savings = numFailed.toLong * avgMs
                <tr class="danger">
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td><strong>{numFailed}</strong></td>
                  <td>{numTotal}</td>
                  <td><span class="label label-danger">{f"$rate%.1f"}%</span></td>
                  <td sorttable_customkey={savings.toString}>{if (savings > 0) <span><strong>🔄 {Utils.formatDuration(savings)}</strong> <small style="color:#888">(compute)</small></span>
                      else <span style="color:#ccc">—</span>}</td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }

        <h4>Speculative Tasks</h4>
        <p>
          Stages where Spark launched duplicate speculative tasks because some tasks
          ran significantly slower than peers. This is a symptom of skew, GC, or node
          degradation — check the Skew and GC pages for the same stages.
        </p>
        <p style="font-size:12px;color:#888">
          🔄 <strong>Est. Savings</strong> = speculative tasks × average task duration (wasted compute on duplicates).
        </p>
        {if (speculative.isEmpty)
          <div class="alert alert-success">No speculative tasks detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>Speculative Tasks</th><th>Total Tasks</th><th>🔄 Est. Savings</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {speculative.map { case (stageId, name, numSpec, numTotal, avgMs) =>
                val savings = numSpec.toLong * avgMs
                <tr class="warning">
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td><strong>{numSpec}</strong></td>
                  <td>{numTotal}</td>
                  <td sorttable_customkey={savings.toString}>{if (savings > 0) <span><strong>🔄 {Utils.formatDuration(savings)}</strong> <small style="color:#888">(compute)</small></span>
                      else <span style="color:#ccc">—</span>}</td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }

        <h4>Large Task Results (Driver Memory Risk)</h4>
        <p>
          Stages where the P95 task result returned to the driver exceeds
          <strong>{parent.config.largeResultMB} MB</strong>.
          Large results accumulate in driver heap and risk OOM. Replace
          <code>collect()</code> with <code>write()</code> or <code>show()</code>.
        </p>
        {if (largeResult.isEmpty)
          <div class="alert alert-success">No large result sizes detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th><th>P95 Result Size</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {largeResult.map { case (stageId, name, sizeMB) =>
                <tr class="warning">
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td><strong>{sizeMB} MB</strong></td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }

        <h4>High Shuffle Fetch Wait (Network Bottleneck)</h4>
        <p>
          Stages where the median shuffle read fetch wait exceeds
          <strong>{f"${parent.config.fetchWaitRatioThreshold * 100}%.0f"}%</strong> of
          median task run time. The executor is blocked waiting for shuffle blocks from
          other nodes. Causes: slow network, overloaded shuffle service, or many small blocks.
        </p>
        <p style="font-size:12px;color:#888">
          ⚡ <strong>Est. Savings</strong> = median fetch wait time per task wave (wall-clock blocked time).
        </p>
        {if (fetchWait.isEmpty)
          <div class="alert alert-success">No high fetch wait detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>Fetch Wait (P50)</th><th>Task Run Time (P50)</th><th>Wait Ratio</th><th>⚡ Est. Savings</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {fetchWait.map { case (stageId, name, ratio, fetchMs, runMs) =>
                <tr class="warning">
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td sorttable_customkey={fetchMs.toString}>{Utils.formatDuration(fetchMs)}</td>
                  <td sorttable_customkey={runMs.toString}>{Utils.formatDuration(runMs)}</td>
                  <td><span class="label label-warning">{f"${ratio * 100}%.1f"}%</span></td>
                  <td sorttable_customkey={fetchMs.toString}><strong>⚡ {Utils.formatDuration(fetchMs)}</strong> <small style="color:#888">(wall-clock)</small></td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }

        <h4>High Task Deserialization</h4>
        <p>
          Stages where the P95 task deserialization time exceeds
          <strong>{parent.config.highDeserMs} ms</strong>.
          Long deserialization means tasks carry large closures or are deserializing big
          broadcast variables. Reduce closure size and avoid capturing large driver-side
          objects in lambdas.
        </p>
        <p style="font-size:12px;color:#888">
          🔄 <strong>Est. Savings</strong> = P95 deser time × number of tasks (total compute wasted on deserializing).
        </p>
        {if (highDeser.isEmpty)
          <div class="alert alert-success">No high deserialization times detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th><th>P95 Deserialization Time</th><th>Tasks</th><th>🔄 Est. Savings</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {highDeser.map { case (stageId, name, deserMs, numTasks) =>
                val savings = deserMs * numTasks
                <tr class="warning">
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td sorttable_customkey={deserMs.toString}><strong>{Utils.formatDuration(deserMs)}</strong></td>
                  <td>{numTasks}</td>
                  <td sorttable_customkey={savings.toString}><strong>🔄 {Utils.formatDuration(savings)}</strong> <small style="color:#888">(compute)</small></td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }
      </div>

    UIUtils.headerSparkPage(request, "sparkx — Task Stability Analysis", content, parent)
  }
}
