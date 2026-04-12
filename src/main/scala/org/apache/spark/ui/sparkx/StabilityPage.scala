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
    val retries     = IssueDetector.stageRetries(parent.sparkUI.store)
    val slowSer     = IssueDetector.slowResultSerStages(parent.sparkUI.store, parent.config)
    val memDist     = IssueDetector.executorMemoryDistribution(parent.sparkUI.store)
    val stageJobs    = IssueDetector.stageJobMap(parent.sparkUI.store)
    val stageSqlExec = IssueDetector.stageSqlExecMap(parent.sparkUI.store)

    val content =
      <div>
        <p>
          Task-level health indicators: failures waste compute resources, speculative tasks
          signal severe stragglers, stage retries re-run entire stages, large results risk
          driver OOM, high fetch wait indicates network bottlenecks, high deserialization
          points to oversized task payloads, and slow serialization wastes executor CPU.
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

        <h4>Stage Retries</h4>
        <p>
          Stages that were retried due to executor loss, fetch failures, or other errors.
          Each retry re-runs all tasks in the stage, wasting significant compute.
        </p>
        <p style="font-size:12px;color:#888">
          🔄 <strong>Est. Savings</strong> = retries × tasks × average task duration (wasted compute on re-execution).
        </p>
        {if (retries.isEmpty)
          <div class="alert alert-success">No stage retries detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>Retries</th><th>Tasks/Attempt</th><th>Avg Task Duration</th><th>🔄 Est. Savings</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {retries.map { case (stageId, name, numRetries, avgMs, numTasks) =>
                val savings = numRetries.toLong * numTasks * avgMs
                <tr class="danger">
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td><strong>{numRetries}</strong></td>
                  <td>{numTasks}</td>
                  <td sorttable_customkey={avgMs.toString}>{Utils.formatDuration(avgMs)}</td>
                  <td sorttable_customkey={savings.toString}>{if (savings > 0) <span><strong>🔄 {Utils.formatDuration(savings)}</strong> <small style="color:#888">(compute)</small></span>
                      else <span style="color:#ccc">—</span>}</td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }

        <h4>Slow Result Serialization</h4>
        <p>
          Stages where the P95 result serialization time exceeds
          <strong>{parent.config.resultSerializationMs} ms</strong>.
          Tasks are spending significant time serializing results back to the driver.
          Consider reducing result size or using Kryo serialization.
        </p>
        <p style="font-size:12px;color:#888">
          🔄 <strong>Est. Savings</strong> = P95 serialization time × number of tasks (total compute wasted).
        </p>
        {if (slowSer.isEmpty)
          <div class="alert alert-success">No slow result serialization detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Job</th><th>Stage Name</th>
                <th>P95 Serialization Time</th><th>Tasks</th><th>🔄 Est. Savings</th><th>DAG</th>
              </tr>
            </thead>
            <tbody>
              {slowSer.map { case (stageId, name, serMs, numTasks) =>
                val savings = serMs * numTasks
                <tr class="warning">
                  <td><a href={SparkXPageUtils.stageUrl(request, stageId)}>{stageId}</a></td>
                  {SparkXPageUtils.jobTd(request, stageId, stageJobs)}
                  <td>{name}</td>
                  <td sorttable_customkey={serMs.toString}><strong>{Utils.formatDuration(serMs)}</strong></td>
                  <td>{numTasks}</td>
                  <td sorttable_customkey={savings.toString}><strong>🔄 {Utils.formatDuration(savings)}</strong> <small style="color:#888">(compute)</small></td>
                  {SparkXPageUtils.dagTd(request, stageId, stageSqlExec)}
                </tr>
              }}
            </tbody>
          </table>
        }

        <h4>Executor Memory Skew</h4>
        <p>
          Peak JVM heap memory usage across active executors. Uneven memory consumption
          suggests partition-level data imbalance. Large differences may cause OOM on
          heavily loaded executors while others have spare capacity.
        </p>
        {if (memDist.isEmpty)
          <div class="alert alert-success">No executor memory data available.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Executor ID</th><th>Peak JVM Heap (MB)</th><th>Max Memory (MB)</th><th>Usage Ratio</th>
              </tr>
            </thead>
            <tbody>
              {memDist.map { case (execId, peakMB, maxMB) =>
                val ratio = if (maxMB > 0) peakMB.toDouble / maxMB else 0.0
                val cls = if (ratio >= 0.9) "danger" else if (ratio >= 0.7) "warning" else ""
                <tr class={cls}>
                  <td>{execId}</td>
                  <td sorttable_customkey={peakMB.toString}><strong>{peakMB} MB</strong></td>
                  <td>{maxMB} MB</td>
                  <td><span class={s"label ${if (ratio >= 0.9) "label-danger" else if (ratio >= 0.7) "label-warning" else "label-success"}"}>{f"${ratio * 100}%.0f"}%</span></td>
                </tr>
              }}
            </tbody>
          </table>
        }
      </div>

    UIUtils.headerSparkPage(request, "sparkx — Task Stability Analysis",
      SparkXPageUtils.subNavBar(request, "stability") ++ content, parent)
  }
}
