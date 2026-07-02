package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import com.sparkx.analysis._
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

/**
 * Renders Spark configuration tuning recommendations derived from detected performance symptoms
 * (currently GC pressure and shuffle spill). These are `SparkConf`-level knobs that cannot be
 * expressed as per-query plan hints, so they live here rather than in the Auto-Fix / Suggestions
 * pages. All recommendations are advisory — sparkx never changes the running configuration.
 */
class ConfigPage(parent: SparkXTab) extends WebUIPage("config") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val recs = ConfigAdvisor.advise(parent.sparkUI.store, parent.config)

    val content =
      if (recs.isEmpty)
        <div class="alert alert-success">
          <strong>No configuration recommendations.</strong> No configuration-tunable
          symptoms (GC pressure, spill, stragglers, fetch wait, disk shuffle read,
          slow serialization, task failures) were detected, so no `SparkConf` tuning is advised.
        </div>
      else
        <div>
          <p>
            Found <strong>{recs.size}</strong> configuration recommendation(s), derived from detected
            performance symptoms (GC pressure, spill, stragglers, fetch wait, and more). These are
            session/executor-level
            <code>SparkConf</code> knobs — set them at submit time (they cannot be applied as per-query
            hints). sparkx never changes your configuration; this is advisory only.
          </p>
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Severity</th>
                <th>Config Key</th>
                <th>Current</th>
                <th>Recommended</th>
                <th>Why</th>
                <th>Triggered By</th>
                <th>🔄 Est. Savings</th>
              </tr>
            </thead>
            <tbody>
              {recs.map { r =>
                val labelClass = r.severity match {
                  case Critical => "label-danger"
                  case Warning  => "label-warning"
                  case _        => "label-info"
                }
                val savings = r.estimatedSavingsMs.filter(_ > 0)
                  .map(ms => Utils.formatDuration(ms)).getOrElse("—")
                val savingsKey = r.estimatedSavingsMs.getOrElse(0L).toString
                <tr>
                  <td><span class={s"label $labelClass"}>{r.severity.toString}</span></td>
                  <td><code>{r.configKey}</code>
                    <div style="font-size:12px;color:#555">{r.title}</div></td>
                  <td><code>{r.currentValue}</code></td>
                  <td><code>{r.recommendedValue}</code></td>
                  <td style="font-size:12px">{r.rationale}</td>
                  <td>{r.relatedIssue}</td>
                  <td sorttable_customkey={savingsKey}>{savings}</td>
                </tr>
              }}
            </tbody>
          </table>
          <p style="font-size:12px;color:#888">
            🔄 <strong>Est. Savings</strong> reflects the wasted time of the symptom that triggered the
            recommendation (GC time, or estimated spill I/O) — an upper bound on what the tuning can recover.
          </p>
        </div>

    UIUtils.headerSparkPage(request, "SparkX — Configuration Recommendations",
      SparkXPageUtils.subNavBar(request, "config") ++ content, parent)
  }
}
