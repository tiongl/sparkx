package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import com.sparkx.autofix.{FixProfile, FixProfileStore, Hint}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

/**
 * Renders the learned SparkSQL auto-fix profiles: for each query fingerprint, its status,
 * baseline vs best timing, estimated savings, the hints being applied, and a before/after
 * SQL preview.
 */
class AutoFixPage(parent: SparkXTab) extends WebUIPage("autofix") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val config = parent.config

    val content =
      if (!config.autofixEnabled) disabledNotice
      else {
        val profiles = loadProfiles()
        if (profiles.isEmpty) emptyNotice
        else summaryTable(profiles) ++ profiles.flatMap(profileDetail)
      }

    UIUtils.headerSparkPage(request, "SparkX — SQL Auto-Fix",
      SparkXPageUtils.subNavBar(request, "autofix") ++ content, parent)
  }

  private def loadProfiles(): Seq[FixProfile] =
    try {
      val hconf = try SparkHadoopUtil.get.conf
                  catch { case _: Throwable => new org.apache.hadoop.conf.Configuration() }
      new FixProfileStore(parent.config.autofixStorePath, hconf).list()
    } catch { case _: Throwable => Seq.empty }

  private def savingsPct(p: FixProfile): Option[Int] =
    for { b <- p.baselineMs; best <- p.bestMs if b > 0 && best <= b }
      yield math.round((b - best).toDouble / b * 100).toInt

  private def statusBadge(status: String): Node = {
    val cls = status match {
      case FixProfile.Converged  => "label-success"
      case FixProfile.Optimizing => "label-warning"
      case _                     => "label-info"
    }
    <span class={s"label $cls"}>{status}</span>
  }

  private def summaryTable(profiles: Seq[FixProfile]): Seq[Node] =
    <div>
      <p>Auto-fix has learned <strong>{profiles.size}</strong> query profile(s).
        Store: <code>{parent.config.autofixStorePath}</code></p>
      <table class="table table-bordered table-condensed table-striped sortable">
        <thead>
          <tr>
            <th>Fingerprint</th><th>Status</th><th>Iterations</th>
            <th>Baseline</th><th>Best</th><th>Savings</th><th>Hints Applied</th>
          </tr>
        </thead>
        <tbody>
          {profiles.map { p =>
            val hints = if (p.bestHints.nonEmpty) p.bestHints else p.pendingHints
            <tr>
              <td><code>{p.fingerprint.take(12)}</code></td>
              <td>{statusBadge(p.status)}</td>
              <td>{p.iterations}</td>
              <td>{p.baselineMs.map(Utils.formatDuration).getOrElse("—")}</td>
              <td>{p.bestMs.map(Utils.formatDuration).getOrElse("—")}</td>
              <td>{savingsPct(p).map(pc => s"$pc%").getOrElse("—")}</td>
              <td>{if (hints.isEmpty) "none" else hints.map(_.render).mkString(", ")}</td>
            </tr>
          }}
        </tbody>
      </table>
    </div> :: Nil

  private def profileDetail(p: FixProfile): Seq[Node] = {
    val applied  = if (p.bestHints.nonEmpty) p.bestHints else Nil
    val proposed = p.pendingHints
    val shadow   = parent.config.autofixShadowEnabled
    <div style="margin-bottom:24px">
      <h5>Query <code>{p.fingerprint.take(12)}</code> — {statusBadge(p.status)}{
        if (shadow) <span class="label label-default" style="margin-left:6px">shadow (dry-run)</span> else Nil
      }</h5>
      {hintBlock("Applied hints (best so far)", applied)}
      {hintBlock(if (shadow) "Proposed hints (not applied — shadow mode)" else "Pending hints (next run)", proposed)}
      <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:8px">
        <div style="flex:1;min-width:320px">
          <strong>Baseline plan (un-hinted)</strong>
          <pre style="white-space:pre;overflow:auto;max-height:320px">{p.baselinePlan.getOrElse("—")}</pre>
        </div>
        <div style="flex:1;min-width:320px">
          <strong>Current plan (latest run)</strong>
          <pre style="white-space:pre;overflow:auto;max-height:320px">{p.currentPlan.getOrElse("—")}</pre>
        </div>
      </div>
    </div> :: Nil
  }

  /** Render a hint set as both its SparkSQL form and the equivalent DataFrame API call. */
  private def hintBlock(title: String, hints: Seq[Hint]): Seq[Node] =
    if (hints.isEmpty) Nil
    else
      <div style="margin-top:8px">
        <strong>{title}:</strong>
        <table class="table table-bordered table-condensed" style="margin-top:4px;max-width:640px">
          <thead><tr><th>SparkSQL hint</th><th>DataFrame equivalent</th></tr></thead>
          <tbody>
            {hints.map { h =>
              <tr>
                <td><code>{"/*+ " + h.render + " */"}</code></td>
                <td><code>{Hint.dataframeOp(h)}</code></td>
              </tr>
            }}
          </tbody>
        </table>
      </div> :: Nil

  private def disabledNotice: Seq[Node] =
    <div class="alert alert-info">
      <strong>Auto-fix is disabled.</strong> Enable it with
      <code>spark.sparkx.autofix.enabled=true</code> and register the extension:
      <code>spark.sql.extensions=org.apache.spark.sql.sparkx.SparkXAutoFixExtension</code>.
      It applies learned hints at the logical-plan level, so it covers both SparkSQL and the
      DataFrame/Dataset API. Use <code>spark.sparkx.autofix.mode=shadow</code> for a dry-run
      that proposes hints without applying them.
    </div> :: Nil

  private def emptyNotice: Seq[Node] =
    <div class="alert alert-success">
      <strong>Auto-fix is enabled.</strong> No query profiles learned yet — run some SparkSQL
      <em>or DataFrame/Dataset</em> queries and they will appear here after the first execution.
    </div> :: Nil
}
