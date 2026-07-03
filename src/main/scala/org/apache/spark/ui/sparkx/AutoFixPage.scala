package org.apache.spark.ui.sparkx

import com.sparkx.Utils
import com.sparkx.autofix.{FixProfile, FixProfileStore, Hint, SaltedJoinHint, SplitBroadcastHint, TargetedSaltHint}
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
      {recommendationBlock(p.recommendations)}
      {planDiffBlock(p)}
    </div> :: Nil
  }

  // ── Plan diff rendering ───────────────────────────────────────────────────────
  /**
   * Render the baseline vs current executed plan as a colour-coded, side-by-side line diff.
   * Matching ignores whitespace (leading/trailing and collapsed runs) so cosmetic indentation
   * changes are not reported. Red = removed from baseline, green = added by the fix. Falls back
   * to a raw side-by-side view for very large plans, and shows an explicit note when the two
   * plans are identical (ignoring whitespace).
   */
  private def planDiffBlock(p: FixProfile): Seq[Node] =
    (p.baselinePlan, p.currentPlan) match {
      case (Some(base), Some(cur)) =>
        val a = base.split("\n", -1)
        val b = cur.split("\n", -1)
        if (a.length + b.length > 1200) sideBySide(base, cur)   // too large to diff cheaply
        else {
          val rows = sideBySideDiff(a, b)
          if (!rows.exists(_.kind != 'S'))
            <div class="alert alert-info" style="margin-top:8px">
              Baseline and current plans are <strong>identical</strong> (ignoring whitespace) — no
              plan change yet (this is the baseline run, or the pending hints have not been
              applied/accepted).
            </div> :: Nil
          else
            <div style="margin-top:8px">
              <strong>Plan diff (baseline vs current)</strong>
              <span style="margin-left:10px;font-size:12px">
                <span style="background:#ffeef0;padding:0 6px;border:1px solid #f0c4cb">− removed (baseline)</span>
                <span style="background:#e6ffed;padding:0 6px;border:1px solid #b4e2c0;margin-left:6px">+ added (current)</span>
                <span style="margin-left:6px;color:#777">ignores whitespace &amp; codegen/expr ids</span>
              </span>
              <table style="width:100%;table-layout:fixed;border-collapse:collapse;font-family:monospace;font-size:12px;margin-top:4px;border:1px solid #ccc">
                <thead>
                  <tr>
                    <th style="width:50%;text-align:left;border-bottom:1px solid #ccc;padding:2px 6px">Baseline plan (un-hinted)</th>
                    <th style="width:50%;text-align:left;border-bottom:1px solid #ccc;border-left:1px solid #ccc;padding:2px 6px">Current plan (latest run)</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map { r =>
                    val leftBg  = if (r.kind == 'D' || r.kind == 'C') "#ffeef0" else "transparent"
                    val rightBg = if (r.kind == 'A' || r.kind == 'C') "#e6ffed" else "transparent"
                    <tr style="vertical-align:top">
                      <td style={s"width:50%;white-space:pre-wrap;word-break:break-all;padding:0 6px;background:$leftBg"}>{r.left.getOrElse("")}</td>
                      <td style={s"width:50%;white-space:pre-wrap;word-break:break-all;padding:0 6px;border-left:1px solid #ccc;background:$rightBg"}>{r.right.getOrElse("")}</td>
                    </tr>
                  }}
                </tbody>
              </table>
            </div> :: Nil
        }
      case _ =>
        sideBySide(p.baselinePlan.getOrElse("—"), p.currentPlan.getOrElse("—"))
    }

  /** Side-by-side raw plans (used as a fallback for very large plans). */
  private def sideBySide(base: String, cur: String): Seq[Node] =
    <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:8px">
      <div style="flex:1;min-width:320px">
        <strong>Baseline plan (un-hinted)</strong>
        <pre style="white-space:pre;overflow:auto;max-height:320px">{base}</pre>
      </div>
      <div style="flex:1;min-width:320px">
        <strong>Current plan (latest run)</strong>
        <pre style="white-space:pre;overflow:auto;max-height:320px">{cur}</pre>
      </div>
    </div> :: Nil

  /** One aligned row of a side-by-side diff. kind: 'S' same, 'D' deleted, 'A' added, 'C' changed. */
  private case class DiffRow(kind: Char, left: Option[String], right: Option[String])

  /**
   * Canonicalize a plan line for *comparison only* (display keeps the original). Beyond
   * whitespace, this masks the volatile tokens Spark regenerates on every run — whole-stage
   * codegen ids `*(3)`, `[plan_id=52]`, and attribute/expr ids like `dim_id#2L` — so the diff
   * highlights real structural changes (e.g. SortMergeJoin → BroadcastHashJoin) instead of
   * noise from renumbering.
   */
  private def normalizeWs(s: String): String =
    s.trim
      .replaceAll("\\s+", " ")
      .replaceAll("\\*\\(\\d+\\)", "*")        // whole-stage codegen id:  *(3)      -> *
      .replaceAll("plan_id=\\d+", "plan_id=")  // exchange/broadcast id:   plan_id=52 -> plan_id=
      .replaceAll("#\\d+", "#")                // attribute/expr id:       dim_id#2L  -> dim_id#L

  /**
   * Classic LCS-based line diff, matching on canonicalized lines but preserving the original
   * text for display. Within each change hunk (a run of removals followed by additions) the
   * removed and added lines are zipped onto the *same* row — a true split diff (left red /
   * right green) — with any surplus on the longer side becoming pure D/A rows. Unchanged lines
   * fill both columns.
   */
  private def sideBySideDiff(a: Array[String], b: Array[String]): Seq[DiffRow] = {
    val na = a.map(normalizeWs)
    val nb = b.map(normalizeWs)
    val n = a.length
    val m = b.length
    val lcs = Array.ofDim[Int](n + 1, m + 1)
    var i = n - 1
    while (i >= 0) {
      var j = m - 1
      while (j >= 0) {
        lcs(i)(j) = if (na(i) == nb(j)) lcs(i + 1)(j + 1) + 1
                    else math.max(lcs(i + 1)(j), lcs(i)(j + 1))
        j -= 1
      }
      i -= 1
    }

    val out  = scala.collection.mutable.ListBuffer[DiffRow]()
    val dels = scala.collection.mutable.ListBuffer[String]()
    val adds = scala.collection.mutable.ListBuffer[String]()

    // Emit the buffered change hunk, zipping removals against additions row-by-row.
    def flushHunk(): Unit = {
      val k = math.max(dels.size, adds.size)
      var t = 0
      while (t < k) {
        val l = if (t < dels.size) Some(dels(t)) else None
        val r = if (t < adds.size) Some(adds(t)) else None
        val kind = (l.isDefined, r.isDefined) match {
          case (true, true) => 'C'
          case (true, _)    => 'D'
          case _            => 'A'
        }
        out += DiffRow(kind, l, r)
        t += 1
      }
      dels.clear(); adds.clear()
    }

    i = 0
    var j = 0
    while (i < n && j < m) {
      if (na(i) == nb(j))                      { flushHunk(); out += DiffRow('S', Some(a(i)), Some(b(j))); i += 1; j += 1 }
      else if (lcs(i + 1)(j) >= lcs(i)(j + 1)) { dels += a(i); i += 1 }
      else                                     { adds += b(j); j += 1 }
    }
    while (i < n) { dels += a(i); i += 1 }
    while (j < m) { adds += b(j); j += 1 }
    flushHunk()
    out.toList
  }

  /**
   * Skew-resolution recommendations. These are *not* auto-applied hints — they are DataFrame-API
   * strategies (double-broadcast / salting) that eliminate join skew that a plain hint cannot fix.
   */
  private def recommendationBlock(recs: Seq[Hint]): Seq[Node] =
    if (recs.isEmpty) Nil
    else {
      def label(h: Hint): String = h match {
        case SplitBroadcastHint(t, n) => s"Double broadcast — split $t into $n broadcastable chunks"
        case SaltedJoinHint(t, n)     => s"Salting — replicate $t across $n salt buckets"
        case TargetedSaltHint(t, n, ks) => s"Targeted salting — spread ${ks.size} hot key(s) of $t across $n buckets"
        case other                    => other.render
      }
      <div style="margin-top:8px">
        <strong>Skew-resolution recommendations:</strong>
        <span style="color:#777;font-size:12px;margin-left:6px">
          applied via the sparkx DataFrame join API, not as an auto-injected plan hint
        </span>
        <table class="table table-bordered table-condensed" style="margin-top:4px;max-width:820px">
          <thead><tr><th>Strategy</th><th>DataFrame API call</th></tr></thead>
          <tbody>
            {recs.map { h =>
              <tr>
                <td>{label(h)}</td>
                <td><code>{Hint.dataframeOp(h)}</code></td>
              </tr>
            }}
          </tbody>
        </table>
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
