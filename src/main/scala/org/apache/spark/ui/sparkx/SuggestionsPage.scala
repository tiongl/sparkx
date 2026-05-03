package org.apache.spark.ui.sparkx

import com.sparkx.analysis._
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class SuggestionsPage(parent: SparkXTab) extends WebUIPage("suggestions") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val suggestions = SuggestionDetector.detect(parent.sparkUI.store, parent.config)

    val content = if (suggestions.isEmpty) {
      <div class="alert alert-success">
        <strong>No optimization suggestions.</strong> Your queries look well-optimized.
      </div>
    } else {
      val grouped = suggestions.groupBy(_.executionId).toSeq
        .sortBy { case (idOpt, _) => -idOpt.getOrElse(0L) }

      <div>
        <p>Found <strong>{suggestions.size}</strong> optimization suggestion(s)
          across <strong>{grouped.size}</strong> SQL execution(s).</p>
        {grouped.map { case (execIdOpt, execSuggestions) =>
          val execLabel = execIdOpt.map(id => s"SQL Execution #$id").getOrElse("Unknown Execution")
          val dagLink = execIdOpt.map { id =>
            <a href={SparkXPageUtils.sqlExecUrl(request, id)} style="margin-left:12px">
              📊 View DAG
            </a>
          }.getOrElse(scala.xml.NodeSeq.Empty)

          <div style="margin-bottom:24px">
            <h4>{execLabel} {dagLink}</h4>
            <table class="table table-bordered table-condensed table-striped sortable">
              <thead>
                <tr>
                  <th>Severity</th>
                  <th>Suggestion</th>
                  <th>Description</th>
                  <th>Recommendation</th>
                </tr>
              </thead>
              <tbody>
                {execSuggestions.map { s =>
                  val labelClass = s.severity match {
                    case Critical => "label-danger"
                    case Warning  => "label-warning"
                    case _        => "label-info"
                  }
                  <tr>
                    <td><span class={s"label $labelClass"}>{s.severity.toString}</span></td>
                    <td><strong>{s.title}</strong></td>
                    <td>{s.description}</td>
                    <td style="font-size:12px">{s.recommendation}</td>
                  </tr>
                }}
              </tbody>
            </table>
          </div>
        }}
      </div>
    }

    UIUtils.headerSparkPage(request, "sparkx — Optimization Suggestions",
      SparkXPageUtils.subNavBar(request, "suggestions") ++ content, parent)
  }
}
