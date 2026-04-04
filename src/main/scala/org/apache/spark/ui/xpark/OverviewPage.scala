package org.apache.spark.ui.xpark

import com.xpark.analysis.{Critical, Warning}
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class OverviewPage(parent: XParkTab) extends WebUIPage("") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val issues = IssueDetector.detect(parent.sparkUI.store, parent.config)

    val content = if (issues.isEmpty) {
      <div class="alert alert-success">
        <strong>No performance issues detected.</strong> Your Spark job looks healthy.
      </div>
    } else {
      <div>
        <p>Found <strong>{issues.length}</strong> performance issue(s).
          Use the sub-tabs above for detailed analysis.</p>
        <table class="table table-bordered table-condensed table-striped sortable">
          <thead>
            <tr>
              <th>Severity</th><th>Stage</th><th>Issue</th><th>Description</th>
            </tr>
          </thead>
          <tbody>
            {issues.map { issue =>
              val labelClass = issue.severity match {
                case Critical => "label-danger"
                case Warning  => "label-warning"
                case _        => "label-info"
              }
              <tr>
                <td><span class={s"label $labelClass"}>{issue.severity.toString}</span></td>
                <td>{issue.stageId.map(_.toString).getOrElse("—")}</td>
                <td><strong>{issue.title}</strong></td>
                <td>{issue.description}</td>
              </tr>
            }}
          </tbody>
        </table>
      </div>
    }

    UIUtils.headerSparkPage(request, "xpark — Performance Analysis", content, parent)
  }
}
