package org.apache.spark.ui.xpark

import com.xpark.Utils
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class StragglerPage(parent: XParkTab) extends WebUIPage("stragglers") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val rows = IssueDetector.stragglerStages(parent.sparkUI.store, parent.config)

    val content =
      <div>
        <p>
          A task is a <strong>straggler</strong> when its duration exceeds
          <code>Q3 + {parent.config.stragglerIQRFactor} × IQR</code>.
          Stragglers slow down the entire stage. Common causes: data skew, GC, network issues.
        </p>
        {if (rows.isEmpty)
          <div class="alert alert-success">No straggler tasks detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Stage Name</th><th>Median</th>
                <th>P75</th><th>Max</th><th>IQR Threshold</th>
              </tr>
            </thead>
            <tbody>
              {rows.map { case (stageId, name, med, q3, max, fence) =>
                <tr class="warning">
                  <td>{stageId}</td>
                  <td>{name}</td>
                  <td>{Utils.formatDuration(med.toLong)}</td>
                  <td>{Utils.formatDuration(q3.toLong)}</td>
                  <td><strong>{Utils.formatDuration(max.toLong)}</strong></td>
                  <td>{Utils.formatDuration(fence.toLong)}</td>
                </tr>
              }}
            </tbody>
          </table>
        }
      </div>

    UIUtils.headerSparkPage(request, "xpark — Straggler Task Analysis", content, parent)
  }
}
