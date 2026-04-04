package org.apache.spark.ui.xpark

import com.xpark.Utils
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class SkewPage(parent: XParkTab) extends WebUIPage("skew") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val rows = IssueDetector.skewStages(parent.sparkUI.store, parent.config)
      .sortBy(-_._5) // sort by ratio descending

    val content =
      <div>
        <p>
          Stages where <strong>max task duration / median task duration</strong> exceeds
          <strong>{parent.config.skewMultiplier}×</strong> are flagged as skewed.
          Data skew means some tasks receive significantly more data than others.
        </p>
        {if (rows.isEmpty)
          <div class="alert alert-success">No skewed stages detected.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Stage Name</th>
                <th>Median Duration</th><th>Max Duration</th>
                <th>Ratio (Max/Median)</th><th>Status</th>
              </tr>
            </thead>
            <tbody>
              {rows.map { case (stageId, stageName, med, max, ratio) =>
                val isSkewed = ratio >= parent.config.skewMultiplier
                <tr class={if (isSkewed) "danger" else ""}>
                  <td>{stageId}</td>
                  <td>{stageName}</td>
                  <td>{Utils.formatDuration(med.toLong)}</td>
                  <td>{Utils.formatDuration(max.toLong)}</td>
                  <td>{f"$ratio%.2f"}×</td>
                  <td>{if (isSkewed) <span class="label label-danger">Skewed</span>
                       else <span class="label label-success">OK</span>}</td>
                </tr>
              }}
            </tbody>
          </table>
        }
      </div>

    UIUtils.headerSparkPage(request, "xpark — Data Skew Analysis", content, parent)
  }
}
