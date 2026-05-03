package org.apache.spark.ui.sparkx

import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class BroadcastPage(parent: SparkXTab) extends WebUIPage("broadcast") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val broadcasts = IssueDetector.broadcastRDDs(parent.sparkUI.store, parent.config)

    val content =
      <div>
        <p>
          Broadcast variables larger than <strong>{parent.config.broadcastSizeMB} MB</strong> are listed below.
          Large broadcasts increase driver memory pressure and network overhead.
          Consider filtering the dataset before broadcasting, or using a join strategy instead.
        </p>
        {if (broadcasts.isEmpty)
          <div class="alert alert-success">
            No oversized broadcast variables detected (threshold: {parent.config.broadcastSizeMB} MB).
          </div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr><th>Broadcast Variable</th><th>Size (MB)</th></tr>
            </thead>
            <tbody>
              {broadcasts.map { case (name, sizeMB) =>
                <tr class="warning">
                  <td>{name}</td>
                  <td>{sizeMB}</td>
                </tr>
              }}
            </tbody>
          </table>
        }
      </div>

    UIUtils.headerSparkPage(request, "SparkX — Broadcast Size Analysis",
      SparkXPageUtils.subNavBar(request, "broadcast") ++ content, parent)
  }
}
