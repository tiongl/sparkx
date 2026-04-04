package org.apache.spark.ui.xpark

import com.xpark.Utils
import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node

class SpillPage(parent: XParkTab) extends WebUIPage("spill") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val spillStages = IssueDetector.spillStages(parent.sparkUI.store)

    val content =
      <div>
        <p>
          Stages with <strong>disk spill &gt; 0</strong> are listed below.
          Shuffle spill occurs when executor memory is insufficient to hold intermediate shuffle data.
          Consider increasing <code>spark.executor.memory</code> or reducing partition size.
        </p>
        {if (spillStages.isEmpty)
          <div class="alert alert-success">No shuffle spill detected in any stage.</div>
        else
          <table class="table table-bordered table-condensed table-striped sortable">
            <thead>
              <tr>
                <th>Stage ID</th><th>Stage Name</th>
                <th>Disk Spill</th><th>Memory Spill</th>
              </tr>
            </thead>
            <tbody>
              {spillStages.map { case (stageId, name, disk, mem) =>
                <tr class="warning">
                  <td>{stageId}</td>
                  <td>{name}</td>
                  <td><strong>{Utils.formatBytes(disk)}</strong></td>
                  <td>{Utils.formatBytes(mem)}</td>
                </tr>
              }}
            </tbody>
          </table>
        }
      </div>

    UIUtils.headerSparkPage(request, "xpark — Shuffle Spill Analysis", content, parent)
  }
}
