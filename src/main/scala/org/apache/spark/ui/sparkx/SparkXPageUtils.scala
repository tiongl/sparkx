package org.apache.spark.ui.sparkx

import javax.servlet.http.HttpServletRequest

/** URL helpers that work for both live Spark UI and History Server. */
object SparkXPageUtils {

  def stageUrl(request: HttpServletRequest, stageId: Int, attemptId: Int = 0): String = {
    val base = appBase(request)
    s"$base/stages/stage/?id=$stageId&attempt=$attemptId"
  }

  def jobUrl(request: HttpServletRequest, jobId: Int): String =
    s"${appBase(request)}/jobs/job/?id=$jobId"

  def sqlExecUrl(request: HttpServletRequest, execId: Long): String =
    s"${appBase(request)}/SQL/execution/?id=$execId"

  /** URL to a named sparkx sub-page (e.g. "skew", "gc"). */
  def sparkxSubUrl(request: HttpServletRequest, subPath: String): String = {
    val base = appBase(request)
    s"$base/sparkx/$subPath"
  }

  /** Render a Job ID table cell for the given stage. */
  def jobTd(request: HttpServletRequest, stageId: Int,
            stageJobs: Map[Int, Int]): scala.xml.Elem =
    stageJobs.get(stageId) match {
      case Some(jid) => <td><a href={jobUrl(request, jid)}>{jid}</a></td>
      case None      => <td style="color:#ccc">—</td>
    }

  /** Render a Job ID table cell for an optional stage. */
  def jobTd(request: HttpServletRequest, stageId: Option[Int],
            stageJobs: Map[Int, Int]): scala.xml.Elem =
    stageId.flatMap(stageJobs.get) match {
      case Some(jid) => <td><a href={jobUrl(request, jid)}>{jid}</a></td>
      case None      => <td style="color:#ccc">—</td>
    }

  /** Render a DAG link table cell for the given stage. */
  def dagTd(request: HttpServletRequest, stageId: Int,
            stageSqlExecs: Map[Int, Long]): scala.xml.Elem =
    stageSqlExecs.get(stageId) match {
      case Some(eid) => <td><a href={sqlExecUrl(request, eid)}>📊 DAG</a></td>
      case None      => <td style="color:#ccc">—</td>
    }

  /** Render a DAG link table cell for an optional stage. */
  def dagTd(request: HttpServletRequest, stageId: Option[Int],
            stageSqlExecs: Map[Int, Long]): scala.xml.Elem =
    stageId.flatMap(stageSqlExecs.get) match {
      case Some(eid) => <td><a href={sqlExecUrl(request, eid)}>📊 DAG</a></td>
      case None      => <td style="color:#ccc">—</td>
    }

  private def appBase(request: HttpServletRequest): String = {
    val uri = request.getRequestURI
    val idx = uri.indexOf("/sparkx")
    if (idx >= 0) uri.substring(0, idx) else ""
  }
}
