package com.sparkx.autofix

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

/**
 * Persists [[FixProfile]]s as one JSON file per fingerprint under a configurable base
 * path. Local (`file://` or plain) paths use `java.nio` directly so it works everywhere
 * without native Hadoop libraries; distributed schemes (HDFS, object stores) use the
 * Hadoop FileSystem API. All operations are best-effort: any failure is swallowed and
 * logged so that auto-fix can never break the host job.
 */
class FixProfileStore(basePath: String, hadoopConf: Configuration) {
  import FixProfileStore._

  private val backend: Backend = {
    val uri = try new java.net.URI(basePath) catch { case _: Throwable => null }
    val scheme = if (uri != null) uri.getScheme else null
    if (scheme == null || scheme.equalsIgnoreCase("file")) {
      val dir =
        if (scheme == null) java.nio.file.Paths.get(basePath)
        else java.nio.file.Paths.get(uri)
      new LocalBackend(dir)
    } else new HadoopBackend(new Path(basePath), hadoopConf)
  }

  /** Load the profile for a fingerprint, or None if absent/unreadable. */
  def load(fingerprint: String): Option[FixProfile] = quietly(Option.empty[FixProfile]) {
    backend.read(s"$fingerprint.json").map(fromJson)
  }

  /** Persist (overwrite) a profile. */
  def save(profile: FixProfile): Unit = quietly(()) {
    backend.write(s"${profile.fingerprint}.json", toJson(profile))
  }

  /** All stored profiles (for the UI), newest first. */
  def list(): Seq[FixProfile] = quietly(Seq.empty[FixProfile]) {
    backend.listNames()
      .filter(_.endsWith(".json"))
      .flatMap(n => try backend.read(n).map(fromJson) catch { case _: Throwable => None })
      .sortBy(-_.updatedTs)
  }

  private def quietly[T](default: T)(body: => T): T =
    try body catch {
      case e: Throwable =>
        System.err.println(s"[sparkx] FixProfileStore error: ${e.getClass.getName}: ${e.getMessage}")
        default
    }
}

object FixProfileStore {
  private implicit val formats: DefaultFormats.type = DefaultFormats

  // ── Storage backends ─────────────────────────────────────────────────────────

  private trait Backend {
    def read(name: String): Option[String]
    def write(name: String, content: String): Unit
    def listNames(): Seq[String]
  }

  private class LocalBackend(dir: java.nio.file.Path) extends Backend {
    import java.nio.file.Files
    private val charset = java.nio.charset.StandardCharsets.UTF_8
    def read(name: String): Option[String] = {
      val f = dir.resolve(name)
      if (Files.exists(f)) Some(new String(Files.readAllBytes(f), charset)) else None
    }
    def write(name: String, content: String): Unit = {
      Files.createDirectories(dir)
      Files.write(dir.resolve(name), content.getBytes(charset))
    }
    def listNames(): Seq[String] = {
      if (!Files.exists(dir)) return Seq.empty
      val stream = Files.list(dir)
      try {
        val it = stream.iterator()
        val buf = scala.collection.mutable.ArrayBuffer[String]()
        while (it.hasNext) buf += it.next().getFileName.toString
        buf.toSeq
      } finally stream.close()
    }
  }

  private class HadoopBackend(base: Path, hadoopConf: Configuration) extends Backend {
    private def fs: FileSystem = base.getFileSystem(hadoopConf)
    def read(name: String): Option[String] = {
      val f = new Path(base, name)
      val filesystem = fs
      if (!filesystem.exists(f)) None
      else {
        val in = filesystem.open(f)
        try Some(scala.io.Source.fromInputStream(in, "UTF-8").mkString) finally in.close()
      }
    }
    def write(name: String, content: String): Unit = {
      val filesystem = fs
      if (!filesystem.exists(base)) filesystem.mkdirs(base)
      val out = filesystem.create(new Path(base, name), true)
      try out.write(content.getBytes("UTF-8")) finally out.close()
    }
    def listNames(): Seq[String] = {
      val filesystem = fs
      if (!filesystem.exists(base)) Seq.empty
      else filesystem.listStatus(base).toSeq.map(_.getPath.getName)
    }
  }

  // ── JSON (flat, primitive-only DTOs) ─────────────────────────────────────────

  private case class AttemptDTO(hints: List[String], durationMs: Long, ts: Long, improved: Boolean)
  private case class ProfileDTO(
    version:      Int,
    fingerprint:  String,
    sample:       String,
    status:       String,
    baselineMs:   Option[Long],
    bestMs:       Option[Long],
    bestHints:    List[String],
    pendingHints: List[String],
    attempts:     List[AttemptDTO],
    updatedTs:    Long,
    baselinePlan: Option[String] = None,
    currentPlan:  Option[String] = None
  )

  private def parseHints(rendered: Seq[String]): Seq[Hint] = rendered.flatMap(Hint.parse)

  def toJson(p: FixProfile): String = Serialization.writePretty(ProfileDTO(
    version      = 1,
    fingerprint  = p.fingerprint,
    sample       = p.sample,
    status       = p.status,
    baselineMs   = p.baselineMs,
    bestMs       = p.bestMs,
    bestHints    = p.bestHints.map(_.render).toList,
    pendingHints = p.pendingHints.map(_.render).toList,
    attempts     = p.attempts.map(a =>
                     AttemptDTO(a.hints.map(_.render).toList, a.durationMs, a.ts, a.improved)).toList,
    updatedTs    = p.updatedTs,
    baselinePlan = p.baselinePlan,
    currentPlan  = p.currentPlan
  ))

  def fromJson(text: String): FixProfile = {
    val d = Serialization.read[ProfileDTO](text)
    FixProfile(
      fingerprint  = d.fingerprint,
      sample       = d.sample,
      status       = d.status,
      baselineMs   = d.baselineMs,
      bestMs       = d.bestMs,
      bestHints    = parseHints(d.bestHints),
      pendingHints = parseHints(d.pendingHints),
      attempts     = d.attempts.map(a =>
                       FixAttempt(parseHints(a.hints), a.durationMs, a.ts, a.improved)),
      updatedTs    = d.updatedTs,
      baselinePlan = d.baselinePlan,
      currentPlan  = d.currentPlan
    )
  }
}
