package com.sparkx.autofix

import org.apache.hadoop.conf.Configuration

/**
 * Public, reusable helper for **SQL-text** hint manipulation — independent of the Spark UI
 * and the runtime interception path. Use it to:
 *
 *  - manually apply hints to a query: [[rewrite]]
 *  - compute a query's text fingerprint: [[fingerprint]]
 *  - look up a text-keyed profile and produce the fixed SQL: [[fix]]
 *
 * Note: the runtime auto-fix loop ([[org.apache.spark.sql.sparkx.SparkXAutoFixExtension]]) keys
 * profiles by *logical-plan* fingerprint so it can cover both SparkSQL and the DataFrame/Dataset
 * API. This facade is a standalone text utility and uses a separate, text-based key scheme; it is
 * not wired to the runtime plan-keyed store.
 *
 * {{{
 *   val store = SparkXAutoFix.store("file:///tmp/sparkx-autofix-text", hadoopConf)
 *   val fixed = SparkXAutoFix.fix("SELECT * FROM a JOIN b ON a.id = b.id", store)
 * }}}
 */
object SparkXAutoFix {

  /** Fingerprint (hash key) for a query, ignoring literal values. */
  def fingerprint(sql: String): String = QueryFingerprint.compute(sql)

  /** Apply an explicit set of hints to a query's outer SELECT. */
  def rewrite(sql: String, hints: Seq[Hint]): String = HintRewriter.rewrite(sql, hints)

  /** Build a profile store rooted at `basePath`. */
  def store(basePath: String, hadoopConf: Configuration): FixProfileStore =
    new FixProfileStore(basePath, hadoopConf)

  /**
   * Return the hint-annotated version of `sql` using the best-known-good hints from a
   * previously learned profile, or the original SQL if nothing has been learned yet.
   */
  def fix(sql: String, store: FixProfileStore): String = {
    val hints = hintsFor(sql, store)
    if (hints.isEmpty) sql else HintRewriter.rewrite(sql, hints)
  }

  /** The hints that would be applied for `sql` given the current learned state. */
  def hintsFor(sql: String, store: FixProfileStore): Seq[Hint] =
    store.load(fingerprint(sql)).map { p =>
      if (p.pendingHints.nonEmpty) p.pendingHints else p.bestHints
    }.getOrElse(Nil)
}
