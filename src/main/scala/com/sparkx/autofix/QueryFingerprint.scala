package com.sparkx.autofix

/**
 * Computes a stable, literal-insensitive fingerprint for a SQL query so that the
 * same logical query re-run with different parameter values (e.g. a scheduled job
 * with a different `dt='...'`) maps to a single fix profile.
 *
 * Normalization: strip comments (including existing hint blocks), replace string
 * and numeric literals with `?`, lower-case, and collapse whitespace.
 */
object QueryFingerprint {

  private val BlockComment = """(?s)/\*.*?\*/""".r
  private val LineComment  = """--[^\n]*""".r
  private val StringLit    = """'(?:[^']|'')*'""".r
  // A number not glued to an identifier char or a dot on the left (so `col1` is safe).
  private val NumberLit    = """(?<![\w.])\d+(?:\.\d+)?""".r
  private val Whitespace   = """\s+""".r

  /** Canonical, literal-insensitive form of the query. */
  def normalize(sql: String): String = {
    if (sql == null) return ""
    var s = sql
    s = BlockComment.replaceAllIn(s, " ")
    s = LineComment.replaceAllIn(s, " ")
    s = StringLit.replaceAllIn(s, "?")
    s = NumberLit.replaceAllIn(s, "?")
    s = s.toLowerCase
    s = Whitespace.replaceAllIn(s, " ")
    s = s.trim
    if (s.endsWith(";")) s = s.dropRight(1).trim
    s
  }

  /** 32-hex-char fingerprint (first 128 bits of SHA-256 over the normalized text). */
  def compute(sql: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    val bytes = md.digest(normalize(sql).getBytes("UTF-8"))
    bytes.take(16).map(b => f"${b & 0xff}%02x").mkString
  }
}
