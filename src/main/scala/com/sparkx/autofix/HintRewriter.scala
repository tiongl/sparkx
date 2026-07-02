package com.sparkx.autofix

/**
 * Pure text-level SQL rewriter that splices SparkSQL hints into a query's leading
 * hint block that follows the outer SELECT keyword. This powers the runtime auto-fix
 * parser, the UI page, and the public library API.
 *
 * The rewriter targets the first `SELECT` at parenthesis depth 0 (so it attaches to
 * the outer query rather than a CTE/sub-select), and merges with any hint block that
 * is already present, replacing hints of the same category.
 */
object HintRewriter {

  /** Return `sql` with the given hints applied to its outer SELECT hint block. */
  def rewrite(sql: String, hints: Seq[Hint]): String = {
    val clean = dedupe(hints)
    if (sql == null || clean.isEmpty) return sql
    findOuterSelectEnd(sql) match {
      case None => sql // could not locate a safe injection point; leave untouched
      case Some(idx) =>
        val head = sql.substring(0, idx)
        val tail = sql.substring(idx)
        LeadingHintBlock.findFirstMatchIn(tail) match {
          case Some(m) =>
            val existing = splitHintTokens(m.group(1))
            val merged = mergeTokens(existing, clean)
            val rest = tail.substring(m.end)
            s"$head /*+ ${merged.mkString(", ")} */$rest"
          case None =>
            s"$head /*+ ${clean.map(_.render).mkString(", ")} */$tail"
        }
    }
  }

  // ── Merging ────────────────────────────────────────────────────────────────

  /** Keep at most one hint per category (last occurrence wins), preserving order. */
  def dedupe(hints: Seq[Hint]): Seq[Hint] = {
    val seen = scala.collection.mutable.LinkedHashMap[String, Hint]()
    hints.foreach(h => seen(h.key) = h)
    seen.values.toSeq
  }

  /** Merge new hints into the existing (already-present) hint tokens. */
  private def mergeTokens(existing: Seq[String], added: Seq[Hint]): Seq[String] = {
    val addedKeys = added.map(_.key).toSet
    val kept = existing.filterNot { tok =>
      Hint.parse(tok).exists(h => addedKeys.contains(h.key))
    }
    (kept ++ added.map(_.render)).distinct
  }

  private def splitHintTokens(block: String): Seq[String] =
    block.split(",").map(_.trim).filter(_.nonEmpty).toSeq

  private val LeadingHintBlock = """(?s)\A\s*/\*\+(.*?)\*/""".r

  // ── Locating the outer SELECT ────────────────────────────────────────────────

  /** Index immediately after the first depth-0 `SELECT` keyword, or None. */
  private def findOuterSelectEnd(sql: String): Option[Int] = {
    val n = sql.length
    var i = 0
    var depth = 0
    var inStr = false
    while (i < n) {
      val c = sql(i)
      if (inStr) {
        if (c == '\'') {
          if (i + 1 < n && sql(i + 1) == '\'') i += 1 // escaped quote
          else inStr = false
        }
      } else if (c == '\'') {
        inStr = true
      } else if (c == '-' && i + 1 < n && sql(i + 1) == '-') {
        while (i < n && sql(i) != '\n') i += 1
      } else if (c == '/' && i + 1 < n && sql(i + 1) == '*') {
        i += 2
        while (i + 1 < n && !(sql(i) == '*' && sql(i + 1) == '/')) i += 1
        i += 1 // land on the closing '/'
      } else if (c == '(') {
        depth += 1
      } else if (c == ')') {
        depth -= 1
      } else if (depth == 0 && (c == 's' || c == 'S') && matchesWord(sql, i, "select")) {
        return Some(i + 6)
      }
      i += 1
    }
    None
  }

  private def matchesWord(s: String, at: Int, word: String): Boolean = {
    val end = at + word.length
    if (end > s.length) return false
    if (!s.regionMatches(true, at, word, 0, word.length)) return false
    val leftOk  = at == 0 || !isIdentChar(s(at - 1))
    val rightOk = end == s.length || !isIdentChar(s(end))
    leftOk && rightOk
  }

  private def isIdentChar(c: Char): Boolean = c.isLetterOrDigit || c == '_'
}
