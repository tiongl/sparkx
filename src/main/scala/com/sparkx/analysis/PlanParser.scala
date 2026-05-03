package com.sparkx.analysis

/**
 * Parses Spark's physicalPlanDescription text into structured data
 * for detecting missed optimization opportunities.
 */
object PlanParser {

  case class ParsedPlan(
    hasAQE:         Boolean,
    joinNodes:      Seq[JoinInfo],
    exchangeCount:  Int,
    scanNodes:      Seq[ScanInfo],
    cartesianNodes: Seq[CartesianInfo],
    pythonUDFs:     Seq[PythonUDFInfo],
    collectNodes:   Seq[CollectInfo],
    sortNodes:      Seq[SortInfo]
  )

  case class JoinInfo(joinType: String, keys: String, line: String)
  case class ScanInfo(
    format:           String,
    partitionFilters: String,
    pushedFilters:    String,
    dataFilters:      String,
    line:             String
  )
  case class CartesianInfo(nodeType: String, line: String)
  case class PythonUDFInfo(nodeType: String, line: String)
  case class CollectInfo(nodeType: String, line: String)
  case class SortInfo(global: Boolean, line: String)

  // Matches SortMergeJoin/ShuffledHashJoin in both old and Spark 3.5 formatted plans:
  //   old:       SortMergeJoin [dept_id#10], [dept_id#20], Inner
  //   formatted: * SortMergeJoin Inner (11)
  //   detail:    (11) SortMergeJoin [codegen id : 6]
  private val JoinPattern =
    """(?i)\b(SortMergeJoin|ShuffledHashJoin)\b""".r
  private val JoinKeysPattern =
    """(?i)(?:SortMergeJoin|ShuffledHashJoin)\s+\[(.+?)\],\s*\[(.+?)\]""".r
  private val CartesianPattern =
    """(?i)(CartesianProduct|BroadcastNestedLoopJoin)""".r
  private val ExchangePattern =
    """(?i)\bExchange\b""".r
  // Matches both "FileScan csv" (old) and "Scan csv" (Spark 3.5 formatted)
  private val ScanPattern =
    """(?i)\b(?:File)?Scan\s+(\w+)""".r
  private val PartitionFiltersPattern =
    """PartitionFilters:\s*\[([^\]]*)\]""".r
  private val PushedFiltersPattern =
    """PushedFilters:\s*\[([^\]]*)\]""".r
  private val DataFiltersPattern =
    """DataFilters:\s*\[([^\]]*)\]""".r
  private val PythonUDFPattern =
    """(?i)(BatchEvalPython|ArrowEvalPython|FlatMapGroupsInPandas|MapInPandas|AggregateInPandas)""".r
  private val CollectPattern =
    """(?i)(CollectLimit|GlobalLimit)\b""".r
  private val SortPattern =
    """(?i)\bSort\s+\[(.+?)\],\s*(true|false)""".r

  def parse(planDescription: String): ParsedPlan = {
    if (planDescription == null || planDescription.isEmpty)
      return ParsedPlan(hasAQE = false, Nil, 0, Nil, Nil, Nil, Nil, Nil)

    val lines = planDescription.split("\n")
    val hasAQE = lines.exists(_.contains("AdaptiveSparkPlan"))

    val joins = scala.collection.mutable.ArrayBuffer[JoinInfo]()
    val scans = scala.collection.mutable.ArrayBuffer[ScanInfo]()
    val cartesians = scala.collection.mutable.ArrayBuffer[CartesianInfo]()
    val pythonUDFs = scala.collection.mutable.ArrayBuffer[PythonUDFInfo]()
    val collects = scala.collection.mutable.ArrayBuffer[CollectInfo]()
    val sorts = scala.collection.mutable.ArrayBuffer[SortInfo]()
    var exchangeCount = 0
    val seenJoins = scala.collection.mutable.Set[String]()
    val seenScans = scala.collection.mutable.Set[String]()

    for (i <- lines.indices) {
      val line = lines(i)
      val trimmed = line.replaceAll("""^[\s|:+\-\\]+""", "")

      // Joins: match keyword, deduplicate tree vs detail references
      JoinPattern.findFirstMatchIn(trimmed).foreach { m =>
        val joinType = m.group(1)
        val keys = JoinKeysPattern.findFirstMatchIn(trimmed) match {
          case Some(km) => s"[${km.group(1)}], [${km.group(2)}]"
          case None     => ""
        }
        // Deduplicate: tree has "SortMergeJoin Inner (11)", detail has "(11) SortMergeJoin"
        // Only deduplicate when a node ID is present (Spark 3.5 formatted plans)
        val nodeId = """\((\d+)\)""".r.findFirstMatchIn(trimmed).map(_.group(1))
        val shouldAdd = nodeId match {
          case Some(id) => seenJoins.add(s"$joinType-$id")
          case None     => true // old format without node IDs — always add
        }
        if (shouldAdd) {
          joins += JoinInfo(joinType, keys, trimmed)
        }
      }

      CartesianPattern.findFirstMatchIn(trimmed).foreach { m =>
        cartesians += CartesianInfo(m.group(1), trimmed)
      }

      if (ExchangePattern.findFirstIn(trimmed).isDefined)
        exchangeCount += 1

      PythonUDFPattern.findFirstMatchIn(trimmed).foreach { m =>
        pythonUDFs += PythonUDFInfo(m.group(1), trimmed)
      }

      CollectPattern.findFirstMatchIn(trimmed).foreach { m =>
        collects += CollectInfo(m.group(1), trimmed)
      }

      SortPattern.findFirstMatchIn(trimmed).foreach { m =>
        sorts += SortInfo(global = m.group(2) == "true", trimmed)
      }

      // Scans: match "Scan csv" or "FileScan csv", then look ahead for filters
      ScanPattern.findFirstMatchIn(trimmed).foreach { m =>
        val format = m.group(1).toLowerCase
        // Only deduplicate when a node ID is present (Spark 3.5 formatted plans)
        val nodeId = """\((\d+)\)""".r.findFirstMatchIn(trimmed).map(_.group(1))
        val shouldAdd = nodeId match {
          case Some(id) => seenScans.add(s"scan-$format-$id")
          case None     => true // old format without node IDs — always add
        }
        if (shouldAdd) {
          // Look ahead in subsequent lines for filter info (Spark 3.5 detail section)
          val lookahead = lines.slice(i, Math.min(i + 8, lines.length)).mkString("\n")
          val partFilters = PartitionFiltersPattern.findFirstMatchIn(lookahead)
            .map(_.group(1)).getOrElse("")
          val pushedFilters = PushedFiltersPattern.findFirstMatchIn(lookahead)
            .map(_.group(1)).getOrElse("")
          val dataFilters = DataFiltersPattern.findFirstMatchIn(lookahead)
            .map(_.group(1)).getOrElse("")
          scans += ScanInfo(format, partFilters, pushedFilters, dataFilters, trimmed)
        }
      }
    }

    ParsedPlan(hasAQE, joins.toSeq, exchangeCount, scans.toSeq, cartesians.toSeq,
      pythonUDFs.toSeq, collects.toSeq, sorts.toSeq)
  }
}
