package org.apache.spark.ui.sparkx

import com.sparkx.SparkXConfig
import com.sparkx.analysis._
import org.apache.spark.status.AppStatusStore

/**
 * Derives Spark [[com.sparkx.analysis.ConfigRecommendation]]s from detected performance symptoms.
 *
 * This is the "config channel" that complements the two existing advisory paths: unlike the
 * auto-fix hint injector (which can only emit per-query plan hints like BROADCAST/REPARTITION) and
 * [[SuggestionDetector]] (which suggests SQL/plan rewrites), this maps stage-health issues —
 * primarily GC pressure and shuffle spill — onto `SparkConf` knobs that must be set at the
 * session/executor level (spill thresholds, off-heap, memory fraction, serializer, executor memory).
 *
 * All recommendations are advisory; sparkx never changes the running configuration.
 */
object ConfigAdvisor {

  private val CACHE_TTL_MS = 30000L
  private case class CacheEntry(result: Seq[ConfigRecommendation], ts: Long)
  private val cache = new java.util.concurrent.ConcurrentHashMap[String, CacheEntry]()

  def advise(store: AppStatusStore, config: SparkXConfig): Seq[ConfigRecommendation] = {
    val e = cache.get("advise")
    if (e != null && System.currentTimeMillis() - e.ts < CACHE_TTL_MS) e.result
    else {
      val r = compute(store, config)
      cache.put("advise", CacheEntry(r, System.currentTimeMillis()))
      r
    }
  }

  private def compute(store: AppStatusStore, config: SparkXConfig): Seq[ConfigRecommendation] =
    recommend(IssueDetector.detect(store, config), sparkProps(store))

  /**
   * Pure decision logic: map detected issues + the app's Spark properties onto config
   * recommendations. Separated from [[advise]] so it can be unit-tested without a SparkSession.
   */
  private[sparkx] def recommend(
      issues: Seq[PerformanceIssue],
      props:  Map[String, String]): Seq[ConfigRecommendation] = {
    val gcIssues     = issues.collect { case g: GCPressureIssue => g }
    val spillIssues  = issues.collect { case s: ShuffleSpillIssue => s }
    val stragglers   = issues.collect { case s: StragglerIssue => s }
    val fetchWaits   = issues.collect { case f: HighFetchWaitIssue => f }
    val diskReads    = issues.collect { case d: DiskShuffleReadIssue => d }
    val slowSer      = issues.collect { case s: SlowResultSerializationIssue => s }
    val highDeser    = issues.collect { case d: HighDeserializationIssue => d }
    val failures     = issues.collect { case t: TaskFailuresIssue => t }
    val retries      = issues.collect { case r: StageRetryIssue => r }
    val serIssues    = slowSer ++ highDeser
    val failIssues: Seq[PerformanceIssue] = failures ++ retries

    if (gcIssues.isEmpty && spillIssues.isEmpty && stragglers.isEmpty && fetchWaits.isEmpty &&
        diskReads.isEmpty && serIssues.isEmpty && failIssues.isEmpty) return Nil

    val gcSeverity = worst(gcIssues.map(_.severity))
    val totalGcMs  = gcIssues.flatMap(_.estimatedSavingsMs).sum
    val totalSpill  = spillIssues.map(_.diskSpillBytes).sum
    val spillSaveMs = spillIssues.flatMap(_.estimatedSavingsMs).sum
    val gcAndSpill  = gcIssues.nonEmpty && spillIssues.nonEmpty

    // configKey -> recommendation, de-duplicated keeping the most urgent.
    val recs = scala.collection.mutable.LinkedHashMap[String, ConfigRecommendation]()
    def add(r: ConfigRecommendation): Unit = recs.get(r.configKey) match {
      case Some(prev) if ConfigRecommendation.rank(prev.severity) >= ConfigRecommendation.rank(r.severity) =>
      case _ => recs(r.configKey) = r
    }

    // ── GC-pressure driven config knobs ─────────────────────────────────────────
    if (gcIssues.nonEmpty) {
      // Explicit/earlier spill: directly shrinks the on-heap live set to relieve GC.
      add(ConfigRecommendation(
        severity         = gcSeverity,
        configKey        = "spark.shuffle.spill.numElementsForceSpillThreshold",
        currentValue     = props.getOrElse("spark.shuffle.spill.numElementsForceSpillThreshold",
                             "unset (spills only under memory pressure)"),
        recommendedValue = "5000000",
        title            = "Force aggregation/sort buffers to spill earlier",
        rationale        = "Forcing large in-memory aggregation/sort/join buffers to spill to disk " +
          "sooner shrinks the on-heap live set, which directly lowers GC frequency and full-GC pauses. " +
          "Trade-off: extra disk I/O and serialization CPU. Most effective when GC pressure coincides " +
          "with large buffers or existing spill; it does not help GC caused by allocation churn.",
        relatedIssue     = if (gcAndSpill) "GC Pressure + Shuffle Spill" else "GC Pressure",
        estimatedSavingsMs = Some(totalGcMs)))

      // Off-heap memory: moves buffers out of GC scope entirely.
      if (!props.get("spark.memory.offHeap.enabled").contains("true")) {
        add(ConfigRecommendation(
          severity         = gcSeverity,
          configKey        = "spark.memory.offHeap.enabled",
          currentValue     = props.getOrElse("spark.memory.offHeap.enabled", "false"),
          recommendedValue = "true (also set spark.memory.offHeap.size, e.g. 2g)",
          title            = "Enable off-heap execution memory",
          rationale        = "Moving shuffle/aggregation/join buffers into off-heap memory removes them " +
            "from garbage-collection scope, cutting GC time. Requires sizing spark.memory.offHeap.size.",
          relatedIssue     = "GC Pressure",
          estimatedSavingsMs = Some(totalGcMs)))
      }

      // Kryo moved to the shared serialization block below (also triggered by ser/deser issues).

      // If GC is critical but nothing is spilling, the heap is simply too small.
      if (spillIssues.isEmpty && gcIssues.exists(_.severity == Critical)) {
        add(ConfigRecommendation(
          severity         = Critical,
          configKey        = "spark.executor.memory",
          currentValue     = props.getOrElse("spark.executor.memory", "1g (cluster default)"),
          recommendedValue = "increase (e.g. +50%)",
          title            = "Increase executor memory",
          rationale        = "Sustained high GC with no spill usually means the executor heap is too " +
            "small for the working set, forcing constant collection. More executor memory reduces GC churn.",
          relatedIssue     = "GC Pressure",
          estimatedSavingsMs = Some(totalGcMs)))
      }
    }

    // ── Shuffle-spill driven config knobs ───────────────────────────────────────
    if (spillIssues.nonEmpty) {
      add(ConfigRecommendation(
        severity         = Warning,
        configKey        = "spark.executor.memory",
        currentValue     = props.getOrElse("spark.executor.memory", "1g (cluster default)"),
        recommendedValue = "increase (e.g. +50%)",
        title            = "Increase executor memory to reduce spill",
        rationale        = s"Stages spilled ${com.sparkx.Utils.formatBytes(totalSpill)} to disk because " +
          "shuffle/aggregation data exceeded execution memory. More executor memory lets more data stay " +
          "in memory. Alternatively raise spark.sql.shuffle.partitions to shrink per-task data.",
        relatedIssue     = "Shuffle Spill",
        estimatedSavingsMs = Some(spillSaveMs)))

      val fraction = props.get("spark.memory.fraction").flatMap(s => scala.util.Try(s.toDouble).toOption)
      if (fraction.exists(_ < 0.6)) {
        add(ConfigRecommendation(
          severity         = Warning,
          configKey        = "spark.memory.fraction",
          currentValue     = fraction.get.toString,
          recommendedValue = "0.6 (default) – 0.8",
          title            = "Raise the unified memory fraction",
          rationale        = "spark.memory.fraction is below the 0.6 default, leaving little execution " +
            "memory before spilling. Raising it gives shuffles/aggregations more room before spilling to disk.",
          relatedIssue     = "Shuffle Spill",
          estimatedSavingsMs = Some(spillSaveMs)))
      }
    }

    // ── Serializer (GC allocation churn + slow (de)serialization) ────────────────
    if (gcIssues.nonEmpty || serIssues.nonEmpty) {
      if (!props.get("spark.serializer").exists(_.contains("KryoSerializer"))) {
        val serSaveMs = serIssues.flatMap(_.estimatedSavingsMs).sum
        val related =
          if (gcIssues.nonEmpty && serIssues.nonEmpty) "GC Pressure + Slow (De)serialization"
          else if (serIssues.nonEmpty) "Slow (De)serialization"
          else "GC Pressure"
        add(ConfigRecommendation(
          severity         = if (serIssues.nonEmpty) Warning else Info,
          configKey        = "spark.serializer",
          currentValue     = props.getOrElse("spark.serializer", "org.apache.spark.serializer.JavaSerializer"),
          recommendedValue = "org.apache.spark.serializer.KryoSerializer",
          title            = "Switch to the Kryo serializer",
          rationale        = "Kryo produces smaller, faster-to-(de)serialize, cheaper-to-allocate objects " +
            "than Java serialization. This cuts task (de)serialization time and reduces the allocation " +
            "churn that drives GC pressure during shuffles and caching. Register hot classes with " +
            "spark.kryo.registrator for best results. Trade-off: unregistered classes fall back to a " +
            "slower path, and some custom types may need registration.",
          relatedIssue     = related,
          estimatedSavingsMs = if (serSaveMs > 0) Some(serSaveMs) else None))
      }
    }

    // ── Stragglers → speculative execution ───────────────────────────────────────
    if (stragglers.nonEmpty && !props.get("spark.speculation").contains("true")) {
      add(ConfigRecommendation(
        severity         = Warning,
        configKey        = "spark.speculation",
        currentValue     = props.getOrElse("spark.speculation", "false"),
        recommendedValue = "true",
        title            = "Enable speculative execution",
        rationale        = s"${stragglers.size} stage(s) were held up by straggler tasks. Speculation " +
          "re-launches abnormally slow tasks on other executors and uses whichever copy finishes first, " +
          "capping wall-clock time when stragglers come from a slow node or transient contention. " +
          "Tune with spark.speculation.multiplier / spark.speculation.quantile. Trade-off: extra compute " +
          "from duplicate tasks; it does NOT help stragglers caused by data skew (fix the partitioning " +
          "instead), since the re-run task processes the same oversized partition.",
        relatedIssue     = "Straggler Tasks",
        estimatedSavingsMs = Some(stragglers.flatMap(_.estimatedSavingsMs).sum)))
    }

    // ── High shuffle fetch wait → bigger fetch buffers + external shuffle service ─
    if (fetchWaits.nonEmpty) {
      val fetchSaveMs = fetchWaits.flatMap(_.estimatedSavingsMs).sum
      add(ConfigRecommendation(
        severity         = Warning,
        configKey        = "spark.reducer.maxSizeInFlight",
        currentValue     = props.getOrElse("spark.reducer.maxSizeInFlight", "48m"),
        recommendedValue = "96m (raise gradually)",
        title            = "Increase in-flight shuffle fetch size",
        rationale        = "Tasks spent a large fraction of their time blocked waiting for shuffle data. " +
          "Raising spark.reducer.maxSizeInFlight lets each reducer fetch more map output in parallel, " +
          "hiding network latency. Also consider spark.reducer.maxReqsInFlight and " +
          "spark.shuffle.io.numConnectionsPerPeer. Trade-off: higher fetch buffers use more executor " +
          "memory, so raise gradually and watch for GC/OOM.",
        relatedIssue     = "High Shuffle Fetch Wait",
        estimatedSavingsMs = Some(fetchSaveMs)))

      if (!props.get("spark.shuffle.service.enabled").contains("true")) {
        add(ConfigRecommendation(
          severity         = Info,
          configKey        = "spark.shuffle.service.enabled",
          currentValue     = props.getOrElse("spark.shuffle.service.enabled", "false"),
          recommendedValue = "true",
          title            = "Enable the external shuffle service",
          rationale        = "The external shuffle service serves shuffle blocks from a long-lived daemon " +
            "instead of the executors themselves, so fetches don't compete with executor GC/CPU and " +
            "survive executor loss. This reduces fetch wait and fetch failures under load. Requires " +
            "cluster-side setup (YARN/K8s auxiliary service).",
          relatedIssue     = "High Shuffle Fetch Wait"))
      }
    }

    // ── Remote shuffle blocks spilled to disk on fetch ───────────────────────────
    if (diskReads.nonEmpty) {
      add(ConfigRecommendation(
        severity         = Warning,
        configKey        = "spark.maxRemoteBlockSizeFetchToMem",
        currentValue     = props.getOrElse("spark.maxRemoteBlockSizeFetchToMem", "200m"),
        recommendedValue = "increase, or reduce partition size instead",
        title            = "Keep fetched shuffle blocks in memory",
        rationale        = "Remote shuffle blocks were large enough to be fetched straight to disk, adding " +
          "I/O latency. Raising spark.maxRemoteBlockSizeFetchToMem lets more blocks stay in memory, but " +
          "the more robust fix is smaller shuffle partitions (raise spark.sql.shuffle.partitions) or more " +
          "executor memory so individual blocks are smaller. Trade-off: larger in-memory fetches raise " +
          "memory/GC pressure.",
        relatedIssue     = "Disk Shuffle Read",
        estimatedSavingsMs = Some(diskReads.flatMap(_.estimatedSavingsMs).sum)))
    }

    // ── Task failures / stage retries usually mean executor memory loss ──────────
    if (failIssues.nonEmpty) {
      val failSaveMs = failIssues.flatMap(_.estimatedSavingsMs).sum
      val label =
        if (failures.nonEmpty && retries.nonEmpty) "Task Failures + Stage Retry"
        else if (failures.nonEmpty) "Task Failures" else "Stage Retry"
      add(ConfigRecommendation(
        severity         = Warning,
        configKey        = "spark.executor.memory",
        currentValue     = props.getOrElse("spark.executor.memory", "1g (cluster default)"),
        recommendedValue = "increase, and raise spark.executor.memoryOverhead",
        title            = "Increase executor memory to reduce failures/retries",
        rationale        = "Task failures and stage retries are most often caused by executors being " +
          "killed for exceeding memory (OOM / overhead limits), wasting all completed work in the stage. " +
          "More executor memory and a larger spark.executor.memoryOverhead reduce OOM kills. If failures " +
          "are not memory-related, investigate the exception before raising spark.task.maxFailures, which " +
          "only masks the symptom.",
        relatedIssue     = label,
        estimatedSavingsMs = if (failSaveMs > 0) Some(failSaveMs) else None))
    }

    recs.values.toSeq.sortBy(r => -ConfigRecommendation.rank(r.severity))
  }

  private def sparkProps(store: AppStatusStore): Map[String, String] =
    try store.environmentInfo().sparkProperties.toMap
    catch { case _: Throwable => Map.empty }

  private def worst(sevs: Seq[Severity]): Severity =
    if (sevs.isEmpty) Info else sevs.maxBy(ConfigRecommendation.rank)
}
