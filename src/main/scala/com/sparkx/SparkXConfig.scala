package com.sparkx

import org.apache.spark.SparkConf

case class SparkXConfig(
  // Existing detections
  skewMultiplier:          Double,  // max/median > N => data skew
  gcRatioThreshold:        Double,  // gcTime/runTime > N => GC pressure
  stragglerIQRFactor:      Double,  // p75 + N*IQR => straggler upper fence
  broadcastSizeMB:         Long,    // broadcast size warning threshold in MB
  // Partitioning detections
  smallTaskMedianMs:       Long,    // median task < N ms AND many tasks => over-partitioned
  smallTaskMinCount:       Int,     // minimum task count before flagging small tasks
  underPartitionRatio:     Double,  // numTasks < activeCores * ratio => under-partitioned
  shuffleAmplifyRatio:     Double,  // shuffleWriteBytes > inputBytes * ratio => amplification
  // Stability / task-health detections
  largeResultMB:           Long,    // P95 task result > N MB => large result
  fetchWaitRatioThreshold: Double,  // fetchWaitTime / runTime > N => network bottleneck
  highDeserMs:             Long,    // P95 task deserialize time > N ms
  // Phase 2 detections
  lowCpuRatioThreshold:    Double,  // cpuTime / runTime < N => low CPU utilization
  lowCpuMinRunTimeMs:      Long,    // minimum stage executor run time before flagging low CPU
  diskShuffleReadMinMB:    Long,    // minimum remoteBytesReadToDisk before flagging
  highSchedulerDelayMs:    Long,    // P95 scheduler delay > N ms
  schedulerDelayRatio:     Double,  // P95 scheduler delay / P50 run time > N
  resultSerializationMs:   Long,    // P95 result serialization time > N ms
  executorMemoryCov:       Double,  // coefficient of variation threshold for memory skew
  executorMemoryMinCount:  Int,     // minimum executor count before flagging memory skew
  // Suggestion detections
  broadcastThresholdBytes: Long,    // shuffle side < N bytes => broadcast candidate
  excessiveShuffleCount:   Int,     // > N exchange nodes in one execution => flag
  partitionPruneScanMinMB: Long,    // only flag missing partition pruning if scan > N MB
  collectLargeDataMinMB:   Long,    // flag collect when stage input > N MB
  // Auto-fix (closed-loop SQL hint injection)
  autofixEnabled:              Boolean, // master on/off switch for auto-fix
  autofixMode:                 String,  // "auto" (learn+fix) | "learn" | "shadow" | "fix"
  autofixStorePath:            String,  // where fix profiles are persisted
  autofixMaxIterations:        Int,     // tuning attempts before locking in the best hints
  autofixBroadcastMaxBytes:    Long,    // join side below this => try a BROADCAST hint
  autofixTargetPartitionBytes: Long,    // desired bytes per shuffle partition when tuning
  // Skew resolution (advisory: surfaced as DataFrame-API recommendations, not plan-injected)
  autofixSkewFactor:           Double,  // join is skew-prone when max(side)/min(side) >= this (<=1 forces all joins)
  autofixSkewBroadcastMaxBytes: Long,   // artificial ceiling: small side below this => "double broadcast" (split-broadcast) instead of salting
  autofixSaltFactor:           Int,     // salt buckets recommended for a salted (AutoSaltJoin) skew fix
  autofixSkewTargeted:         Boolean, // discover hot keys (sampling) and salt only those (targeted salting)
  autofixSkewSampleFraction:   Double,  // fraction of the skewed side to sample when discovering hot keys
  autofixSkewThresholdMult:    Double,  // a key is hot when its sampled freq >= median * this
  autofixSkewMaxKeys:          Int      // cap on how many hot keys to salt
) {
  def autofixLearnEnabled: Boolean = autofixEnabled
  def autofixApplyEnabled: Boolean = autofixEnabled && (autofixMode == "auto" || autofixMode == "fix")
  def autofixShadowEnabled: Boolean = autofixEnabled && autofixMode == "shadow"
}

object SparkXConfig {
  val SKEW_MULTIPLIER          = "spark.sparkx.skewMultiplier"
  val GC_RATIO_THRESHOLD       = "spark.sparkx.gcRatioThreshold"
  val STRAGGLER_IQR_FACTOR     = "spark.sparkx.stragglerIQRFactor"
  val BROADCAST_SIZE_MB        = "spark.sparkx.broadcastSizeMB"
  val SMALL_TASK_MEDIAN_MS     = "spark.sparkx.smallTaskMedianMs"
  val SMALL_TASK_MIN_COUNT     = "spark.sparkx.smallTaskMinCount"
  val UNDER_PARTITION_RATIO    = "spark.sparkx.underPartitionRatio"
  val SHUFFLE_AMPLIFY_RATIO    = "spark.sparkx.shuffleAmplifyRatio"
  val LARGE_RESULT_MB          = "spark.sparkx.largeResultMB"
  val FETCH_WAIT_RATIO         = "spark.sparkx.fetchWaitRatioThreshold"
  val HIGH_DESER_MS            = "spark.sparkx.highDeserMs"
  val LOW_CPU_RATIO            = "spark.sparkx.lowCpuRatioThreshold"
  val LOW_CPU_MIN_RUNTIME_MS   = "spark.sparkx.lowCpuMinRunTimeMs"
  val DISK_SHUFFLE_READ_MIN_MB = "spark.sparkx.diskShuffleReadMinMB"
  val HIGH_SCHEDULER_DELAY_MS  = "spark.sparkx.highSchedulerDelayMs"
  val SCHEDULER_DELAY_RATIO    = "spark.sparkx.schedulerDelayRatio"
  val RESULT_SERIALIZATION_MS  = "spark.sparkx.resultSerializationMs"
  val EXECUTOR_MEMORY_COV      = "spark.sparkx.executorMemoryCov"
  val EXECUTOR_MEMORY_MIN_COUNT = "spark.sparkx.executorMemoryMinCount"
  // Suggestion thresholds
  val BROADCAST_THRESHOLD_BYTES = "spark.sparkx.suggestion.broadcastThresholdBytes"
  val EXCESSIVE_SHUFFLE_COUNT   = "spark.sparkx.suggestion.excessiveShuffleCount"
  val PARTITION_PRUNE_SCAN_MIN_MB = "spark.sparkx.suggestion.partitionPruneScanMinMB"
  val COLLECT_LARGE_DATA_MIN_MB  = "spark.sparkx.suggestion.collectLargeDataMinMB"
  // Auto-fix keys
  val AUTOFIX_ENABLED               = "spark.sparkx.autofix.enabled"
  val AUTOFIX_MODE                  = "spark.sparkx.autofix.mode"
  val AUTOFIX_STORE_PATH            = "spark.sparkx.autofix.store.path"
  val AUTOFIX_MAX_ITERATIONS        = "spark.sparkx.autofix.maxIterations"
  val AUTOFIX_BROADCAST_MAX_BYTES   = "spark.sparkx.autofix.broadcastMaxBytes"
  val AUTOFIX_TARGET_PARTITION_BYTES = "spark.sparkx.autofix.targetPartitionBytes"
  val AUTOFIX_SKEW_FACTOR            = "spark.sparkx.autofix.skew.factor"
  val AUTOFIX_SKEW_BROADCAST_MAX_BYTES = "spark.sparkx.autofix.skew.broadcastMaxBytes"
  val AUTOFIX_SALT_FACTOR           = "spark.sparkx.autofix.skew.saltFactor"
  val AUTOFIX_SKEW_TARGETED         = "spark.sparkx.autofix.skew.targeted"
  val AUTOFIX_SKEW_SAMPLE_FRACTION  = "spark.sparkx.autofix.skew.sampleFraction"
  val AUTOFIX_SKEW_THRESHOLD_MULT   = "spark.sparkx.autofix.skew.thresholdMultiplier"
  val AUTOFIX_SKEW_MAX_KEYS         = "spark.sparkx.autofix.skew.maxKeys"

  private def defaultStorePath: String =
    new java.io.File(System.getProperty("java.io.tmpdir"), "sparkx-autofix").toURI.toString

  def fromConf(conf: SparkConf): SparkXConfig = SparkXConfig(
    skewMultiplier          = conf.getDouble(SKEW_MULTIPLIER, 3.0),
    gcRatioThreshold        = conf.getDouble(GC_RATIO_THRESHOLD, 0.10),
    stragglerIQRFactor      = conf.getDouble(STRAGGLER_IQR_FACTOR, 1.5),
    broadcastSizeMB         = conf.getLong(BROADCAST_SIZE_MB, 200L),
    smallTaskMedianMs       = conf.getLong(SMALL_TASK_MEDIAN_MS, 200L),
    smallTaskMinCount       = conf.getInt(SMALL_TASK_MIN_COUNT, 100),
    underPartitionRatio     = conf.getDouble(UNDER_PARTITION_RATIO, 0.5),
    shuffleAmplifyRatio     = conf.getDouble(SHUFFLE_AMPLIFY_RATIO, 5.0),
    largeResultMB           = conf.getLong(LARGE_RESULT_MB, 50L),
    fetchWaitRatioThreshold = conf.getDouble(FETCH_WAIT_RATIO, 0.2),
    highDeserMs             = conf.getLong(HIGH_DESER_MS, 200L),
    lowCpuRatioThreshold    = conf.getDouble(LOW_CPU_RATIO, 0.5),
    lowCpuMinRunTimeMs      = conf.getLong(LOW_CPU_MIN_RUNTIME_MS, 60000L),
    diskShuffleReadMinMB    = conf.getLong(DISK_SHUFFLE_READ_MIN_MB, 100L),
    highSchedulerDelayMs    = conf.getLong(HIGH_SCHEDULER_DELAY_MS, 500L),
    schedulerDelayRatio     = conf.getDouble(SCHEDULER_DELAY_RATIO, 0.5),
    resultSerializationMs   = conf.getLong(RESULT_SERIALIZATION_MS, 200L),
    executorMemoryCov       = conf.getDouble(EXECUTOR_MEMORY_COV, 0.5),
    executorMemoryMinCount  = conf.getInt(EXECUTOR_MEMORY_MIN_COUNT, 3),
    broadcastThresholdBytes = conf.getLong(BROADCAST_THRESHOLD_BYTES, 100L * 1024 * 1024),
    excessiveShuffleCount   = conf.getInt(EXCESSIVE_SHUFFLE_COUNT, 4),
    partitionPruneScanMinMB = conf.getLong(PARTITION_PRUNE_SCAN_MIN_MB, 1024L),
    collectLargeDataMinMB   = conf.getLong(COLLECT_LARGE_DATA_MIN_MB, 100L),
    autofixEnabled              = conf.getBoolean(AUTOFIX_ENABLED, defaultValue = false),
    autofixMode                 = conf.get(AUTOFIX_MODE, "auto"),
    autofixStorePath            = conf.get(AUTOFIX_STORE_PATH, defaultStorePath),
    autofixMaxIterations        = conf.getInt(AUTOFIX_MAX_ITERATIONS, 5),
    autofixBroadcastMaxBytes    = conf.getLong(AUTOFIX_BROADCAST_MAX_BYTES, 10L * 1024 * 1024),
    autofixTargetPartitionBytes = conf.getLong(AUTOFIX_TARGET_PARTITION_BYTES, 128L * 1024 * 1024),
    autofixSkewFactor            = conf.getDouble(AUTOFIX_SKEW_FACTOR, 10.0),
    autofixSkewBroadcastMaxBytes = conf.getLong(AUTOFIX_SKEW_BROADCAST_MAX_BYTES, 100L * 1024 * 1024),
    autofixSaltFactor            = conf.getInt(AUTOFIX_SALT_FACTOR, 16),
    autofixSkewTargeted          = conf.getBoolean(AUTOFIX_SKEW_TARGETED, defaultValue = true),
    autofixSkewSampleFraction    = conf.getDouble(AUTOFIX_SKEW_SAMPLE_FRACTION, 0.01),
    autofixSkewThresholdMult     = conf.getDouble(AUTOFIX_SKEW_THRESHOLD_MULT, 10.0),
    autofixSkewMaxKeys           = conf.getInt(AUTOFIX_SKEW_MAX_KEYS, 100)
  )
}
