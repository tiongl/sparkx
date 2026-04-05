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
  highDeserMs:             Long     // P95 task deserialize time > N ms
)

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
    highDeserMs             = conf.getLong(HIGH_DESER_MS, 200L)
  )
}
