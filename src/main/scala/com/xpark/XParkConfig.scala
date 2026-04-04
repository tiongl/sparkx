package com.xpark

import org.apache.spark.SparkConf

case class XParkConfig(
  skewMultiplier:     Double,  // max/median > N => data skew
  gcRatioThreshold:   Double,  // gcTime/runTime > N => GC pressure
  stragglerIQRFactor: Double,  // p75 + N*IQR => straggler upper fence
  broadcastSizeMB:    Long     // broadcast size warning threshold in MB
)

object XParkConfig {
  val SKEW_MULTIPLIER      = "spark.xpark.skewMultiplier"
  val GC_RATIO_THRESHOLD   = "spark.xpark.gcRatioThreshold"
  val STRAGGLER_IQR_FACTOR = "spark.xpark.stragglerIQRFactor"
  val BROADCAST_SIZE_MB    = "spark.xpark.broadcastSizeMB"

  def fromConf(conf: SparkConf): XParkConfig = XParkConfig(
    skewMultiplier     = conf.getDouble(SKEW_MULTIPLIER, 3.0),
    gcRatioThreshold   = conf.getDouble(GC_RATIO_THRESHOLD, 0.10),
    stragglerIQRFactor = conf.getDouble(STRAGGLER_IQR_FACTOR, 1.5),
    broadcastSizeMB    = conf.getLong(BROADCAST_SIZE_MB, 200L)
  )
}
