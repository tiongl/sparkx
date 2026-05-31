package com.sparkx

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart}

/**
 * Spark listener that registers the sparkx UI tab when an application starts.
 *
 * Integrate by adding to spark-submit:
 *   --conf spark.extraListeners=com.sparkx.SparkXListener
 *
 * All sparkx.* configuration keys are read from SparkConf at startup.
 */
class SparkXListener(conf: SparkConf) extends SparkListener {

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    val sc = org.apache.spark.SparkContext.getOrCreate()

    // Register UI tab
    org.apache.spark.ui.sparkx.SparkXPluginBridge.registerForSparkContext(sc, conf)

    // Register resilient join strategy if enabled
    val resilientEnabled = conf.getBoolean(
      "spark.sparkx.resilientJoin.enabled", defaultValue = false)
    if (resilientEnabled) {
      val spark = org.apache.spark.sql.SparkSession.builder().getOrCreate()
      spark.experimental.extraStrategies ++=
        Seq(new org.apache.spark.sql.execution.sparkx.ResilientJoinStrategy())
    }
  }
}
