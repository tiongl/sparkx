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
    org.apache.spark.ui.sparkx.SparkXPluginBridge.registerForSparkContext(
      org.apache.spark.SparkContext.getOrCreate(), conf)
  }
}
