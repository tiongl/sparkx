package com.xpark

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart}

/**
 * Spark listener that registers the xpark UI tab when an application starts.
 *
 * Integrate by adding to spark-submit:
 *   --conf spark.extraListeners=com.xpark.XParkListener
 *
 * All xpark.* configuration keys are read from SparkConf at startup.
 */
class XParkListener(conf: SparkConf) extends SparkListener {

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    org.apache.spark.ui.xpark.XParkPluginBridge.registerForSparkContext(
      org.apache.spark.SparkContext.getOrCreate(), conf)
  }
}
