package org.apache.spark.ui.xpark

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.status.{AppHistoryServerPlugin, ElementTrackingStore}
import org.apache.spark.ui.SparkUI

/**
 * Spark History Server plugin that adds xpark performance analysis tabs to
 * replayed application UIs.
 *
 * Registered automatically via Java SPI:
 *   META-INF/services/org.apache.spark.status.AppHistoryServerPlugin
 *
 * Users only need to copy the xpark JAR to $SPARK_HOME/jars/ and restart the
 * History Server — no other configuration is required.
 */
class XParkHistoryPlugin extends AppHistoryServerPlugin {

  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] =
    Seq.empty

  override def setupUI(ui: SparkUI): Unit =
    ui.attachTab(XParkTab(ui, ui.conf))
}
