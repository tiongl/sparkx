package org.apache.spark.ui.sparkx

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.status.{AppHistoryServerPlugin, ElementTrackingStore}
import org.apache.spark.ui.SparkUI

/**
 * Spark History Server plugin that adds sparkx performance analysis tabs to
 * replayed application UIs.
 *
 * Registered automatically via Java SPI:
 *   META-INF/services/org.apache.spark.status.AppHistoryServerPlugin
 *
 * Users only need to copy the sparkx JAR to $SPARK_HOME/jars/ and restart the
 * History Server — no other configuration is required.
 */
class SparkXHistoryPlugin extends AppHistoryServerPlugin {

  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] =
    Seq.empty

  override def setupUI(ui: SparkUI): Unit =
    ui.attachTab(SparkXTab(ui, ui.conf))
}
