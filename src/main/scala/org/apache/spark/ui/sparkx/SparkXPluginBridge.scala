package org.apache.spark.ui.sparkx

import com.sparkx.SparkXConfig
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ui.SparkUI

/** Bridge object exposing private[spark] methods to com.sparkx classes. */
object SparkXPluginBridge {

  def setupHistoryUI(ui: SparkUI): Unit =
    ui.attachTab(SparkXTab(ui, ui.conf))

  def registerForSparkContext(sc: SparkContext, conf: SparkConf): Unit =
    sc.ui.foreach(ui => ui.attachTab(SparkXTab(ui, conf)))
}
