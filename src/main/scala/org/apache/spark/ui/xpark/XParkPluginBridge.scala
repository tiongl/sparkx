package org.apache.spark.ui.xpark

import com.xpark.XParkConfig
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ui.SparkUI

/** Bridge object exposing private[spark] methods to com.xpark classes. */
object XParkPluginBridge {

  def setupHistoryUI(ui: SparkUI): Unit =
    ui.attachTab(XParkTab(ui, ui.conf))

  def registerForSparkContext(sc: SparkContext, conf: SparkConf): Unit =
    sc.ui.foreach(ui => ui.attachTab(XParkTab(ui, conf)))
}
