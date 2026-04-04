package org.apache.spark.ui.xpark

import com.xpark.XParkConfig
import org.apache.spark.SparkConf
import org.apache.spark.ui.{SparkUI, SparkUITab}

class XParkTab(val sparkUI: SparkUI, val config: XParkConfig)
    extends SparkUITab(sparkUI, "xpark") {

  override val name = "xpark"

  attachPage(new OverviewPage(this))
  attachPage(new SkewPage(this))
  attachPage(new GCPage(this))
  attachPage(new SpillPage(this))
  attachPage(new StragglerPage(this))
  attachPage(new BroadcastPage(this))
}

object XParkTab {
  def apply(ui: SparkUI, conf: SparkConf): XParkTab =
    new XParkTab(ui, XParkConfig.fromConf(conf))
}
