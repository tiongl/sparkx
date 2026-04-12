package org.apache.spark.ui.sparkx

import com.sparkx.SparkXConfig
import org.apache.spark.SparkConf
import org.apache.spark.ui.{SparkUI, SparkUITab}

class SparkXTab(val sparkUI: SparkUI, val config: SparkXConfig)
    extends SparkUITab(sparkUI, "sparkx") {

  override val name = "sparkx"

  attachPage(new OverviewPage(this))
  attachPage(new RootCausePage(this))
  attachPage(new StagesSummaryPage(this))
  attachPage(new SkewPage(this))
  attachPage(new GCPage(this))
  attachPage(new SpillPage(this))
  attachPage(new StragglerPage(this))
  attachPage(new BroadcastPage(this))
  attachPage(new PartitioningPage(this))
  attachPage(new StabilityPage(this))
}

object SparkXTab {
  def apply(ui: SparkUI, conf: SparkConf): SparkXTab =
    new SparkXTab(ui, SparkXConfig.fromConf(conf))
}
