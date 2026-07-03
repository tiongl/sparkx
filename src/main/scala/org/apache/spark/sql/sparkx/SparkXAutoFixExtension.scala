package org.apache.spark.sql.sparkx

import com.sparkx.SparkXConfig
import com.sparkx.autofix.FixProfileStore
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/**
 * The `spark.sql.extensions` entry point for auto-fix. It injects [[AutoFixRule]] as a
 * post-hoc resolution rule (so learned hints are applied to *any* resolved plan — SQL text
 * or DataFrame/Dataset API alike), injects [[SparkXHintRule]] as a resolution rule (so the
 * sparkx skew pseudo-hints can be written directly in SQL text), and lazily registers an
 * [[AutoFixLearner]] on the session (so completed queries feed the learning loop).
 *
 * Enable with:
 * {{{
 *   --conf spark.sql.extensions=org.apache.spark.sql.sparkx.SparkXAutoFixExtension
 *   --conf spark.sparkx.autofix.enabled=true
 * }}}
 */
class SparkXAutoFixExtension extends (SparkSessionExtensions => Unit) {
  override def apply(ext: SparkSessionExtensions): Unit = {
    // Resolve sparkx SQL pseudo-hints (SPLIT_BROADCAST / SALT) written in query text.
    ext.injectResolutionRule(_ => new SparkXHintRule)
    ext.injectPostHocResolutionRule { session =>
      val config = SparkXConfig.fromConf(session.sparkContext.getConf)
      val store  = new FixProfileStore(config.autofixStorePath, session.sparkContext.hadoopConfiguration)
      SparkXAutoFixExtension.ensureLearner(session, config, store)
      new AutoFixRule(config, store)
    }
  }
}

object SparkXAutoFixExtension {
  private val registered =
    java.util.Collections.synchronizedSet(new java.util.HashSet[String]())

  /** Register the learner listener at most once per Spark session. */
  private def ensureLearner(session: SparkSession, config: SparkXConfig, store: FixProfileStore): Unit = {
    if (!config.autofixLearnEnabled) return
    if (registered.add(session.sessionUUID)) {
      session.listenerManager.register(new AutoFixLearner(config, store))
    }
  }
}
