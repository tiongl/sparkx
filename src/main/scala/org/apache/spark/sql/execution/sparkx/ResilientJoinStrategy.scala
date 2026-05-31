package org.apache.spark.sql.execution.sparkx

import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * A [[Strategy]] that intercepts equi-join logical nodes and produces
 * a [[ResilientJoinExec]] physical node when
 * `spark.sparkx.resilientJoin.enabled` is `true`.
 *
 * Register via:
 * {{{
 *   spark.experimental.extraStrategies ++= Seq(new ResilientJoinStrategy())
 * }}}
 *
 * The strategy only matches equi-joins (joins with equality-based key
 * conditions). Non-equi joins, cross joins without keys, and cartesian
 * products fall through to Spark's default strategies.
 */
class ResilientJoinStrategy extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, nonEquiCond,
                             _, left, right, _) =>
      val sqlConf = SQLConf.get
      val enabled = ResilientJoinConf.getBoolean(sqlConf,
        ResilientJoinConf.ENABLED, ResilientJoinConf.ENABLED_DEFAULT)

      if (!enabled) {
        Nil
      } else {
        ResilientJoinExec(
          leftKeys, rightKeys, joinType, nonEquiCond,
          planLater(left), planLater(right)
        ) :: Nil
      }

    case _ => Nil
  }
}
