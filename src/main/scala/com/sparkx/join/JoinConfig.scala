package com.sparkx.join

/**
 * Configuration for [[ResilientJoin]].
 *
 * @param strategies            Ordered fallback chain of join strategies.
 * @param broadcastThresholdBytes  Size threshold (bytes) for auto-broadcast
 *                                 eligibility in [[BroadcastJoinStrategy]].
 * @param checkpointOnSuccess   Whether to cache the result after a
 *                              successful join.
 * @param maxAttempts           Maximum number of strategies to try before
 *                              throwing [[ResilientJoinExhaustedException]].
 */
case class JoinConfig(
    strategies: Seq[JoinStrategy] = JoinConfig.defaultStrategies,
    broadcastThresholdBytes: Long = 100L * 1024 * 1024,
    checkpointOnSuccess: Boolean = true,
    maxAttempts: Int = 3
)

object JoinConfig {
  val defaultStrategies: Seq[JoinStrategy] = Seq(
    new BroadcastJoinStrategy(),
    new SortMergeJoinStrategy(),
    new RepartitionJoinStrategy()
  )

  val default: JoinConfig = JoinConfig()
}
