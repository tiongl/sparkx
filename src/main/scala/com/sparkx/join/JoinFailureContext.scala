package com.sparkx.join

/**
 * Context passed to the next join strategy after a previous one has failed.
 *
 * Strategies use this to adapt their behaviour — for example, a broadcast
 * strategy will decline to run if the previous failure was an OOM from a
 * broadcast attempt, and a repartition strategy may increase partition
 * count after a shuffle-related failure.
 *
 * @param failedStrategy  Name of the strategy that failed (e.g. "broadcast").
 * @param exception       The exception that caused the failure.
 * @param attempt         1-based attempt number within the fallback chain.
 * @param leftSizeBytes   Estimated left DataFrame size (from plan stats), if available.
 * @param rightSizeBytes  Estimated right DataFrame size (from plan stats), if available.
 */
case class JoinFailureContext(
    failedStrategy: String,
    exception: Throwable,
    attempt: Int,
    leftSizeBytes: Option[Long] = None,
    rightSizeBytes: Option[Long] = None
)
