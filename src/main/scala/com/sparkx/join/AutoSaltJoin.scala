package com.sparkx.join

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.slf4j.LoggerFactory

/**
 * Configuration for [[AutoSaltJoin]].
 *
 * @param sampleFraction           Fraction of rows to sample for skew detection.
 * @param skewThresholdMultiplier  A key's estimated frequency must exceed
 *                                 `median × multiplier` to be considered hot.
 * @param minHotKeyCount           Absolute minimum estimated row count for a key
 *                                 to be considered hot (guards against noisy
 *                                 median on sparse samples).
 * @param saltFactor               Number of salt buckets per hot key. Hot-side
 *                                 rows get a random salt in `[0, saltFactor)`;
 *                                 the other side is replicated `saltFactor` times.
 * @param maxSkewedKeys            Cap on how many keys to salt. If more hot keys
 *                                 are detected, only the top-N by frequency are
 *                                 salted.
 * @param seed                     Random seed for reproducible salt assignment.
 * @param skewedSide               Which side to treat as skewed: `Auto` (detect),
 *                                 `Left`, or `Right`.
 * @param cacheResult              Whether to cache the final joined result.
 */
case class AutoSaltJoinConfig(
    sampleFraction: Double = 0.01,
    skewThresholdMultiplier: Double = 10.0,
    minHotKeyCount: Long = 100,
    saltFactor: Int = 10,
    maxSkewedKeys: Int = 1000,
    seed: Long = 42L,
    skewedSide: SkewedSide = SkewedSide.Auto,
    cacheResult: Boolean = true
) {
  require(sampleFraction > 0 && sampleFraction <= 1.0,
    "sampleFraction must be in (0, 1]")
  require(skewThresholdMultiplier > 1.0,
    "skewThresholdMultiplier must be > 1")
  require(saltFactor >= 2, "saltFactor must be >= 2")
}

/** Controls which side of the join is treated as the skewed (salted) side. */
sealed trait SkewedSide
object SkewedSide {
  /** Detect automatically by sampling both sides. */
  case object Auto extends SkewedSide
  /** Force left side as skewed. */
  case object Left extends SkewedSide
  /** Force right side as skewed. */
  case object Right extends SkewedSide
}

/**
 * Join strategy that detects skewed keys via sampling and applies
 * salt-based redistribution to eliminate join stragglers.
 *
 * Unlike Spark's built-in AQE skew join (which splits partitions at
 * runtime), AutoSaltJoin works at the logical level: it replicates the
 * non-skewed side for hot keys and randomises the skewed side across
 * `saltFactor` buckets, producing an even key distribution before the
 * join executes.
 *
 * ==Workflow==
 *  1. '''Sample''' both sides to estimate per-key frequencies.
 *  2. '''Detect hot keys''' — keys whose estimated count exceeds
 *     `median × skewThresholdMultiplier` and `minHotKeyCount`.
 *  3. '''Salt the skewed side''' — append a random salt column
 *     `[0, saltFactor)` for hot keys, `0` for cold keys.
 *  4. '''Replicate the other side''' — explode hot-key rows
 *     `saltFactor` times (one per salt value), cold keys get salt `0`.
 *  5. '''Join''' on `(original_keys :+ salt_column)`.
 *  6. '''Drop''' the salt column from the result.
 *
 * ==Usage==
 * {{{
 *   import com.sparkx.join.AutoSaltJoin._
 *
 *   val result = skewedDf.autoSaltJoin(otherDf, Seq("user_id"))
 *
 *   val result = skewedDf.autoSaltJoin(otherDf, Seq("user_id"), "inner",
 *     AutoSaltJoinConfig(saltFactor = 20, skewedSide = SkewedSide.Left))
 * }}}
 *
 * '''Supported join types:''' `inner`, `left_outer`, `right_outer`.
 */
object AutoSaltJoin {

  private val log = LoggerFactory.getLogger(getClass)

  private val SaltCol = "__sparkx_auto_salt"
  private val SupportedJoinTypes = Set("inner", "left_outer", "right_outer")

  implicit class AutoSaltJoinOps(private val left: DataFrame) extends AnyVal {

    /**
     * Join `left` with `right` using automatic skew detection and salting.
     *
     * @param right    The other DataFrame.
     * @param keys     Column names to join on.
     * @param joinType Spark join type (`inner`, `left_outer`, `right_outer`).
     * @param config   Salt join options.
     * @return The joined DataFrame (salt column removed).
     */
    def autoSaltJoin(right: DataFrame, keys: Seq[String],
                     joinType: String = "inner",
                     config: AutoSaltJoinConfig =
                       AutoSaltJoinConfig()): DataFrame = {
      AutoSaltJoin.execute(left, right, keys, joinType, config)
    }
  }

  /**
   * Execute an auto-salt join.
   */
  def execute(left: DataFrame, right: DataFrame,
              keys: Seq[String], joinType: String,
              config: AutoSaltJoinConfig): DataFrame = {
    require(SupportedJoinTypes.contains(joinType),
      s"AutoSaltJoin does not support join type '$joinType'. " +
      s"Supported: ${SupportedJoinTypes.mkString(", ")}")
    require(keys.nonEmpty, "keys must not be empty")

    // ── 1. Detect hot keys ──────────────────────────────────────────────
    val (hotKeysDf, skewSide) = detectSkew(left, right, keys, config)

    if (hotKeysDf.isEmpty) {
      log.info("AutoSaltJoin: no skewed keys detected, using regular join")
      val result = left.join(right, keys, joinType)
      if (config.cacheResult) result.cache()
      return result
    }

    val hotCount = hotKeysDf.count()
    log.info(s"AutoSaltJoin: detected $hotCount hot key(s) on $skewSide side, " +
      s"salting with factor ${config.saltFactor}")

    // ── 2. Determine which side to salt vs replicate ────────────────────
    val (skewed, other, isLeftSkewed) = skewSide match {
      case "left"  => (left, right, true)
      case "right" => (right, left, false)
      case _ => throw new IllegalStateException(s"Unexpected skew side: $skewSide")
    }

    // Broadcast the hot keys for efficient marking
    val hotKeysBc = broadcast(hotKeysDf)

    // ── 3. Salt the skewed side ─────────────────────────────────────────
    val saltedSkewed = saltSide(skewed, keys, hotKeysBc, config)

    // ── 4. Replicate the other side ─────────────────────────────────────
    val replicatedOther = replicateSide(other, keys, hotKeysBc, config)

    // ── 5. Join on keys + salt ──────────────────────────────────────────
    val joinKeys = keys :+ SaltCol
    val joined = if (isLeftSkewed)
      saltedSkewed.join(replicatedOther, joinKeys, joinType)
    else
      replicatedOther.join(saltedSkewed, joinKeys, joinType)

    // ── 6. Drop salt column ─────────────────────────────────────────────
    val result = joined.drop(SaltCol)

    if (config.cacheResult) result.cache()
    result
  }

  /**
   * Detect skewed keys by sampling both sides.
   *
   * @return (hotKeysDf, "left" or "right") — the hot keys DataFrame contains
   *         the key columns of the skewed side. Returns empty DataFrame if
   *         no skew is detected.
   */
  private[join] def detectSkew(left: DataFrame, right: DataFrame,
                                keys: Seq[String],
                                config: AutoSaltJoinConfig
                               ): (DataFrame, String) = {
    config.skewedSide match {
      case SkewedSide.Left =>
        (findHotKeys(left, keys, config), "left")
      case SkewedSide.Right =>
        (findHotKeys(right, keys, config), "right")
      case SkewedSide.Auto =>
        val leftHot  = findHotKeys(left, keys, config)
        val rightHot = findHotKeys(right, keys, config)
        val leftCount  = leftHot.count()
        val rightCount = rightHot.count()

        if (leftCount == 0 && rightCount == 0) {
          (leftHot, "left") // empty — no skew
        } else if (leftCount >= rightCount) {
          // Left has more (or equal) hot keys → left is the skewed side
          log.info(s"AutoSaltJoin: auto-detected left side as skewed " +
            s"($leftCount hot keys vs $rightCount on right)")
          (leftHot, "left")
        } else {
          log.info(s"AutoSaltJoin: auto-detected right side as skewed " +
            s"($rightCount hot keys vs $leftCount on left)")
          (rightHot, "right")
        }
    }
  }

  /**
   * Find hot keys in a DataFrame by sampling and comparing frequencies
   * to the median.
   *
   * @return A DataFrame containing only the key columns of hot keys,
   *         limited to `maxSkewedKeys` rows, ordered by frequency desc.
   */
  private[join] def findHotKeys(df: DataFrame, keys: Seq[String],
                                 config: AutoSaltJoinConfig): DataFrame = {
    val keyCols = keys.map(col)
    val sampled = df.sample(config.sampleFraction, seed = config.seed)

    // Count per key group (using struct avoids string-concat collisions)
    val freqs = sampled
      .filter(keyCols.map(_.isNotNull).reduce(_ && _))  // skip nulls
      .groupBy(keyCols: _*)
      .agg(count("*").as("_freq"))

    // Compute median frequency
    val medianRow = freqs
      .stat.approxQuantile("_freq", Array(0.5), 0.05)
    val median = if (medianRow.nonEmpty) medianRow(0) else 1.0

    // Scale threshold: account for sampling fraction
    val scaledMinCount = (config.minHotKeyCount * config.sampleFraction).toLong
      .max(1L)
    val threshold = math.max(median * config.skewThresholdMultiplier, scaledMinCount)

    log.info(s"AutoSaltJoin: sample median freq=$median, " +
      s"threshold=$threshold (multiplier=${config.skewThresholdMultiplier})")

    freqs
      .filter(col("_freq") >= lit(threshold))
      .orderBy(col("_freq").desc)
      .limit(config.maxSkewedKeys)
      .select(keyCols: _*)
  }

  /**
   * Add a random salt column to the skewed side.
   * Hot-key rows get `floor(rand(seed) * saltFactor)`;
   * cold-key rows get `0`.
   */
  private def saltSide(df: DataFrame, keys: Seq[String],
                       hotKeysBc: DataFrame,
                       config: AutoSaltJoinConfig): DataFrame = {
    // Mark hot rows via left_semi join
    val isHot = df.join(hotKeysBc, keys, "left_semi")
      .withColumn(SaltCol,
        floor(rand(config.seed) * config.saltFactor).cast(IntegerType))

    val isCold = df.join(hotKeysBc, keys, "left_anti")
      .withColumn(SaltCol, lit(0).cast(IntegerType))

    // Handle null keys — always cold
    val keyCols = keys.map(col)
    val hasNull = keyCols.map(_.isNull).reduce(_ || _)
    val nullRows = df.filter(hasNull)
      .withColumn(SaltCol, lit(0).cast(IntegerType))

    val nonNullDf = isHot.unionByName(isCold)

    if (nullRows.isEmpty) nonNullDf
    else nonNullDf.unionByName(nullRows)
  }

  /**
   * Replicate hot-key rows on the other side `saltFactor` times,
   * one per salt bucket. Cold-key rows get salt `0`.
   */
  private def replicateSide(df: DataFrame, keys: Seq[String],
                            hotKeysBc: DataFrame,
                            config: AutoSaltJoinConfig): DataFrame = {
    val spark = df.sparkSession

    // Hot rows: explode with salt values [0, saltFactor)
    val hotRows = df.join(hotKeysBc, keys, "left_semi")
    val saltArray = array((0 until config.saltFactor).map(lit(_)): _*)
    val replicatedHot = hotRows
      .withColumn(SaltCol, explode(saltArray))
      .withColumn(SaltCol, col(SaltCol).cast(IntegerType))

    // Cold rows: salt = 0
    val coldRows = df.join(hotKeysBc, keys, "left_anti")
      .withColumn(SaltCol, lit(0).cast(IntegerType))

    // Null key rows: salt = 0
    val keyCols = keys.map(col)
    val hasNull = keyCols.map(_.isNull).reduce(_ || _)
    val nullRows = df.filter(hasNull)
      .withColumn(SaltCol, lit(0).cast(IntegerType))

    val nonNullDf = replicatedHot.unionByName(coldRows)

    if (nullRows.isEmpty) nonNullDf
    else nonNullDf.unionByName(nullRows)
  }
}
