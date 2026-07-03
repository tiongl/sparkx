package org.apache.spark.sql.sparkx

import com.sparkx.autofix._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, Cast, CreateArray, EqualTo, Explode, Expression, If, In, Literal, Multiply, Murmur3Hash, Pmod, Rand}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.IntegerType

/**
 * Plan-level equivalents of SparkSQL hints. This is the heart of the unified (SQL + DataFrame)
 * auto-fix path: instead of rewriting SQL text, it injects the corresponding catalyst nodes
 * ([[Repartition]], [[RebalancePartitions]], join [[JoinHint]]s) directly into a resolved
 * [[LogicalPlan]], tags them as auto-fix-injected, and can later strip/detect them again.
 *
 * The tags let us (a) stay idempotent across analyzer fixed-point passes, (b) recover exactly
 * which hints were applied for a run, and (c) compute a *stable* fingerprint by stripping our
 * own nodes so a hinted plan and its un-hinted original share one identity.
 */
object PlanHints {

  /** Marks nodes that auto-fix injected, so they can be recognised and removed later. */
  val Injected = TreeNodeTag[Boolean]("sparkx.autofix.injected")

  /**
   * Records the auto-fix fingerprint on an injected plan root. Complex skew rewrites
   * (split-broadcast, salting) are not structurally reversible by [[strip]], so the fix
   * side stamps identity directly and the learn side reads it back (see [[identityFrom]]).
   */
  val Fingerprint = TreeNodeTag[String]("sparkx.autofix.fingerprint")

  /** Records the `;`-joined rendered hints that were applied to a plan (see [[Fingerprint]]). */
  val AppliedHints = TreeNodeTag[String]("sparkx.autofix.appliedHints")

  /** Fixed seed used by the salted-join rewrite (combined with the partition index at runtime). */
  private val SaltSeed = 0L
  private val SkewSaltCol = "__sparkx_skew_salt"
  private val SaltRangeCol = "__sparkx_salt_range"

  /** True if the plan already carries auto-fix-injected nodes (used for idempotency). */
  def hasInjected(plan: LogicalPlan): Boolean =
    plan.find(_.getTagValue(Injected).contains(true)).isDefined

  /** Stamp fingerprint + applied-hint identity on an injected plan root. */
  def stampIdentity(plan: LogicalPlan, fingerprint: String, hints: Seq[Hint]): Unit = {
    plan.setTagValue(Fingerprint, fingerprint)
    plan.setTagValue(AppliedHints, hints.map(_.render).mkString(";"))
  }

  /**
   * Recover a stamped `(fingerprint, appliedHints)` identity from anywhere in the tree, if the
   * fix side stamped one. Preferred over structural [[strip]] for hinted runs because complex
   * skew rewrites can't be reversed structurally.
   */
  def identityFrom(plan: LogicalPlan): Option[(String, Seq[Hint])] =
    plan.find(_.getTagValue(Fingerprint).isDefined).map { p =>
      val fp = p.getTagValue(Fingerprint).get
      val hints = p.getTagValue(AppliedHints).toList
        .flatMap(_.split(";")).map(_.trim).filter(_.nonEmpty).flatMap(Hint.parse)
      (fp, hints)
    }

  // ── Injection ────────────────────────────────────────────────────────────────

  /** Apply the given hints to a resolved plan, tagging every injected node. */
  def apply(plan: LogicalPlan, hints: Seq[Hint]): LogicalPlan = {
    var p = plan
    hints.foreach {
      case BroadcastHint(tables)      => tables.foreach(t => p = applyBroadcast(p, t))
      case SplitBroadcastHint(t, n)   => p = applySplitBroadcast(p, t, n)
      case SaltedJoinHint(t, n)       => p = applySalt(p, t, n)
      case TargetedSaltHint(t, n, ks) => p = applyTargetedSalt(p, t, n, ks)
      case _                          => // partitioning handled below
    }
    partitioningNode(p, hints).foreach { node =>
      node.setTagValue(Injected, value = true)
      p = node
    }
    p
  }

  private def partitioningNode(child: LogicalPlan, hints: Seq[Hint]): Option[LogicalPlan] =
    hints.collectFirst {
      case RepartitionHint(n) => Repartition(n, shuffle = true, child)
      case CoalesceHint(n)    => Repartition(n, shuffle = false, child)
      case RebalanceHint      => RebalancePartitions(Nil, child)
    }

  private def applyBroadcast(plan: LogicalPlan, table: String): LogicalPlan = {
    var done = false
    plan.transformUp {
      case j: Join if !done && j.hint == JoinHint.NONE =>
        val hinted =
          if (containsRelation(j.left, table))
            Some(j.copy(hint = JoinHint(Some(HintInfo(Some(BROADCAST))), None)))
          else if (containsRelation(j.right, table))
            Some(j.copy(hint = JoinHint(None, Some(HintInfo(Some(BROADCAST))))))
          else None
        hinted match {
          case Some(nj) => done = true; nj.setTagValue(Injected, value = true); nj
          case None     => j
        }
    }
  }

  // ── Skew resolution: split (N-way) broadcast ──────────────────────────────────

  /**
   * "Double / N-way broadcast": split the build side `table` into `splits` hash-disjoint chunks
   * (partitioned by `pmod(murmur3(joinKeys), splits)`), broadcast each chunk against the large
   * side, and union the partials. Correct for inner joins (either side) and left-semi joins
   * (build on the right only); other shapes are left untouched.
   */
  private def applySplitBroadcast(plan: LogicalPlan, table: String, splits: Int): LogicalPlan = {
    if (splits < 2) return plan
    var done = false
    plan.transformUp {
      case j: Join if !done && j.condition.isDefined && j.hint == JoinHint.NONE =>
        val cond = j.condition.get
        val buildOnLeft  = containsRelation(j.left, table)
        val buildOnRight = containsRelation(j.right, table)
        val supported = j.joinType match {
          case Inner    => buildOnLeft || buildOnRight
          case LeftSemi => buildOnRight
          case _        => false
        }
        if (!supported) j
        else {
          val buildIsRight = buildOnRight
          val build = if (buildIsRight) j.right else j.left
          val keys  = sideKeys(cond, build)
          if (keys.isEmpty) j
          else {
            done = true
            val partials = (0 until splits).map { i =>
              val bucket = EqualTo(Pmod(Murmur3Hash(keys, 42), Literal(splits)), Literal(i))
              val chunk  = Filter(bucket, build)
              if (buildIsRight)
                Join(j.left, chunk, j.joinType, Some(cond),
                  JoinHint(None, Some(HintInfo(Some(BROADCAST)))))
              else
                Join(chunk, j.right, j.joinType, Some(cond),
                  JoinHint(Some(HintInfo(Some(BROADCAST))), None))
            }
            val unioned = Union(partials)
            unioned.setTagValue(Injected, value = true)
            unioned
          }
        }
    }
  }

  // ── Skew resolution: salted join ──────────────────────────────────────────────

  /**
   * Salted join: spread a hot key across `saltFactor` buckets by adding a random salt to the
   * skewed side and replicating the build side `table` across all salt values, joining on the
   * original condition plus salt equality. Correct for inner equi-joins; other shapes are
   * left untouched. A top projection drops the salt columns to preserve the original schema.
   */
  private def applySalt(plan: LogicalPlan, table: String, saltFactor: Int): LogicalPlan = {
    if (saltFactor < 2) return plan
    var done = false
    plan.transformUp {
      case j: Join if !done && j.joinType == Inner && j.condition.isDefined && j.hint == JoinHint.NONE =>
        val cond = j.condition.get
        val buildOnLeft  = containsRelation(j.left, table)
        val buildOnRight = containsRelation(j.right, table)
        if (!buildOnLeft && !buildOnRight) j
        else {
          done = true
          val buildIsRight = buildOnRight
          val build  = if (buildIsRight) j.right else j.left
          val skewed = if (buildIsRight) j.left else j.right

          // Salt the skewed side: floor(rand() * saltFactor) in [0, saltFactor).
          val saltExpr = Cast(Multiply(Rand(SaltSeed), Literal(saltFactor.toDouble)), IntegerType)
          val saltAlias = Alias(saltExpr, SkewSaltCol)()
          val skewedSalted = Project(skewed.output :+ saltAlias, skewed)

          // Replicate the build side across every salt value via a cross join to a salt range.
          val rangeAttr = AttributeReference(SaltRangeCol, IntegerType, nullable = false)()
          val saltRange = LocalRelation(Seq(rangeAttr), (0 until saltFactor).map(InternalRow(_)))
          val buildReplicated = Join(build, saltRange, Cross, None, JoinHint.NONE)

          val newCond = And(cond, EqualTo(saltAlias.toAttribute, rangeAttr))
          val joined =
            if (buildIsRight) Join(skewedSalted, buildReplicated, Inner, Some(newCond), JoinHint.NONE)
            else Join(buildReplicated, skewedSalted, Inner, Some(newCond), JoinHint.NONE)

          val top = Project(j.output, joined)
          top.setTagValue(Injected, value = true)
          top
        }
    }
  }

  /**
   * Targeted salted join: only the discovered `hotKeys` are spread across `saltFactor` buckets.
   * The skewed side gets a random salt for hot keys and salt 0 for cold keys (a single
   * conditional [[Project]], preserving all attribute ids); the build side `table` is replicated
   * ×`saltFactor` for hot keys and ×1 for cold keys via [[Generate]]+[[Explode]] of a conditional
   * salt array. Correct for inner equi-joins on a single key; other shapes are left untouched.
   * A top projection drops the salt columns to preserve the original schema.
   */
  private def applyTargetedSalt(
      plan: LogicalPlan, table: String, saltFactor: Int, hotKeys: Seq[String]): LogicalPlan = {
    if (saltFactor < 2 || hotKeys.isEmpty) return plan
    var done = false
    plan.transformUp {
      case j: Join if !done && j.joinType == Inner && j.condition.isDefined && j.hint == JoinHint.NONE =>
        val cond = j.condition.get
        val buildOnLeft  = containsRelation(j.left, table)
        val buildOnRight = containsRelation(j.right, table)
        if (!buildOnLeft && !buildOnRight) j
        else {
          val buildIsRight = buildOnRight
          val build  = if (buildIsRight) j.right else j.left
          val skewed = if (buildIsRight) j.left else j.right
          val skewedKey = sideKeys(cond, skewed).headOption
          val buildKey  = sideKeys(cond, build).headOption
          if (skewedKey.isEmpty || buildKey.isEmpty) j
          else {
            done = true
            val sKey = skewedKey.get
            val bKey = buildKey.get
            val sHotLits = hotKeys.map(v => Cast(Literal(v), sKey.dataType))
            val bHotLits = hotKeys.map(v => Cast(Literal(v), bKey.dataType))

            // Skewed side: random salt for hot keys, 0 otherwise. Single Project keeps exprIds.
            val randSalt = Cast(Multiply(Rand(SaltSeed), Literal(saltFactor.toDouble)), IntegerType)
            val saltExpr = If(In(sKey, sHotLits), randSalt, Literal(0))
            val saltAlias = Alias(saltExpr, SkewSaltCol)()
            val skewedSalted = Project(skewed.output :+ saltAlias, skewed)

            // Build side: replicate hot rows across the salt range, cold rows once. Generate
            // appends the salt column and preserves the child's attribute ids.
            val saltAttr = AttributeReference(SaltRangeCol, IntegerType, nullable = false)()
            val hotArray  = CreateArray((0 until saltFactor).map(i => Literal(i)))
            val coldArray = CreateArray(Seq(Literal(0)))
            val saltArray = If(In(bKey, bHotLits), hotArray, coldArray)
            val buildReplicated =
              Generate(Explode(saltArray), Nil, outer = false, None, Seq(saltAttr), build)

            val newCond = And(cond, EqualTo(saltAlias.toAttribute, saltAttr))
            val joined =
              if (buildIsRight) Join(skewedSalted, buildReplicated, Inner, Some(newCond), JoinHint.NONE)
              else Join(buildReplicated, skewedSalted, Inner, Some(newCond), JoinHint.NONE)

            val top = Project(j.output, joined)
            top.setTagValue(Injected, value = true)
            top
          }
        }
    }
  }
  private[sparkx] def sideKeys(cond: Expression, side: LogicalPlan): Seq[Expression] = {
    def conjuncts(e: Expression): Seq[Expression] = e match {
      case And(l, r) => conjuncts(l) ++ conjuncts(r)
      case other     => Seq(other)
    }
    conjuncts(cond).flatMap {
      case EqualTo(l, r) =>
        if (l.references.nonEmpty && l.references.subsetOf(side.outputSet)) Some(l)
        else if (r.references.nonEmpty && r.references.subsetOf(side.outputSet)) Some(r)
        else None
      case _ => None
    }
  }

  // ── Stripping / detection ─────────────────────────────────────────────────────

  /**
   * Remove auto-fix-injected nodes, returning the pristine plan and the hints that had been
   * applied. Only nodes tagged [[Injected]] are touched, so user-authored repartitions and
   * hints are preserved.
   */
  def strip(plan: LogicalPlan): (LogicalPlan, Seq[Hint]) = {
    val detected = scala.collection.mutable.ArrayBuffer[Hint]()

    // Peel injected partitioning wrappers from the root.
    var p = plan
    var peeling = true
    while (peeling) {
      p match {
        case r: Repartition if r.getTagValue(Injected).contains(true) =>
          detected += (if (r.shuffle) RepartitionHint(r.numPartitions) else CoalesceHint(r.numPartitions))
          p = r.child
        case rb: RebalancePartitions if rb.getTagValue(Injected).contains(true) =>
          detected += RebalanceHint
          p = rb.child
        case _ => peeling = false
      }
    }

    // Clear injected join hints, recording the broadcast target.
    val cleared = p.transformUp {
      case j: Join if j.getTagValue(Injected).contains(true) =>
        val name =
          if (j.hint.leftHint.exists(_.strategy.contains(BROADCAST))) relationName(j.left)
          else if (j.hint.rightHint.exists(_.strategy.contains(BROADCAST))) relationName(j.right)
          else None
        name.foreach(n => detected += BroadcastHint(Seq(n)))
        j.copy(hint = JoinHint.NONE)
    }
    // `transformUp` re-copies tags from the original node onto rewritten ones, so strip our
    // marker from the resulting tree explicitly to keep the pristine plan tag-free.
    cleared.foreach(_.unsetTagValue(Injected))
    (cleared, detected.toSeq)
  }

  // ── Fingerprint ────────────────────────────────────────────────────────────────

  /**
   * A stable, literal-insensitive identity for a query, computed from its plan. Auto-fix nodes
   * are stripped first (so a hinted run and its original match), literal values are nulled (so
   * re-runs with different parameters match), and the result is canonicalized.
   */
  def fingerprintOf(plan: LogicalPlan): String = {
    val (pristine, _) = strip(plan)
    val normalized =
      try pristine.transformAllExpressions { case l: Literal => Literal(null, l.dataType) }
      catch { case _: Throwable => pristine }
    val canonical =
      try normalized.canonicalized.toString
      catch { case _: Throwable => normalized.toString }
    val md = java.security.MessageDigest.getInstance("SHA-256")
    md.digest(canonical.getBytes("UTF-8")).take(16).map(b => f"${b & 0xff}%02x").mkString
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def containsRelation(plan: LogicalPlan, table: String): Boolean =
    plan.collectFirst {
      case SubqueryAlias(id, _) if id.name.equalsIgnoreCase(table) => true
    }.isDefined

  private def relationName(plan: LogicalPlan): Option[String] =
    plan.collectFirst { case SubqueryAlias(id, _) => id.name }
}
