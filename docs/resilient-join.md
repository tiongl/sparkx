# Resilient Join Framework

SparkX provides a fault-tolerant join framework that automatically retries joins with different strategies when failures occur. It includes a strategy-switching orchestrator (`ResilientJoin`) and a specialized split-broadcast join (`SplitBroadcastJoin`) for handling broadcast OOM scenarios.

---

## Quick Start

### ResilientJoin — Strategy Fallback Chain

```scala
import com.sparkx.join.ResilientJoin._

// Default chain: broadcast → sort-merge → repartition
val result = left.resilientJoin(right, Seq("id"))

// With custom configuration
import com.sparkx.join._
val config = JoinConfig(
  strategies = Seq(
    new BroadcastJoinStrategy(thresholdBytes = 200L * 1024 * 1024),
    new SortMergeJoinStrategy(),
    new RepartitionJoinStrategy(basePartitions = 400)
  ),
  checkpointOnSuccess = true,
  maxAttempts = 3
)
val result = left.resilientJoin(right, Seq("id"), "inner", config)
```

### SplitBroadcastJoin — OOM-Safe Broadcast

```scala
import com.sparkx.join.SplitBroadcastJoin._

// Auto-detect splits from OOM (starts with full broadcast)
val result = largeDf.splitBroadcastJoin(smallDf, Seq("id"))

// With custom config
val result = largeDf.splitBroadcastJoin(smallDf, Seq("id"), "inner",
  SplitBroadcastConfig(
    broadcastBudgetBytes = 50L * 1024 * 1024,  // 50 MB per chunk
    maxSplits = 32,
    cacheLargeSide = true,   // cache the probe side to avoid recomputation
    cacheResult = true       // cache the final union result
  ))
```

---

## ResilientJoin

An orchestrator that wraps any DataFrame join with automatic fallback across a chain of `JoinStrategy` implementations.

### How It Works

1. Iterates through the configured strategy chain in order.
2. Each strategy's `canHandle()` is checked first — strategies can decline based on data size or previous failure context.
3. On success, the result is optionally cached and returned.
4. On failure, the exception is wrapped in a `JoinFailureContext` and passed to the next strategy.
5. If all strategies fail (or `maxAttempts` is reached), throws `ResilientJoinExhaustedException`.

### Built-in Strategies

| Strategy | Description | When it declines |
|----------|-------------|-----------------|
| `BroadcastJoinStrategy` | Wraps the smaller side in `broadcast()`. | Smaller side exceeds `thresholdBytes`, or a previous broadcast attempt OOM'd. |
| `SortMergeJoinStrategy` | Standard sort-merge join. Temporarily disables auto-broadcast. | Never — always willing to try. |
| `RepartitionJoinStrategy` | Repartitions both sides by join keys before joining. | Never — always willing to try. |

### Custom Strategies

Implement the `JoinStrategy` trait:

```scala
import com.sparkx.join._

class MyCustomStrategy extends JoinStrategy {
  override val name: String = "my-custom"

  override def canHandle(left: DataFrame, right: DataFrame,
                         failureCtx: Option[JoinFailureContext]): Boolean = {
    // Return false to skip this strategy
    true
  }

  override def join(left: DataFrame, right: DataFrame,
                    keys: Seq[String], joinType: String): DataFrame = {
    // Your join logic here
    left.join(right, keys, joinType)
  }
}

val config = JoinConfig(strategies = Seq(new MyCustomStrategy(), new SortMergeJoinStrategy()))
val result = left.resilientJoin(right, Seq("id"), config = config)
```

### JoinFailureContext

When a strategy fails, the next strategy receives a `JoinFailureContext` containing:

| Field            | Type            | Description |
|------------------|----------------|-------------|
| `failedStrategy` | `String`       | Name of the strategy that failed. |
| `exception`      | `Throwable`    | The exception that caused the failure. |
| `attempt`        | `Int`          | 1-based attempt number. |
| `leftSizeBytes`  | `Option[Long]` | Estimated left DataFrame size (from plan stats). |
| `rightSizeBytes` | `Option[Long]` | Estimated right DataFrame size (from plan stats). |

---

## SplitBroadcastJoin

A specialized join strategy for when the smaller side is too large for a single broadcast but you still want to avoid shuffle. It splits the smaller DataFrame into multiple chunks, broadcasts each chunk individually, and unions the partial results.

### How It Works

1. **Attempt full broadcast** (1 split).
2. **On OOM**, parse Spark's error message to extract the actual broadcast table size.
3. **Calculate optimal splits**: `ceil(actualSize / broadcastBudgetBytes)`, with a minimum of `currentSplits × 2`.
4. **Split using hash-modulo**: `pmod(hash(keys), numSplits)` ensures deterministic, non-overlapping chunks.
5. **Execute N broadcast joins** — one per chunk — and `union` the results.
6. **Cache the large side** (configurable) so repeated scans hit memory instead of recomputing.
7. If splits exceed `maxSplits`, throw `SplitBroadcastExhaustedException`.

### Why Cache the Large Side?

With N splits, the large (probe) DataFrame is scanned N times. If it comes from storage (Parquet/ORC), repeated scans hit the OS page cache. But if it's derived from a complex subquery chain, each split would recompute the entire pipeline. Setting `cacheLargeSide = true` (the default) caches it in memory before the split loop and unpersists it automatically after completion.

### Supported Join Types

| Join Type    | Supported | Why |
|-------------|-----------|-----|
| `inner`     | ✅        | Union of partial inner joins = full inner join. |
| `left_semi` | ✅        | Union of partial semi joins = full semi join (deduplication is idempotent). |
| `cross`     | ✅        | Union of partial cross joins = full cross join. |
| `left_outer` | ❌       | Each split would produce null-padded rows for non-matching keys that actually match in another split. |
| `right_outer` | ❌      | Same issue as left_outer. |
| `full_outer` | ❌       | Same issue as left/right outer. |
| `left_anti`  | ❌       | Each split would include rows whose keys match in another split's chunk. |

### OOM Message Parsing

The strategy extracts broadcast table size from Spark's error messages using regex patterns:

- `"The size of the broadcast table is 524288000 bytes"` → 524288000
- `"The size of the broadcast table is 800.5 MiB"` → ~839 MB
- Supports `bytes`, `KiB`/`KB`, `MiB`/`MB`, `GiB`/`GB`
- Walks the exception cause chain to find the message

If parsing fails, the strategy falls back to doubling the split count.

### SplitBroadcastConfig

| Parameter              | Type      | Default        | Description |
|------------------------|----------|----------------|-------------|
| `broadcastBudgetBytes` | `Long`   | `100 MB`       | Target size per broadcast chunk. |
| `maxSplits`            | `Int`    | `16`           | Upper limit on split count. |
| `cacheResult`          | `Boolean`| `true`         | Cache the final union result. |
| `cacheLargeSide`       | `Boolean`| `true`         | Cache the large (probe) DataFrame during execution to avoid recomputation on repeated scans. Automatically unpersisted after completion. |

### JoinConfig

| Parameter                | Type              | Default | Description |
|--------------------------|-------------------|---------|-------------|
| `strategies`             | `Seq[JoinStrategy]` | broadcast → sort-merge → repartition | Ordered fallback chain. |
| `broadcastThresholdBytes` | `Long`           | `100 MB` | Size threshold for auto-broadcast eligibility. |
| `checkpointOnSuccess`    | `Boolean`        | `true`   | Cache the result after a successful join. |
| `maxAttempts`            | `Int`            | `3`      | Maximum number of strategies to try. |

---

## Combining ResilientJoin with SplitBroadcastJoin

You can use `SplitBroadcastJoin` as a custom strategy within `ResilientJoin`:

```scala
import com.sparkx.join._
import com.sparkx.join.SplitBroadcastJoin._

// Try full broadcast first, then split-broadcast, then sort-merge
val config = JoinConfig(strategies = Seq(
  new BroadcastJoinStrategy(),
  // On OOM, SplitBroadcastJoin can be called directly:
  new SortMergeJoinStrategy()
))

// Or use SplitBroadcastJoin standalone:
val result = large.splitBroadcastJoin(small, Seq("id"))
```

---

## AutoSaltJoin — Skew-Aware Salted Join

A join strategy that detects skewed (hot) keys via sampling and applies
salt-based redistribution to eliminate join stragglers — without relying
on Spark's built-in AQE skew join.

### Quick Start

```scala
import com.sparkx.join.AutoSaltJoin._

// Auto-detect skewed side and salt hot keys
val result = skewedDf.autoSaltJoin(otherDf, Seq("user_id"))

// With custom config
val result = skewedDf.autoSaltJoin(otherDf, Seq("user_id"), "inner",
  AutoSaltJoinConfig(
    sampleFraction = 0.05,
    skewThresholdMultiplier = 5.0,
    minHotKeyCount = 50,
    saltFactor = 20,
    skewedSide = SkewedSide.Left
  ))
```

### How It Works

1. **Sample** both sides (default 1%) to estimate per-key frequencies.
2. **Detect hot keys** — keys whose sampled frequency exceeds
   `max(median × multiplier, minHotKeyCount × sampleFraction)`.
3. **If no hot keys**, fall through to a regular join (zero overhead).
4. **Salt the skewed side** — hot-key rows get a random salt column
   `floor(rand(seed) × saltFactor)` in `[0, saltFactor)`;
   cold-key rows get salt `0`.
5. **Replicate the other side** — hot-key rows are exploded
   `saltFactor` times (one per salt value); cold-key rows get salt `0`.
6. **Join** on `(original_keys + salt_column)` — hot keys are now
   distributed across `saltFactor` partitions instead of one.
7. **Drop** the salt column from the result.

### When to Use

| Scenario | AutoSaltJoin | AQE Skew Join |
|----------|-------------|---------------|
| Known hot keys, want deterministic redistribution | ✅ | — |
| Need control over salt factor per workload | ✅ | — |
| AQE disabled or unavailable | ✅ | ❌ |
| Automatic runtime adaptation | — | ✅ |
| No prior knowledge of data distribution | Possible (samples) | ✅ |

AutoSaltJoin complements AQE — it works at the logical level before
execution, while AQE adapts at runtime. They can be used together.

### Skew Detection

- Uses `approxQuantile` to compute the median key frequency from a sample.
- A key is "hot" if its sampled count ≥ `max(median × multiplier, minHotKeyCount × sampleFraction)`.
- Hot keys are capped at `maxSkewedKeys` (top-N by frequency).
- Null keys are always treated as cold (Spark equi-joins don't match nulls).
- Detection uses `groupBy(keys: _*)` with struct-based grouping (safe for composite keys — no string-concatenation collisions).

### Supported Join Types

| Join Type      | Supported | Notes |
|---------------|-----------|-------|
| `inner`       | ✅        | Full correctness — salt is symmetric. |
| `left_outer`  | ✅        | Non-matching left rows preserved with salt `0`. |
| `right_outer` | ✅        | Non-matching right rows preserved with salt `0`. |
| `left_semi`   | ❌        | Replication can produce duplicate matches. |
| `left_anti`   | ❌        | Salted anti-join would incorrectly include matching rows from other salt buckets. |
| `full_outer`  | ❌        | Both-side preservation + salting produces incorrect null-padded rows. |

### AutoSaltJoinConfig

| Parameter                  | Type         | Default | Description |
|----------------------------|-------------|---------|-------------|
| `sampleFraction`           | `Double`    | `0.01`  | Fraction of rows to sample for skew detection. |
| `skewThresholdMultiplier`  | `Double`    | `10.0`  | Key frequency must exceed `median × multiplier` to be hot. |
| `minHotKeyCount`           | `Long`      | `100`   | Absolute minimum estimated row count for a hot key. |
| `saltFactor`               | `Int`       | `10`    | Number of salt buckets per hot key. |
| `maxSkewedKeys`            | `Int`       | `1000`  | Cap on number of keys to salt (top-N by frequency). |
| `seed`                     | `Long`      | `42`    | Random seed for reproducible salt assignment. |
| `skewedSide`               | `SkewedSide`| `Auto`  | `Auto` (detect), `Left`, or `Right`. |
| `cacheResult`              | `Boolean`   | `true`  | Cache the final joined result. |

### SkewedSide

| Value   | Behaviour |
|---------|-----------|
| `Auto`  | Samples both sides; picks the side with more hot keys. |
| `Left`  | Forces the left side as skewed (skips right-side sampling). |
| `Right` | Forces the right side as skewed (skips left-side sampling). |

---

## Engine-Level Resilient Join (ResilientJoinExec)

An alternative approach that operates **inside Spark's query engine** as a custom physical plan node. Unlike the wrapper-level `ResilientJoin` which eagerly materialises DataFrames (breaking Catalyst's lazy optimisation), `ResilientJoinExec` preserves the full query plan — downstream filters, projections, and aggregations remain optimised by Catalyst.

### Quick Start

```scala
// Enable via SparkConf (no code changes to queries needed)
spark.conf.set("spark.sparkx.resilientJoin.enabled", "true")

// Register the strategy (done automatically by SparkXListener if using the plugin)
spark.experimental.extraStrategies ++= Seq(new ResilientJoinStrategy())

// All equi-joins now use the resilient fallback chain transparently
val result = left.join(right, Seq("id"))
```

### How It Works

1. **`ResilientJoinStrategy`** intercepts equi-join logical plan nodes via `ExtractEquiJoinKeys` and produces a `ResilientJoinExec` physical node.
2. **`ResilientJoinExec.doExecute()`** runs a fallback chain:
   - **Broadcast hash join** — builds a `HashedRelation`, broadcasts it. On OOM, falls back.
   - **Shuffled hash join** — hash-partitions both sides, per-partition hash join.
   - **Sort-merge join** — hash-partitions + sorts both sides, merge join.
3. Because fallback happens **inside** `doExecute()`, downstream operators (filters, aggregations, projections) remain in the same optimised plan and are never re-evaluated.

### Build-Side Selection

| Join Type | Build Side | Reason |
|-----------|-----------|--------|
| `inner` | Smaller side (by stats) | Standard optimisation |
| `left_outer`, `left_semi`, `left_anti` | Right | Must preserve left side |
| `right_outer` | Left | Must preserve right side |
| `full_outer` | Smaller side | Both sides preserved |

### Broadcast Limitations for Outer Joins

For outer joins, broadcast hash join is skipped and the node falls directly to shuffled hash join. This is because manually constructing `BroadcastHashJoinExec` for outer joins requires codegen preparation that isn't available when creating nodes dynamically inside `doExecute()`.

### Configuration

All keys are prefixed with `spark.sparkx.resilientJoin.`:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | Boolean | `false` | Master switch for the planner strategy. |
| `broadcastThreshold` | Long | `100 MB` | Max estimated build-side size for broadcast attempt. |
| `splitBroadcast.enabled` | Boolean | `true` | Try split-broadcast on broadcast OOM (future). |
| `splitBroadcast.budgetBytes` | Long | `100 MB` | Target size per broadcast chunk. |
| `splitBroadcast.maxSplits` | Int | `16` | Max split count. |
| `autoSalt.enabled` | Boolean | `true` | Enable auto-salt skew handling (future). |
| `autoSalt.sampleFraction` | Double | `0.01` | Skew detection sample fraction. |
| `autoSalt.skewMultiplier` | Double | `10.0` | Hot-key frequency multiplier. |
| `autoSalt.saltFactor` | Int | `10` | Salt buckets per hot key. |
| `autoSalt.minHotKeyCount` | Long | `100` | Minimum estimated hot-key count. |

### Wrapper vs Engine Comparison

| Aspect | Wrapper (`ResilientJoin`) | Engine (`ResilientJoinExec`) |
|--------|--------------------------|------------------------------|
| Activation | Explicit API call | SparkConf flag (transparent) |
| Catalyst optimisation | Broken (eager materialisation) | Preserved (stays in plan) |
| Per-call configuration | ✅ Full control | ❌ Global SparkConf only |
| Custom strategy chain | ✅ Any `JoinStrategy` impl | ❌ Fixed broadcast → shuffle → sort-merge |
| Retry granularity | Per DataFrame operation | Per physical operator execution |
| Code changes needed | Yes (change join calls) | No (enable via config) |

**Recommendation:** Use the engine-level approach for transparent resilience across all joins in an application. Use the wrapper when you need per-join custom strategy chains or fine-grained control.

---

## Error Handling

| Exception | When thrown |
|-----------|-----------|
| `ResilientJoinExhaustedException` | All strategies in the fallback chain have failed. Contains the list of `JoinFailureContext` objects. |
| `SplitBroadcastExhaustedException` | Split count would exceed `maxSplits`. Contains the last split count and the root cause. |
| `IllegalArgumentException` | Unsupported join type passed to `SplitBroadcastJoin`. |
