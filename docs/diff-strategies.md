# Dataset Diff Strategies

SparkX provides two high-performance strategies for comparing large Spark DataFrames and pinpointing row-level differences. Both strategies identify **added**, **removed**, and **changed** rows between a `left` and `right` DataFrame.

---

## Quick Start

```scala
import com.sparkx.diff._

val config = DiffConfig(
  keyColumns  = Seq("id"),
  diffColumns = Seq("name", "value"),  // optional — defaults to all non-key columns
  limit       = 1000                   // max rows per category (added/removed/changed)
)

// Bloom filter strategy (best for large datasets with few differences)
val result = new BloomDiffStrategy().diff(left, right, config)

// MD5 hash strategy (best for change detection with minimal shuffle)
val result = new MD5DiffStrategy().diff(left, right, config)
```

### Reading Results

```scala
result.summary.addedCount    // total added rows (before limit)
result.summary.removedCount  // total removed rows
result.summary.changedCount  // total changed rows
result.summary.comparisonTimeMs

result.added.show()   // DataFrame of added rows
result.removed.show() // DataFrame of removed rows
result.changed.show() // DataFrame with key cols, left_*/right_* value pairs,
                       // and a changed_columns array
```

---

## Strategies

### BloomDiffStrategy

Uses a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) to pre-filter candidates before precise comparison.

**Workflow:**

1. Determines the smaller dataset (via logical-plan statistics).
2. Builds a Bloom filter on the smaller side's composite key.
3. Broadcasts the filter and pre-filters the larger side — eliminates rows whose keys definitely don't exist on the other side.
4. Anti-joins (with broadcast key-only projections) find added/removed rows.
5. Inner join on Bloom-filtered data + column-wise comparison finds changed rows.

**Tuning parameters:**

```scala
new BloomDiffStrategy(
  expectedNumItems = 10000000L,  // sizing hint for Bloom filter
  fpp              = 0.01        // false-positive probability (1%)
)
```

**When to use:**

- Datasets with very different key sets (many adds/removes).
- When you want to reduce shuffle by broadcasting a compact Bloom filter instead of full key sets.
- The Bloom filter is tiny (~1.2 MB per million keys at 1% FPP) so broadcasts cheaply.

**Trade-offs:**

- False positives in the Bloom filter cause extra rows in the inner join (but never produce incorrect results — verification is exact).
- Two broadcast operations: the Bloom filter + key-only projections for anti-joins.

---

### MD5DiffStrategy

Uses MD5 hashing to quickly identify changed rows, then verifies with exact column-wise comparison.

**Workflow:**

1. Computes an MD5 hash of the diff columns for every row (via `md5(to_json(struct(...)))`).
2. Anti-joins on key columns find added/removed rows.
3. **Narrow join** — joins only `(key_columns, md5_hash)` projections to find rows with mismatched hashes. This shuffles ~36 bytes per row instead of the full row width.
4. Broadcasts the mismatched key set back to fetch full rows only for changed candidates.
5. Exact null-safe column-wise comparison on candidates produces the final diff.

**When to use:**

- Datasets with the same key sets but potential value changes (e.g., snapshot comparison).
- When most rows are unchanged — the narrow hash join avoids shuffling full-width rows for the unchanged majority.
- At 0% diff, MD5 produces **near-zero shuffle** (only the narrow key+hash join, and zero broadcast for mismatches).

**Trade-offs:**

- MD5 computation adds CPU overhead.
- Hash collisions are theoretically possible but vanishingly rare (2⁻¹²⁸ per row pair). The exact verification step guarantees correctness regardless.

---

## API Reference

### DiffConfig

| Parameter     | Type         | Default                  | Description |
|---------------|-------------|--------------------------|-------------|
| `keyColumns`  | `Seq[String]` | *(required)*            | Columns that uniquely identify a row (used for joining). |
| `diffColumns` | `Seq[String]` | `Seq.empty`             | Columns to compare for changes. Empty = all non-key columns. |
| `limit`       | `Int`         | `100`                   | Max rows returned per category (added/removed/changed). |

### DiffResult

| Field     | Type          | Description |
|-----------|--------------|-------------|
| `added`   | `DataFrame`  | Rows in `right` but not in `left` (limited). |
| `removed` | `DataFrame`  | Rows in `left` but not in `right` (limited). |
| `changed` | `DataFrame`  | Rows with matching keys but different values. Contains key columns, `left_<col>`/`right_<col>` pairs, and a `changed_columns` array. |
| `summary` | `DiffSummary` | Aggregate counts (pre-limit) and timing. |

### DiffSummary

| Field              | Type   | Description |
|--------------------|--------|-------------|
| `addedCount`       | `Long` | Total added rows (before limit). |
| `removedCount`     | `Long` | Total removed rows (before limit). |
| `changedCount`     | `Long` | Total changed rows (before limit). |
| `comparisonTimeMs` | `Long` | Wall-clock time for the comparison. |

---

## Performance Characteristics

Benchmarked with 20-column tables (id + 19 attribute columns), varying row counts and diff percentages.

### Shuffle Reduction (2M rows, 30% diff)

| Strategy  | Shuffle Write |
|-----------|--------------|
| Naive     | ~454 MB      |
| BloomDiff | ~341 MB      |
| MD5Diff   | ~341 MB      |

### Scaling (10K → 2M rows)

Both strategies scale near-linearly. MD5Diff shows the largest speedup at low diff percentages because the narrow hash join avoids shuffling unchanged rows entirely.

### By Diff Percentage (500K rows)

| Diff %  | Naive Shuffle | MD5 Shuffle | Speedup |
|---------|--------------|-------------|---------|
| 0%      | 107 MB       | ~0 MB       | Excellent |
| 5%      | 107 MB       | ~5 MB       | Very good |
| 30%     | 107 MB       | ~32 MB      | Good |
| 50%     | 107 MB       | ~54 MB      | Moderate |

**Key insight:** At 0% diff, MD5Diff's shuffle drops to nearly zero because the narrow join finds no mismatches, so no full rows need to be fetched.
