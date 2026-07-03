# SparkX Auto-Fix — Closed-Loop Hint Injection

Auto-fix is an **opt-in, self-tuning optimizer** for Spark queries. It observes each query as it
runs, learns which plan-level hints make it faster, persists that knowledge keyed by a stable query
fingerprint, and — on subsequent runs of the *same* query — injects the winning hints automatically.
It works uniformly for **SparkSQL text** and the **DataFrame/Dataset API**, because it operates on the
resolved logical plan rather than on SQL strings.

> **Status:** experimental. Disabled by default. Read-only unless you explicitly enable applying.

---

## Table of contents

- [Quick start](#quick-start)
- [Modes](#modes)
- [How it works: the closed loop](#how-it-works-the-closed-loop)
- [Query fingerprinting](#query-fingerprinting)
- [The hint catalog](#the-hint-catalog)
- [Skew resolution](#skew-resolution)
- [SQL-text pseudo-hints](#sql-text-pseudo-hints)
- [The learning lifecycle](#the-learning-lifecycle)
- [Tag-based identity](#tag-based-identity)
- [The Auto-Fix UI page](#the-auto-fix-ui-page)
- [Standalone SQL-text facade](#standalone-sql-text-facade)
- [Configuration reference](#configuration-reference)
- [Performance & safety](#performance--safety)
- [Testing](#testing)
- [Limitations](#limitations)

---

## Quick start

Auto-fix is delivered through a `spark.sql.extensions` entry point, separate from the read-only
SparkX UI listener. Enable both the extension and the feature flag:

```bash
spark-submit \
  --jars /path/to/sparkx-assembly-0.1.0.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.sparkx.SparkXAutoFixExtension \
  --conf spark.sparkx.autofix.enabled=true \
  --conf spark.sparkx.autofix.mode=auto \
  --conf spark.sparkx.autofix.store.path=file:///var/lib/sparkx-autofix \
  --class com.example.MyApp my-app.jar
```

- `spark.sql.extensions` registers the fix-side rule, the SQL pseudo-hint resolver, and the learner.
- `spark.sparkx.autofix.enabled=true` turns the loop on (it is `false` by default — nothing happens
  otherwise).
- `spark.sparkx.autofix.store.path` is where learned profiles are persisted (any Hadoop-compatible
  path: local, HDFS, S3, …). Defaults to a temp directory.

The learned profiles then appear under the **SparkX → Auto-Fix** tab.

---

## Modes

`spark.sparkx.autofix.mode` selects how aggressive the loop is. All modes require
`spark.sparkx.autofix.enabled=true`.

| Mode | Learns? | Applies hints? | Use when |
|---|---|---|---|
| `auto` (default) | ✅ | ✅ | You want fully automatic tuning. |
| `learn` | ✅ | ❌ | You want to build up profiles without changing any plan. |
| `shadow` | ✅ | ❌ | Same as `learn`; records what *would* be proposed for audit. |
| `fix` | ✅ | ✅ | Apply already-learned hints; keep learning. |

Mapping in code (`SparkXConfig`):

```scala
def autofixLearnEnabled  = autofixEnabled
def autofixApplyEnabled  = autofixEnabled && (mode == "auto" || mode == "fix")
def autofixShadowEnabled = autofixEnabled && mode == "shadow"
```

The **learner always runs** when the feature is enabled; only *applying* is gated by mode. This is
what lets you safely dry-run: turn on `learn`/`shadow`, inspect the Auto-Fix tab, then promote to
`auto`.

---

## How it works: the closed loop

```
              ┌──────────────────────── one query execution ───────────────────────┐
              │                                                                     │
  resolved    │   ┌──────────────┐   apply pending hints   ┌───────────────┐        │
  logical  ───┼──▶│ AutoFixRule  │────────────────────────▶│ hinted plan   │──▶ run │
  plan        │   │ (fix side)   │   (auto / fix mode)     │ (tagged)      │        │
              │   └──────┬───────┘                         └───────────────┘        │
              │          │ fingerprint + store.load                                 │
              │          ▼                                                           │
              │   ┌──────────────┐                                                   │
              │   │FixProfileStore│◀───────────────────────────────┐                │
              │   └──────────────┘                                  │ store.save     │
              │                                                     │                │
              │   ┌──────────────┐   measure + advise   ┌───────────┴───────┐        │
  on success  └──▶│ AutoFixLearner│───────────────────▶│    HintPolicy      │        │
  (listener)      │ (learn side)  │  PlanAdvisor.advise │ (decide next hints) │       │
                  └──────────────┘                     └────────────────────┘        │
```

The two sides are wired by `SparkXAutoFixExtension`:

- **Fix side — `AutoFixRule`** (`injectPostHocResolutionRule`). Runs during analysis on *every*
  resolved plan. It fingerprints the plan, loads the profile, and — if applying is enabled — injects
  the profile's `pendingHints` as catalyst nodes via `PlanHints.apply`. Idempotent: it bails if the
  plan already carries injected nodes.
- **Learn side — `AutoFixLearner`** (a `QueryExecutionListener`, registered once per session). On
  each successful run it recovers the applied hints, measures wall-clock duration, asks
  `PlanAdvisor` what problems remain, and folds everything into the profile through `HintPolicy`.
- **SQL pseudo-hints — `SparkXHintRule`** (`injectResolutionRule`). Lets you hand-write SparkX skew
  hints directly in SQL text (see [below](#sql-text-pseudo-hints)).

Because both sides key off the **same plan fingerprint**, a query written as SQL and the "same" query
written with the DataFrame API — or re-run tomorrow with different literal values — map to one profile.

### The core files

| File | Package | Role |
|---|---|---|
| `SparkXAutoFixExtension` | `org.apache.spark.sql.sparkx` | `spark.sql.extensions` entry point; wires all three rules + the learner. |
| `AutoFixRule` | `org.apache.spark.sql.sparkx` | Fix side: inject learned hints into resolved plans. |
| `AutoFixLearner` | `org.apache.spark.sql.sparkx` | Learn side: measure + advise + persist. |
| `PlanHints` | `org.apache.spark.sql.sparkx` | Catalyst-level hint injection / strip / fingerprint / identity. |
| `PlanAdvisor` | `org.apache.spark.sql.sparkx` | Derive candidate hints from analyzed + physical plans. |
| `SkewKeyDiscovery` | `org.apache.spark.sql.sparkx` | Sample the skewed side to find hot join keys. |
| `SparkXHintRule` | `org.apache.spark.sql.sparkx` | Resolve SparkX skew pseudo-hints written in SQL text. |
| `HintPolicy` | `com.sparkx.autofix` | Decide the next hint set; track best; converge. |
| `Hint` | `com.sparkx.autofix` | The hint algebra (render / parse / key). |
| `FixProfile` / `FixProfileStore` | `com.sparkx.autofix` | Persisted per-fingerprint learning state (JSON). |
| `HintRewriter` | `com.sparkx.autofix` | Dedupe hints; rewrite SQL text hint blocks. |
| `AutoFixPage` | `org.apache.spark.ui.sparkx` | Renders learned profiles in the SparkX UI. |

> **Two-package split.** UI-adjacent and Catalyst-internal classes live in `org.apache.spark.*`
> because they use `private[spark]` / `private[sql]` APIs; everything else lives in `com.sparkx.*`.
> See `.github/copilot-instructions.md`.

---

## Query fingerprinting

A fingerprint is a **stable, literal-insensitive identity** for a query so re-runs with different
parameters share one profile.

Two independent schemes exist:

1. **Plan fingerprint — `PlanHints.fingerprintOf`** (used by the runtime loop). Steps:
   - `strip` any auto-fix-injected nodes so a hinted plan matches its un-hinted original;
   - null out every `Literal` value (keeping its type) so `dt = '2024-01-01'` and `dt = '2024-06-30'`
     collapse together;
   - `canonicalized.toString`, SHA-256, first 128 bits → 32 hex chars.
2. **Text fingerprint — `QueryFingerprint.compute`** (used by the standalone
   [`SparkXAutoFix` facade](#standalone-sql-text-facade)). Strips comments/hint blocks, replaces
   string & numeric literals with `?`, lower-cases, collapses whitespace, then SHA-256.

The runtime loop uses the **plan** fingerprint so it can cover both SparkSQL and DataFrame queries.

---

## The hint catalog

All hints implement the `Hint` trait (`render`, `key`, `advisory`) and are turned into Catalyst nodes
by `PlanHints.apply`. `key` de-duplicates by category (at most one hint per category is proposed).

| Hint | Renders as | Category `key` | Catalyst rewrite |
|---|---|---|---|
| `BroadcastHint(tables)` | `BROADCAST(a, b)` | `broadcast:…` | Sets a `JoinHint` broadcast strategy on the matching side. |
| `RepartitionHint(n)` | `REPARTITION(n)` | `partitioning` | `Repartition(n, shuffle=true)`. |
| `CoalesceHint(n)` | `COALESCE(n)` | `partitioning` | `Repartition(n, shuffle=false)`. |
| `RebalanceHint` | `REBALANCE` | `partitioning` | `RebalancePartitions` (AQE rebalances). |
| `SplitBroadcastHint(t, n)` | `SPLIT_BROADCAST(t, n)` | `skew:t` | N-way "double broadcast" (see below). |
| `SaltedJoinHint(t, n)` | `SALT(t, n)` | `skew:t` | Uniform salted join. |
| `TargetedSaltHint(t, n, keys)` | `TARGETED_SALT(t, n, v1\|v2)` | `skew:t` | Salt **only** the listed hot keys. |

Every hint round-trips through `Hint.render` / `Hint.parse`, which is how they are persisted in a
profile and recovered from stamped plan tags.

---

## Skew resolution

The interesting hints tackle **join skew** — a hot key whose rows all land on one reducer. `PlanAdvisor`
detects a skew-prone equi-join (a physical `SortMergeJoin`, i.e. a real shuffle join — or *any* join
when skew is forced via `spark.sparkx.autofix.skew.factor <= 1`) whose smaller (build) side is bigger
than plain-broadcast size, then picks a strategy by build-side size:

- build side ≤ `skew.broadcastMaxBytes` → **split broadcast**;
- larger → **salting** (targeted if hot keys can be discovered, else uniform).

All three rewrites are **schema-preserving** and correctness-verified by execution tests.

### 1. Split / N-way broadcast — `PlanHints.applySplitBroadcast`

Splits the build side into `n` hash-disjoint chunks and broadcasts each chunk against the large side,
unioning the partials:

```
Union(
  for i in 0 until n:
    Join(large, Filter(pmod(murmur3(joinKeys), n) = i, build), BROADCAST)
)
```

Correct for **inner** joins (build on either side) and **left-semi** joins (build on the right).
Turns one skewed shuffle join into `n` small broadcast joins.

### 2. Uniform salting — `PlanHints.applySalt`

Spreads *every* row across `n` salt buckets and replicates the *entire* build side ×`n`:

- skewed side: `Project(cols :+ floor(rand() * n) AS salt)`;
- build side: cross-joined to a `LocalRelation` of `[0, n)` (each build row copied `n` times);
- join on the original condition **and** `salt = range`;
- a top `Project` drops the salt columns.

Correct for **inner** equi-joins. Simple, but pays an ×`n` blow-up on the build side regardless of how
skewed the data actually is.

### 3. Targeted salting — `PlanHints.applyTargetedSalt`

The refinement: salt **only the discovered hot keys**, leave everything else untouched. This avoids
uniform salting's ×`n` build-side blow-up on the cold majority.

- **skewed side** — one conditional `Project` (preserves attribute ids, no `Union`):
  ```
  salt = IF(key IN (hot…), floor(rand() * n), 0)
  ```
- **build side** — replicate hot rows ×`n`, cold rows ×1, via `Generate`/`Explode` of a conditional
  salt array:
  ```
  Generate(Explode(IF(key IN (hot…), array(0 … n-1), array(0))), … , build)
  ```
- join on the original condition **and** `salt = range`; top `Project` drops the salt column.

Correct for **inner** equi-joins on a single key. Hot keys are carried as literal strings and cast back
to the key's own type at rewrite time (so they serialize cleanly into a profile).

#### How hot keys are discovered — `SkewKeyDiscovery.discover`

This is the **only** part of the learn phase that launches a Spark job. It mirrors the sampling
heuristic of the DataFrame-level `AutoSaltJoin.findHotKeys`:

1. Project the skewed side down to the single join key.
2. `sample(fraction)` — default **1%** (`skew.sampleFraction`).
3. Drop nulls, `groupBy(key).count()`.
4. `approxQuantile(0.5)` → median frequency.
5. Keep keys with `freq ≥ median × thresholdMultiplier` (default **10×**), `orderBy(freq desc)`,
   `limit(maxKeys)` (default **100**), `collect()` the values.

It runs **only** on the learn path (inside `PlanAdvisor.advise`, gated by
`spark.sparkx.autofix.skew.targeted`, default on), only when a salt strategy is chosen, and is fully
wrapped in try/catch: on *any* failure it returns `Nil` and the advisor falls back to uniform
`SaltedJoinHint`. It re-samples on each un-converged learn cycle while the `SortMergeJoin` is still
present, and stops once the plan converges to a non-shuffle join.

Everything *else* in the learn phase is job-free — it reads plan trees and catalog statistics
(`plan.stats.sizeInBytes`) that already exist from the query that just ran.

---

## SQL-text pseudo-hints

The skew rewrites are also usable as **hand-authored SQL hints**, resolved by `SparkXHintRule`:

```sql
SELECT /*+ SPLIT_BROADCAST(dim, 6) */  f.*, dim.name FROM fact f JOIN dim ON f.k = dim.k;
SELECT /*+ SALT(dim, 16) */            f.*, dim.name FROM fact f JOIN dim ON f.k = dim.k;
SELECT /*+ TARGETED_SALT(dim, 16, 0) */ f.*, dim.name FROM fact f JOIN dim ON f.k = dim.k;
--                                  ▲   ▲  ▲
--                              table  n  hot key values (0, 1, …)
```

**Why a custom rule is needed.** Spark parses any unknown `/*+ NAME(args) */` into an `UnresolvedHint`
and then *silently drops it* (`ResolveHints.RemoveAllHints`). `SparkXHintRule` is injected as a
resolution rule that runs **before** that removal: once the hint's child is resolved, it recognizes our
hint names, parses the arguments (the relation name arrives as an `UnresolvedAttribute`; the count as an
integer `Literal`; for `TARGETED_SALT`, the trailing params are the hot values), and rewrites into the
same Catalyst plan the auto-fix loop injects. Because the result is tagged `Injected`, the auto-fix rule
bails (user intent wins) and the learner skips the manually-hinted run.

These pseudo-hints only work when the extension is registered. Unknown hints are left untouched for
Spark to handle normally.

---

## The learning lifecycle

A profile moves through three statuses, driven by `HintPolicy.update`:

| Status | Meaning |
|---|---|
| `learning` | Initial state; no attempts recorded yet. |
| `optimizing` | Still trying new hint sets — a fresh candidate is pending for the next run. |
| `converged` | Out of untried candidates (or hit the iteration budget); best-known-good hints are locked in. |

On each run the policy:

1. Appends a `FixAttempt(hints, durationMs, ts, improved)` to the history.
2. Records the **baseline** duration from the first un-hinted run.
3. Keeps the **strictly-fastest** run as `bestMs` / `bestHints` (baseline included) — so it **never
   regresses**: if a hinted attempt is slower, the best set is retained and a *different* candidate is
   tried next.
4. Computes the next untried candidate via `nextCandidate`:
   - layer each fresh `PlanAdvisor` recommendation on top of the current best; then
   - tune an existing numeric partitioning hint by doubling / halving.
   - `triedSets` memoizes every hint set already attempted (by rendered form) so nothing is retried.
5. Marks `converged` and pins `bestHints` into `pendingHints` when no new candidate exists or after
   `spark.sparkx.autofix.maxIterations` hinted attempts.

An attempt only counts as `improved` if it beats the prior best by the `ImproveMargin` (must be at
least 2% faster), which damps noise.

> **"converged" vs "optimizing".** `optimizing` means the loop still has an untried hint set queued for
> the next execution. `converged` means it has settled on the fastest set it found and will keep applying
> that. A converged profile can still be nudged if new advice appears (e.g. data characteristics change),
> because advice is recomputed every run.

---

## Tag-based identity

Simple hints (broadcast, partitioning) are structurally reversible: `PlanHints.strip` peels the injected
`Repartition`/`JoinHint` wrappers back off and reports what it removed. But the **skew rewrites**
(`Union` of split-broadcasts, the salt `Project`/`Generate` scaffolding) **cannot** be reversed
structurally.

To bridge this, `AutoFixRule` **stamps identity** onto the injected plan root as `TreeNodeTag`s:

- `PlanHints.Injected` — marks each injected node (idempotency + selective strip);
- `PlanHints.Fingerprint` — the profile fingerprint;
- `PlanHints.AppliedHints` — the `;`-joined rendered hints.

The learner then calls `PlanHints.identityFrom` first — reading the stamped tags — and only falls back
to structural `strip` for un-stamped runs (baselines, broadcast/partitioning fixes). This guarantees the
fingerprint path only ever sees *pristine* plans, and complex rewrites are still attributed correctly.

A plan that carries injected nodes but **no** stamped identity was hinted manually via SQL
(`SparkXHintRule`), not by the loop — the learner detects this and skips folding that run into the
profile.

---

## The Auto-Fix UI page

`AutoFixPage` (SparkX → **Auto-Fix** sub-tab) lists every learned profile: fingerprint, status badge,
baseline-vs-best timing, estimated savings %, the hints applied, a before/after plan preview, and any
advisory skew recommendations. It is **read-only** and works in both the live UI and the History Server.
If the feature is disabled it shows a notice instead.

---

## Standalone SQL-text facade

`com.sparkx.autofix.SparkXAutoFix` is a small **text-only** utility, independent of the runtime loop and
the Spark UI, for scripting or batch SQL rewriting:

```scala
import com.sparkx.autofix.SparkXAutoFix

val store = SparkXAutoFix.store("file:///tmp/sparkx-autofix-text", hadoopConf)
val fixed = SparkXAutoFix.fix("SELECT * FROM a JOIN b ON a.id = b.id", store)
// fingerprint / rewrite / hintsFor are also exposed
```

It keys profiles by **text** fingerprint (`QueryFingerprint`), *not* the plan fingerprint used by the
runtime loop, so it uses a separate store and is not wired to the live interception path.

---

## Configuration reference

All keys require `spark.sparkx.autofix.enabled=true` and the extension registered.

### Core

| Key | Default | Description |
|---|---|---|
| `spark.sparkx.autofix.enabled` | `false` | Master switch for the whole loop. |
| `spark.sparkx.autofix.mode` | `auto` | `auto` \| `learn` \| `shadow` \| `fix` (see [Modes](#modes)). |
| `spark.sparkx.autofix.store.path` | temp dir | Hadoop-compatible path for persisted profiles. |
| `spark.sparkx.autofix.maxIterations` | `5` | Hinted attempts before locking in the best set. |
| `spark.sparkx.autofix.broadcastMaxBytes` | `10485760` (10 MB) | A join side below this triggers a plain `BROADCAST`. |
| `spark.sparkx.autofix.targetPartitionBytes` | `134217728` (128 MB) | Desired bytes per shuffle partition when tuning repartition/coalesce. |

### Skew resolution

| Key | Default | Description |
|---|---|---|
| `spark.sparkx.autofix.skew.factor` | `10.0` | A join is skew-prone when `max(side)/min(side) ≥ this`. `≤ 1` **forces** skew handling on all joins. |
| `spark.sparkx.autofix.skew.broadcastMaxBytes` | `104857600` (100 MB) | Build side below this → split broadcast; above → salting. |
| `spark.sparkx.autofix.skew.saltFactor` | `16` | Number of salt buckets for a salted join. |
| `spark.sparkx.autofix.skew.targeted` | `true` | Discover hot keys and salt only those (targeted salting). Off → uniform salting, no sampling job. |
| `spark.sparkx.autofix.skew.sampleFraction` | `0.01` | Fraction of the skewed side sampled during hot-key discovery. |
| `spark.sparkx.autofix.skew.thresholdMultiplier` | `10.0` | A key is hot when its sampled freq `≥ median × this`. |
| `spark.sparkx.autofix.skew.maxKeys` | `100` | Cap on how many hot keys to salt. |

---

## Performance & safety

- **Read-only by default.** Nothing runs unless you enable the feature; nothing changes a plan unless
  the mode allows applying. Use `learn`/`shadow` to build profiles risk-free, then promote to `auto`.
- **The learner is normally job-free.** It inspects plan trees and catalog statistics already computed
  for the host query. The *only* extra Spark job is hot-key discovery, which is gated
  (`skew.targeted`), sampled (1%), fault-tolerant (falls back to uniform salting), and stops once the
  query converges.
- **Never regresses.** `HintPolicy` keeps the strictly-fastest measured run (baseline included), so a
  bad hint set is discarded rather than pinned.
- **Failures are swallowed.** Both `AutoFixRule` and `AutoFixLearner` wrap their work in try/catch —
  auto-fix can never fail or slow down the host job; on error it logs and leaves the plan untouched.
- **Idempotent.** Injected nodes are tagged, so repeated analyzer passes don't double-apply.

---

## Testing

- **Pure suites** (no SparkSession, run under `sbt testOnly`): `HintSuite`, `PlanHintsSuite`,
  `SparkXHintRuleSuite`, `PlanAdvisorSuite`, `FixProfileStoreSuite`, `SparkXConfigSuite`. Cover
  render/parse round-trips, structural rewrite shapes, hint selection bands, and profile JSON.
- **Execution-correctness suite** — `PlanHintsExecutionSuite` (real local `SparkSession`). Asserts full
  **row-set parity** vs a naive join for split-broadcast (inner + left-semi), uniform salt, and
  targeted salt — including duplicate-key multiplicity, cold-key correctness, and the SQL-text hint
  paths — plus that `SkewKeyDiscovery` finds the planted hot key.
- **Demo** — `com.sparkx.sample.SparkXAutoFixDemo skew` exercises the whole loop end-to-end on 20M
  skewed rows and prints per-run timings, injected/SQL-hint correctness, and the discovered hot key.

> On Windows/JDK17 the SparkSession-based suites are skipped under plain `sbt test`; run them via the
> raw-java recipe documented in the repo memory.

---

## Limitations

- Skew rewrites cover **equi-joins** only; split broadcast supports inner + left-semi, salting supports
  inner. Other join shapes are surfaced as advisory recommendations (applied via the DataFrame API),
  not injected.
- Targeted salting handles a **single** join key.
- Hot keys are discovered **once** and persisted as literals; if the data's skew distribution shifts,
  the stored hot-key set can go stale until the profile is re-learned.
- The runtime loop and the standalone `SparkXAutoFix` text facade use **different** fingerprint schemes
  and stores; they are not shared.
