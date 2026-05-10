# SparkX — Advanced Spark UI Extension

SparkX is a drop-in Spark plugin that detects performance problems and optimization opportunities in your Spark jobs and surfaces them as additional tabs inside the Spark Web UI — for both live applications and the Spark History Server. No code changes are needed in your application.

---

## What is SparkX?

SparkX analyzes runtime metrics collected by Spark's built-in `AppStatusStore` and physical SQL query plans to present actionable performance diagnostics directly inside the Spark UI. It adds a dedicated **SparkX** tab with sub-pages for:

- **Performance Issues** — 18 types of runtime problems detected from stage/task metrics
- **Optimization Suggestions** — 10 types of SQL query plan improvements
- **Root Cause Analysis** — groups co-occurring issues into 6 root-cause clusters with unified recommendations

---

## Performance Issues Detected

SparkX detects 18 types of performance issues from Spark's runtime metrics. Each issue includes a severity level, estimated time savings, and a detail page with per-stage breakdowns.

### Data Distribution Issues

#### Data Skew
| | |
|---|---|
| **What** | Max task duration is significantly larger than the median, indicating uneven data distribution across partitions. |
| **Severity** | 🔴 **Critical** when max/median ≥ threshold; 🟡 **Warning** otherwise |
| **Threshold** | `spark.sparkx.skewMultiplier` = `3.0` (max/median ratio) |
| **Savings** | Wall-clock: `max − median` task duration |
| **Detail page** | Skew |
| **How to fix** | • Salt skewed keys by appending a random suffix before groupBy/join, then aggregate results<br>• Repartition by a more uniform column<br>• Enable AQE skew join: `spark.sql.adaptive.skewJoin.enabled=true`<br>• Use `repartition(n, col)` to spread hot keys across more partitions |

#### Executor Memory Skew
| | |
|---|---|
| **What** | Memory usage across active executors is highly uneven (coefficient of variation). |
| **Severity** | 🟡 **Warning** |
| **Threshold** | `spark.sparkx.executorMemoryCov` = `0.5` (CoV), min executors `spark.sparkx.executorMemoryMinCount` = `3` |
| **Detail page** | Stability |
| **How to fix** | • Repartition data more evenly<br>• Ensure partitions are similarly sized<br>• Check for data skew in upstream stages |

### Memory & I/O Issues

#### GC Pressure
| | |
|---|---|
| **What** | JVM garbage collection consumes a high fraction of executor CPU time, suggesting heap pressure. |
| **Severity** | 🔴 **Critical** when GC ratio ≥ threshold; 🟡 **Warning** otherwise |
| **Threshold** | `spark.sparkx.gcRatioThreshold` = `0.10` (GC time / run time) |
| **Savings** | Compute: total GC time |
| **Detail page** | GC |
| **How to fix** | • Increase `spark.executor.memory`<br>• Reduce partition size with `repartition()`<br>• Filter data earlier in the pipeline<br>• Tune GC: use G1GC with `-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35`<br>• Reduce object creation in UDFs |

#### Shuffle Spill
| | |
|---|---|
| **What** | Intermediate shuffle data overflows executor memory and is written to disk, causing I/O overhead. |
| **Severity** | 🟡 **Warning** |
| **Threshold** | Any disk spill > 0 bytes |
| **Savings** | I/O: estimated from spill volume |
| **Detail page** | Spill |
| **How to fix** | • Increase `spark.executor.memory` or `spark.memory.fraction`<br>• Increase partition count to reduce per-partition data size<br>• Filter data earlier to reduce shuffle volume<br>• Use `spark.sql.shuffle.partitions` to increase partition count for SQL |

#### Disk Shuffle Read
| | |
|---|---|
| **What** | Remote shuffle data is read to disk instead of memory, indicating insufficient memory for shuffle buffers. |
| **Severity** | 🟡 **Warning** |
| **Threshold** | `spark.sparkx.diskShuffleReadMinMB` = `100` MB |
| **Savings** | I/O: estimated from disk read volume |
| **Detail page** | Spill |
| **How to fix** | • Increase `spark.reducer.maxSizeInFlight` (default 48 MB)<br>• Increase executor memory<br>• Reduce shuffle data volume with earlier filtering or aggregation |

#### Large Broadcast
| | |
|---|---|
| **What** | Broadcast variables larger than the configured threshold increase driver memory pressure and network overhead. |
| **Severity** | 🟡 **Warning** |
| **Threshold** | `spark.sparkx.broadcastSizeMB` = `200` MB |
| **Detail page** | Broadcast |
| **How to fix** | • Reduce broadcast variable size by filtering or pre-aggregating<br>• Increase `spark.driver.memory` if broadcast is necessary<br>• Consider using a join instead of a broadcast variable for very large lookups |

### Task Execution Issues

#### Straggler Tasks
| | |
|---|---|
| **What** | Individual tasks run far longer than the rest of the stage, computed using the IQR (interquartile range) fence. |
| **Severity** | 🟡 **Warning** |
| **Threshold** | `spark.sparkx.stragglerIQRFactor` = `1.5` (max > Q3 + factor × IQR) |
| **Savings** | Wall-clock: `max − threshold` duration |
| **Detail page** | Stragglers |
| **How to fix** | • Check for data skew (often the root cause)<br>• Enable speculation: `spark.speculation=true`<br>• Investigate slow executor nodes (disk, network, co-tenancy)<br>• Increase partition count to reduce per-task work |

#### Small Tasks (Over-partitioned)
| | |
|---|---|
| **What** | Too many tiny tasks waste scheduling overhead. |
| **Severity** | 🟡 **Warning** |
| **Threshold** | `spark.sparkx.smallTaskMinCount` = `100` tasks AND `spark.sparkx.smallTaskMedianMs` = `200` ms median |
| **Savings** | Scheduling overhead: total scheduler delay |
| **Detail page** | Partitioning |
| **How to fix** | • Use `coalesce()` to reduce partition count<br>• Increase `spark.sql.files.maxPartitionBytes`<br>• Increase `spark.default.parallelism` |

#### Under-partitioned Stage
| | |
|---|---|
| **What** | Number of tasks is significantly less than available cores, leaving CPUs idle. |
| **Severity** | 🟡 **Warning** |
| **Threshold** | `spark.sparkx.underPartitionRatio` = `0.5` (tasks < cores × ratio) |
| **Detail page** | Partitioning |
| **How to fix** | • Use `repartition(n)` to increase partitions<br>• Raise `spark.sql.shuffle.partitions`<br>• Tune `spark.default.parallelism` to match cluster cores |

#### Low CPU Utilization
| | |
|---|---|
| **What** | CPU time is much lower than wall-clock time, indicating tasks are waiting on I/O or network. |
| **Severity** | 🔴 **Critical** when CPU ratio < threshold; 🟡 **Warning** otherwise |
| **Threshold** | `spark.sparkx.lowCpuRatioThreshold` = `0.5`, min runtime `spark.sparkx.lowCpuMinRunTimeMs` = `60000` ms |
| **Savings** | Compute: `runTime − cpuTime` |
| **Detail page** | Partitioning |
| **How to fix** | • Check for I/O bottlenecks (slow storage, network)<br>• Increase parallelism to utilize idle cores<br>• Use faster storage (SSD, local disk) or compression<br>• Check for lock contention in UDFs |

### Shuffle & Network Issues

#### Shuffle Amplification
| | |
|---|---|
| **What** | Shuffle write bytes are disproportionately larger than input bytes, suggesting unnecessary data expansion. |
| **Severity** | 🔴 **Critical** when ratio ≥ threshold; 🟡 **Warning** otherwise |
| **Threshold** | `spark.sparkx.shuffleAmplifyRatio` = `5.0` (shuffle write / input ratio) |
| **Detail page** | Partitioning |
| **How to fix** | • Pre-aggregate before shuffling<br>• Select only needed columns before joins<br>• Use `reduceByKey` instead of `groupByKey` |

#### High Shuffle Fetch Wait
| | |
|---|---|
| **What** | Tasks spend significant time waiting for shuffle data from other executors. |
| **Severity** | 🟡 **Warning** |
| **Threshold** | `spark.sparkx.fetchWaitRatioThreshold` = `0.2` (fetch wait / run time ratio) |
| **Savings** | Wall-clock: median fetch wait time |
| **Detail page** | Stability |
| **How to fix** | • Check network bandwidth between executors<br>• Reduce shuffle data volume<br>• Increase `spark.reducer.maxSizeInFlight`<br>• Enable external shuffle service: `spark.shuffle.service.enabled=true` |

### Scheduling & Serialization Issues

#### High Scheduler Delay
| | |
|---|---|
| **What** | Task scheduling overhead is high relative to task execution time. |
| **Severity** | 🟡 **Warning** |
| **Threshold** | `spark.sparkx.highSchedulerDelayMs` = `500` ms AND `spark.sparkx.schedulerDelayRatio` = `0.5` |
| **Savings** | Scheduling overhead: `P95 delay × task count` |
| **Detail page** | Partitioning |
| **How to fix** | • Reduce task count with `coalesce()`<br>• Increase task granularity so scheduling is a smaller fraction<br>• Check driver for CPU/memory pressure |

#### High Task Deserialization
| | |
|---|---|
| **What** | Tasks spend significant time deserializing closures and dependencies. |
| **Severity** | 🟡 **Warning** |
| **Threshold** | `spark.sparkx.highDeserMs` = `200` ms (P95) |
| **Savings** | Compute: `P95 deser × task count` |
| **Detail page** | Stability |
| **How to fix** | • Enable Kryo serialization: `spark.serializer=org.apache.spark.serializer.KryoSerializer`<br>• Reduce closure size — avoid capturing large driver objects in lambdas<br>• Register frequently-used classes with `spark.kryo.classesToRegister` |

#### Slow Result Serialization
| | |
|---|---|
| **What** | Tasks spend significant time serializing results back to the driver. |
| **Severity** | 🟡 **Warning** |
| **Threshold** | `spark.sparkx.resultSerializationMs` = `200` ms (P95) |
| **Savings** | Compute: `P95 serialization × task count` |
| **Detail page** | Stability |
| **How to fix** | • Enable Kryo serialization<br>• Reduce result size — avoid returning large objects from tasks<br>• Use `write()` instead of `collect()` for large outputs |

#### Large Task Result
| | |
|---|---|
| **What** | Task result sizes are unusually large, increasing driver memory pressure. |
| **Severity** | 🟡 **Warning** |
| **Threshold** | `spark.sparkx.largeResultMB` = `50` MB (P95) |
| **Detail page** | Stability |
| **How to fix** | • Avoid returning large objects from tasks<br>• Use accumulators for aggregation instead of collecting results<br>• Increase `spark.driver.maxResultSize` if results are expected to be large |

### Reliability Issues

#### Task Failures
| | |
|---|---|
| **What** | Tasks in a stage have failed, wasting compute resources on retries. |
| **Severity** | 🔴 **Critical** |
| **Savings** | Compute: `failed tasks × average task duration` |
| **Detail page** | Stability |
| **How to fix** | • Check executor logs for OOM, disk full, or serialization errors<br>• Increase executor memory if OOM<br>• Check for non-deterministic UDFs that fail on specific data<br>• Review Spark's blacklisting settings if specific nodes fail repeatedly |

#### Stage Retry
| | |
|---|---|
| **What** | A stage was retried (multiple attempts), indicating a complete stage failure. |
| **Severity** | 🔴 **Critical** |
| **Savings** | Compute: `attempts × tasks × average task duration` |
| **Detail page** | Stability |
| **How to fix** | • Investigate executor logs for the root cause (OOM, fetch failures, node loss)<br>• Increase `spark.executor.memory` or `spark.executor.memoryOverhead`<br>• Enable external shuffle service to survive executor failures<br>• Tune `spark.task.maxFailures` (default 4) |

#### Speculative Tasks
| | |
|---|---|
| **What** | Spark launched speculative copies of slow tasks. While speculation helps latency, the duplicate tasks consume extra resources. |
| **Severity** | 🟡 **Warning** |
| **Savings** | Compute: `speculative tasks × average task duration` |
| **Detail page** | Stability |
| **How to fix** | • Investigate why tasks are slow (data skew, GC, slow nodes)<br>• Tune `spark.speculation.quantile` and `spark.speculation.multiplier`<br>• If speculation is not needed, disable with `spark.speculation=false` |

---

## Optimization Suggestions

SparkX analyzes SQL physical query plans to detect 10 types of optimization opportunities. These appear under the **Suggestions** sub-page.

### 🔴 Critical Suggestions

#### Cartesian Product
| | |
|---|---|
| **What** | Query uses a `CartesianProduct` or `BroadcastNestedLoopJoin` which produces a cross-product of all rows — O(N×M) complexity. |
| **When detected** | Any cartesian or broadcast nested loop join node in the query plan |
| **How to fix** | • Add a join condition: `df1.join(df2, df1("key") === df2("key"))`<br>• If the cross-product is intentional on small data, use `crossJoin()` explicitly<br>• Check for missing `WHERE` clauses or accidental joins without conditions |

#### Broadcast Join Candidate
| | |
|---|---|
| **What** | A sort-merge or shuffled-hash join has one side small enough to broadcast, but Spark didn't auto-broadcast it (likely missing table statistics). |
| **Severity** | 🔴 **Critical** if small side < 10 MB; 🟡 **Warning** if 10–100 MB |
| **Threshold** | `spark.sparkx.suggestion.broadcastThresholdBytes` = `100 MB` |
| **How to fix** | • Add a broadcast hint: `df.join(broadcast(smallDf), ...)`<br>• Compute table statistics: `ANALYZE TABLE tableName COMPUTE STATISTICS`<br>• Increase auto-broadcast threshold: `spark.sql.autoBroadcastJoinThreshold` (default 10 MB) |

#### Collect on Large Dataset
| | |
|---|---|
| **What** | A `collect()` or `take()` operation pulls data to the driver on stages processing large volumes. Risks driver OOM. |
| **Severity** | 🔴 **Critical** if input > 1 GB; 🟡 **Warning** if 100 MB–1 GB |
| **Threshold** | `spark.sparkx.suggestion.collectLargeDataMinMB` = `100` MB |
| **How to fix** | • Use `.show()`, `.take(n)`, or `.toLocalIterator()` for sampling<br>• Write directly to storage: `df.write.parquet("path")`<br>• Use `foreach()` or `foreachPartition()` for side effects |

### 🟡 Warning Suggestions

#### Excessive Shuffles
| | |
|---|---|
| **What** | SQL execution has many Exchange (shuffle) nodes. Some may be redundant if data is already co-partitioned. |
| **Threshold** | `spark.sparkx.suggestion.excessiveShuffleCount` = `4` exchanges |
| **How to fix** | • Review the query plan for unnecessary repartitions<br>• Use `repartition()` once before joins rather than multiple times<br>• Enable AQE which can coalesce redundant shuffles<br>• Pre-partition data on join keys when writing to storage |

#### AQE Not Enabled
| | |
|---|---|
| **What** | Adaptive Query Execution is disabled for an execution that has shuffles. AQE dynamically optimizes joins, partition sizes, and skew at runtime. |
| **Severity** | 🟡 **Warning** if co-occurring performance issues exist; ℹ️ **Info** otherwise |
| **How to fix** | • Set `spark.sql.adaptive.enabled=true` (default since Spark 3.2)<br>• AQE can auto-convert sort-merge joins to broadcast joins<br>• AQE coalesces small shuffle partitions automatically<br>• AQE handles skew joins at runtime |

#### Missing Partition Pruning
| | |
|---|---|
| **What** | A table scan has filters (pushed/data filters) but no partition filters. The full partition listing is read. |
| **How to fix** | • Partition your data by frequently filtered columns:<br>&nbsp;&nbsp;`df.write.partitionBy("date").parquet("path")`<br>&nbsp;&nbsp;or `PARTITIONED BY (date)` in DDL<br>• This lets Spark skip reading irrelevant partitions entirely<br>• Works best with high-cardinality filter columns (dates, regions) |

#### Repeated Table Scan
| | |
|---|---|
| **What** | The same data source is scanned multiple times in one execution, re-reading data from storage each time. |
| **How to fix** | • Cache the shared DataFrame before reuse:<br>&nbsp;&nbsp;`val cached = df.cache(); cached.count()` (materialize)<br>• Use `cached` in subsequent operations<br>• Call `.unpersist()` when done<br>• Consider `CACHE TABLE` for SQL queries |

#### Python UDF Detected
| | |
|---|---|
| **What** | Query uses Python UDFs which serialize rows to Python and back, breaking Spark's whole-stage code generation. |
| **How to fix** | • Replace with native Spark SQL functions where possible<br>• Use Pandas UDFs (`@pandas_udf`) which operate on Arrow batches — 10–100× faster<br>• For complex logic, use `mapInArrow()` (Spark 3.3+)<br>• Consider writing UDFs in Scala/Java if performance is critical |

#### Shuffle Partition Tuning
| | |
|---|---|
| **What** | The number of shuffle partitions (default 200) is significantly mismatched for the actual data volume — either too many (tiny tasks) or too few (large tasks with spill). |
| **How to fix** | • Set `spark.sql.shuffle.partitions` based on data volume (target ~128 MB per partition)<br>• Enable AQE (`spark.sql.adaptive.enabled=true`) which auto-coalesces partitions at runtime<br>• For varying workloads, AQE is preferred over a fixed partition count |

### ℹ️ Info Suggestions

#### Suboptimal File Format
| | |
|---|---|
| **What** | Reading data in CSV, JSON, or TEXT format. Row-based formats lack columnar compression, predicate pushdown, and column pruning. |
| **How to fix** | • Convert to Parquet or ORC for 2–5× faster reads and smaller storage:<br>&nbsp;&nbsp;`df.write.parquet("path")` or `CREATE TABLE ... USING PARQUET`<br>• Parquet supports predicate pushdown — filters are applied during scan<br>• Columnar format enables column pruning — only needed columns are read |

---

## Root Cause Analysis

SparkX groups co-occurring issues by stage into **root-cause clusters**, providing a unified diagnosis instead of listing individual symptoms. Uses conservative savings aggregation (`max` within a cluster) to avoid double-counting.

| Cluster | Primary Issue | Co-occurring Issues | Recommendation |
|---|---|---|---|
| **Uneven Data Distribution** | Data Skew | Straggler Tasks, Shuffle Spill, Executor Memory Skew, GC Pressure | Salt skewed keys, repartition by a more uniform column, or enable AQE skew joins (`spark.sql.adaptive.skewJoin.enabled=true`). |
| **Memory Pressure** | GC Pressure | Shuffle Spill, Large Broadcast, Disk Shuffle Read | Increase `spark.executor.memory`, reduce partition size with `repartition()`, or filter data earlier. |
| **Over-partitioned Workload** | Small Tasks | High Scheduler Delay | Use `coalesce()` to reduce partition count, increase `spark.sql.files.maxPartitionBytes`. |
| **Under-parallelized Workload** | Under-partitioned Stage | Low CPU Utilization, Straggler Tasks | Increase partition count with `repartition()`, raise `spark.sql.shuffle.partitions`. |
| **Serialization Overhead** | High Task Deserialization | Large Task Result, Slow Result Serialization | Enable Kryo serialization, reduce closure size, prefer `write()` over `collect()`. |
| **Node / Executor Instability** | Task Failures | Speculative Tasks, Stage Retry | Investigate executor logs for OOM/disk/network issues, enable `spark.speculation`, increase executor memory headroom. |

---

## Integration

### Live Application

Add the following flags to your `spark-submit` command:

```bash
spark-submit \
  --jars /path/to/sparkx-assembly-0.1.0.jar \
  --conf spark.extraListeners=com.sparkx.SparkXListener \
  --class com.example.MyApp \
  my-app.jar
```

Or add them permanently to `$SPARK_HOME/conf/spark-defaults.conf`:

```
spark.jars            /path/to/sparkx-assembly-0.1.0.jar
spark.extraListeners  com.sparkx.SparkXListener
```

Once the application starts, navigate to the Spark Web UI (default: `http://driver-host:4040`) and click the **SparkX** tab.

### Spark History Server

1. Copy the assembled JAR to Spark's jars directory:

   ```bash
   cp sparkx-assembly-0.1.0.jar $SPARK_HOME/jars/
   ```

2. Restart the History Server:

   ```bash
   $SPARK_HOME/sbin/stop-history-server.sh
   $SPARK_HOME/sbin/start-history-server.sh
   ```

3. Open any completed application in the History Server UI — the **SparkX** tab will appear automatically.

The plugin is registered via Java SPI (`META-INF/services/org.apache.spark.status.AppHistoryServerPlugin`), so no configuration changes are needed.

---

## Configuration Reference

All thresholds are configurable via `SparkConf` or `spark-defaults.conf`.

### Performance Issue Thresholds

| Key | Default | Description |
|---|---|---|
| `spark.sparkx.skewMultiplier` | `3.0` | Flag a stage as skewed when `max task duration / median` exceeds this. |
| `spark.sparkx.gcRatioThreshold` | `0.10` | Flag when `GC time / run time` exceeds this ratio (10%). |
| `spark.sparkx.stragglerIQRFactor` | `1.5` | IQR fence multiplier. Tasks exceeding `Q3 + factor × IQR` are stragglers. |
| `spark.sparkx.broadcastSizeMB` | `200` | Broadcast variables exceeding this size (MB) are flagged. |
| `spark.sparkx.smallTaskMinCount` | `100` | Minimum task count before over-partitioning is flagged. |
| `spark.sparkx.smallTaskMedianMs` | `200` | Median task time (ms) below which tasks are considered too small. |
| `spark.sparkx.underPartitionRatio` | `0.5` | Flag when `task count < cores × ratio`. |
| `spark.sparkx.shuffleAmplifyRatio` | `5.0` | Flag when `shuffle write / input` exceeds this ratio. |
| `spark.sparkx.largeResultMB` | `50` | Flag when P95 task result size exceeds this (MB). |
| `spark.sparkx.fetchWaitRatioThreshold` | `0.2` | Flag when `fetch wait / run time` exceeds this ratio. |
| `spark.sparkx.highDeserMs` | `200` | Flag when P95 task deserialization exceeds this (ms). |
| `spark.sparkx.lowCpuRatioThreshold` | `0.5` | Flag when `CPU time / run time` is below this. |
| `spark.sparkx.lowCpuMinRunTimeMs` | `60000` | Minimum stage run time (ms) before low CPU is flagged. |
| `spark.sparkx.diskShuffleReadMinMB` | `100` | Minimum disk shuffle read (MB) before flagging. |
| `spark.sparkx.highSchedulerDelayMs` | `500` | Flag when P95 scheduler delay exceeds this (ms). |
| `spark.sparkx.schedulerDelayRatio` | `0.5` | Flag when `P95 delay / P50 run time` exceeds this. |
| `spark.sparkx.resultSerializationMs` | `200` | Flag when P95 result serialization exceeds this (ms). |
| `spark.sparkx.executorMemoryCov` | `0.5` | Flag when executor memory CoV exceeds this. |
| `spark.sparkx.executorMemoryMinCount` | `3` | Minimum active executors before memory skew is checked. |

### Suggestion Thresholds

| Key | Default | Description |
|---|---|---|
| `spark.sparkx.suggestion.broadcastThresholdBytes` | `104857600` (100 MB) | Suggest broadcast when join side is smaller than this. |
| `spark.sparkx.suggestion.excessiveShuffleCount` | `4` | Suggest shuffle review when exchange count exceeds this. |
| `spark.sparkx.suggestion.partitionPruneScanMinMB` | `1024` | Minimum scan size (MB) before missing partition pruning is flagged. |
| `spark.sparkx.suggestion.collectLargeDataMinMB` | `100` | Flag collect operations on datasets larger than this (MB). |

### Example Override

```bash
spark-submit \
  --conf spark.sparkx.skewMultiplier=5.0 \
  --conf spark.sparkx.gcRatioThreshold=0.15 \
  --conf spark.sparkx.broadcastSizeMB=500 \
  --conf spark.sparkx.suggestion.broadcastThresholdBytes=209715200 \
  ...
```

---

## Building from Source

Requires Java 8+, Scala 2.12, and [sbt](https://www.scala-sbt.org/) 1.9+.

```bash
git clone https://github.com/tiongl/sparkx.git
cd sparkx
sbt compile                  # compile the plugin
sbt test                     # run tests (68 test cases)
sbt assembly                 # build the sparkx plugin JAR
sbt "sample/assembly"        # build the demo application JAR
```

The fat JARs will be written to:

```
target/scala-2.12/sparkx-assembly-0.1.0.jar
sample/target/scala-2.12/sparkx-sample-assembly-0.1.0.jar
```

The plugin JAR excludes Scala and Spark (marked `provided`), so it is safe to add to `$SPARK_HOME/jars/` without version conflicts.

---

## Demo / Showcase

The `sample/` subproject contains prebuilt scenarios — one per detectable issue category — with a launcher script that **automatically downloads Spark 3.5.0** if it is not already available.

### Running the demo

**Linux / macOS:**
```bash
./run-demo.sh                     # run all scenarios
./run-demo.sh skew --pause        # data-skew only, pause so you can browse the UI
./run-demo.sh straggler --pause   # straggler only
```

**Windows:**
```bat
run-demo.bat                      # run all scenarios
run-demo.bat broadcast --pause    # broadcast only, pause at UI
```

The scripts will:
1. Check that Java 8+ is on `PATH`
2. Download and extract Spark 3.5.0 into `.spark-dist/` if `SPARK_HOME` is not set (cached after first run)
3. Launch the demo app in `local[*]` mode with the SparkX plugin attached
4. Print the Spark UI URL (`http://localhost:4040/sparkx`)

### Performance Issue Scenarios

| Name | Argument | What triggers | SparkX page |
|---|---|---|---|
| Data Skew | `skew` | 95% of 1 M records share one key → one reducer processes ~190× more | Skew |
| Straggler Tasks | `straggler` | 2 of 20 tasks sleep 6 s; the rest finish in < 200 ms | Stragglers |
| GC Pressure | `gc` | Tasks allocate millions of throw-away strings to stress GC | GC |
| Shuffle Spill | `spill` | 3 M large-value records grouped into 4 partitions | Spill |
| Large Broadcast | `broadcast` | 2 M-entry map (~100 MB) broadcast, threshold lowered to 50 MB | Broadcast |

### Suggestion Scenarios

The `suggestion` scenario runs 7 steps that trigger optimization suggestions (AQE is temporarily disabled to showcase opportunities):

| Step | What triggers | Suggestion type |
|---|---|---|
| Write/read CSV data | CSV file scan | Suboptimal File Format |
| Sort-merge join on small table | Join without broadcast | Broadcast Join Candidate |
| Multiple shuffles in one query | 5+ exchanges | Excessive Shuffles |
| Cross-join | Cartesian product node | Cartesian Product |
| Read same CSV table twice | Duplicate scans | Repeated Table Scan |
| Collect large dataset | collect() on 100+ MB | Collect on Large Dataset |
| Query with 200 partitions on small data | Partition mismatch | Shuffle Partition Tuning |

Detection thresholds are automatically lowered for the demo to ensure issues are reliably flagged on any hardware.

---

## How it Works

### Data Source: `AppStatusStore`

All metrics are read from Spark's internal `AppStatusStore`, which is populated in real time by Spark's event bus. This store is available in:

- **Live applications** — via `SparkContext.ui.store`
- **History Server** — via the replayed event log store

SparkX never modifies or intercepts Spark's data pipeline; it is purely a read-only consumer.

### Live Application Integration: `SparkListener`

`SparkXListener` implements `SparkListener` and is registered via `spark.extraListeners`. When the application starts (`onApplicationStart`), it retrieves the live `SparkUI` instance and attaches a new `SparkXTab` to it. From that point forward, each request to a SparkX page re-queries the `AppStatusStore` for the latest metrics.

### History Server Integration: `SparkHistoryServerPlugin`

`SparkXHistoryPlugin` implements `SparkHistoryServerPlugin` and is discovered automatically via Java's `ServiceLoader` mechanism (SPI). When the History Server replays an event log and reconstructs the `SparkUI` for a completed application, it calls `setupUI(ui)` on each registered plugin. SparkX attaches its `SparkXTab` at that point.

### Suggestion Detection: Physical Plan Analysis

The `SuggestionDetector` analyzes SQL physical query plans (from `SQLAppStatusStore`) using the `PlanParser`, which extracts structured information about joins, scans, exchanges, cartesian products, and other operators. The parser handles both Spark 3.5's formatted plan syntax (numbered nodes with separate tree and detail sections) and older single-line formats.

Detection results are cached with a 30-second TTL to avoid re-parsing on every page refresh.

### UI Architecture

Because Spark's `SparkUITab`, `WebUIPage`, and `UIUtils` are `private[spark]`, the UI classes must reside in the `org.apache.spark.*` package hierarchy. Business logic (issue detection, configuration, formatting utilities) lives in `com.sparkx.*` and has no such restriction.

```
com.sparkx.SparkXListener                        ← spark.extraListeners hook
com.sparkx.SparkXConfig                          ← threshold configuration
com.sparkx.Utils                                 ← byte/duration formatting
com.sparkx.analysis.PerformanceIssue             ← sealed ADT of 18 issue types
com.sparkx.analysis.OptimizationSuggestion       ← sealed ADT of 10 suggestion types
com.sparkx.analysis.PlanParser                   ← SQL physical plan parser
com.sparkx.analysis.RootCauseAnalyzer            ← groups issues into root-cause clusters

org.apache.spark.ui.sparkx.SparkXTab             ← SparkUITab, owns sub-pages
org.apache.spark.ui.sparkx.OverviewPage          ← /sparkx (issues + suggestions summary)
org.apache.spark.ui.sparkx.SkewPage              ← /sparkx/skew
org.apache.spark.ui.sparkx.GCPage               ← /sparkx/gc
org.apache.spark.ui.sparkx.SpillPage             ← /sparkx/spill
org.apache.spark.ui.sparkx.StragglerPage         ← /sparkx/stragglers
org.apache.spark.ui.sparkx.BroadcastPage         ← /sparkx/broadcast
org.apache.spark.ui.sparkx.PartitioningPage      ← /sparkx/partitioning
org.apache.spark.ui.sparkx.StabilityPage         ← /sparkx/stability
org.apache.spark.ui.sparkx.RootCausePage         ← /sparkx/rootcause
org.apache.spark.ui.sparkx.StagesSummaryPage     ← /sparkx/stages
org.apache.spark.ui.sparkx.SuggestionsPage       ← /sparkx/suggestions
org.apache.spark.ui.sparkx.IssueDetector         ← queries AppStatusStore for issues
org.apache.spark.ui.sparkx.SuggestionDetector    ← queries SQL plans for suggestions
org.apache.spark.ui.sparkx.SparkXHistoryPlugin   ← SPI entry point for History Server
```
