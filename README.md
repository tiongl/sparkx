# sparkx — Advanced Spark UI Extension

sparkx is a drop-in Spark plugin that detects common performance problems in your Spark jobs and surfaces them as additional tabs inside the Spark Web UI — for both live applications and the Spark History Server. No code changes are needed in your application.

---

## What is sparkx?

sparkx analyzes runtime metrics collected by Spark's built-in `AppStatusStore` and presents actionable performance diagnostics directly inside the Spark UI. It adds a dedicated **sparkx** tab with sub-pages for each category of issue: data skew, GC pressure, shuffle spill, straggler tasks, and oversized broadcast variables.

---

## Performance Issues Detected

| Issue | Description | Default Threshold |
|---|---|---|
| **Data Skew** | Max task duration is significantly larger than the median, indicating uneven data distribution across partitions. | max/median > 3× |
| **GC Pressure** | JVM garbage collection consumes a high fraction of executor CPU time, suggesting heap pressure. | GC time > 10% of run time |
| **Shuffle Spill** | Intermediate shuffle data overflows executor memory and is written to disk, causing I/O overhead. | Any disk spill > 0 bytes |
| **Straggler Tasks** | Individual tasks run far longer than the rest of the stage, computed using the IQR (interquartile range) fence. | max > Q3 + 1.5 × IQR |
| **Large Broadcasts** | Broadcast variables larger than the configured threshold increase driver memory pressure and network overhead. | > 200 MB |

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

Once the application starts, navigate to the Spark Web UI (default: `http://driver-host:4040`) and click the **sparkx** tab.

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

3. Open any completed application in the History Server UI — the **sparkx** tab will appear automatically.

The plugin is registered via Java SPI (`META-INF/services/org.apache.spark.deploy.history.SparkHistoryServerPlugin`), so no configuration changes are needed.

---

## Configuration Reference

All thresholds are configurable via `SparkConf` or `spark-defaults.conf`:

| Key | Default | Description |
|---|---|---|
| `spark.sparkx.skewMultiplier` | `3.0` | Flag a stage as skewed when `max task duration / median task duration` exceeds this multiplier. |
| `spark.sparkx.gcRatioThreshold` | `0.10` | Flag a stage or executor when `JVM GC time / executor run time` exceeds this ratio (10% by default). |
| `spark.sparkx.stragglerIQRFactor` | `1.5` | Multiplier for the IQR fence. Tasks exceeding `Q3 + factor × IQR` are flagged as stragglers. |
| `spark.sparkx.broadcastSizeMB` | `200` | Broadcast variables whose in-memory size exceeds this threshold (in MB) are flagged. |

Example override:

```bash
spark-submit \
  --conf spark.sparkx.skewMultiplier=5.0 \
  --conf spark.sparkx.gcRatioThreshold=0.15 \
  --conf spark.sparkx.broadcastSizeMB=500 \
  ...
```

---

## Building from Source

Requires Java 8+, Scala 2.12, and [sbt](https://www.scala-sbt.org/) 1.9+.

```bash
git clone https://github.com/your-org/sparkx.git
cd sparkx
sbt assembly                 # builds the sparkx plugin JAR
sbt "sample/assembly"        # builds the demo application JAR
```

The fat JARs will be written to:

```
target/scala-2.12/sparkx-assembly-0.1.0.jar
sample/target/scala-2.12/sparkx-sample-assembly-0.1.0.jar
```

The plugin JAR excludes Scala and Spark (marked `provided`), so it is safe to add to `$SPARK_HOME/jars/` without version conflicts.

---

## Demo / Showcase

The `sample/` subproject contains five prebuilt scenarios — one per detectable issue — with a launcher script that **automatically downloads Spark 3.5.0** if it is not already available.

### Running the demo

**Linux / macOS:**
```bash
./run-demo.sh                     # run all 5 scenarios
./run-demo.sh skew --pause        # data-skew only, pause so you can browse the UI
./run-demo.sh straggler --pause   # straggler only
```

**Windows:**
```bat
run-demo.bat                      # run all 5 scenarios
run-demo.bat broadcast --pause    # broadcast only, pause at UI
```

The scripts will:
1. Check that Java 8+ is on `PATH`
2. Download and extract Spark 3.5.0 into `.spark-dist/` if `SPARK_HOME` is not set (cached after first run)
3. Launch the demo app in `local[*]` mode with the sparkx plugin attached
4. Print the Spark UI URL (`http://localhost:4040/sparkx`)

### Scenarios

| Name | Argument | What triggers | sparkx tab |
|---|---|---|---|
| Data Skew | `skew` | 95% of 1 M records share one key → one reducer processes ~190× more | Skew |
| Straggler Tasks | `straggler` | 2 of 20 tasks sleep 6 s; the rest finish in < 200 ms | Stragglers |
| GC Pressure | `gc` | Tasks allocate millions of throw-away strings to stress GC | GC |
| Shuffle Spill | `spill` | 3 M large-value records grouped into 4 partitions | Spill |
| Large Broadcast | `broadcast` | 2 M-entry map (~100 MB) broadcast, threshold lowered to 50 MB | Broadcast |

Detection thresholds are automatically lowered for the demo (e.g. `gcRatioThreshold=1%`, `broadcastSizeMB=50`) to ensure issues are reliably flagged on any hardware.

---

## How it Works

### Data Source: `AppStatusStore`

All metrics are read from Spark's internal `AppStatusStore`, which is populated in real time by Spark's event bus. This store is available in:

- **Live applications** — via `SparkContext.ui.store`
- **History Server** — via the replayed event log store

sparkx never modifies or intercepts Spark's data pipeline; it is purely a read-only consumer.

### Live Application Integration: `SparkListener`

`SparkXListener` implements `SparkListener` and is registered via `spark.extraListeners`. When the application starts (`onApplicationStart`), it retrieves the live `SparkUI` instance and attaches a new `SparkXTab` to it. From that point forward, each request to an sparkx page re-queries the `AppStatusStore` for the latest metrics.

### History Server Integration: `SparkHistoryServerPlugin`

`SparkXHistoryPlugin` implements `SparkHistoryServerPlugin` and is discovered automatically via Java's `ServiceLoader` mechanism (SPI). When the History Server replays an event log and reconstructs the `SparkUI` for a completed application, it calls `setupUI(ui)` on each registered plugin. sparkx attaches its `SparkXTab` at that point.

### UI Architecture

Because Spark's `SparkUITab`, `WebUIPage`, and `UIUtils` are `private[spark]`, the UI classes must reside in the `org.apache.spark.*` package hierarchy. Business logic (issue detection, configuration, formatting utilities) lives in `com.sparkx.*` and has no such restriction.

```
com.sparkx.SparkXListener          ← spark.extraListeners hook
com.sparkx.SparkXConfig            ← threshold configuration
com.sparkx.Utils                  ← byte/duration formatting
com.sparkx.analysis.IssueDetector ← queries AppStatusStore, returns issues
com.sparkx.analysis.PerformanceIssue ← sealed ADT of issue types

org.apache.spark.ui.sparkx.SparkXTab      ← SparkUITab, owns sub-pages
org.apache.spark.ui.sparkx.OverviewPage  ← /sparkx
org.apache.spark.ui.sparkx.SkewPage      ← /sparkx/skew
org.apache.spark.ui.sparkx.GCPage        ← /sparkx/gc
org.apache.spark.ui.sparkx.SpillPage     ← /sparkx/spill
org.apache.spark.ui.sparkx.StragglerPage ← /sparkx/stragglers
org.apache.spark.ui.sparkx.BroadcastPage ← /sparkx/broadcast
org.apache.spark.deploy.history.SparkXHistoryPlugin ← SPI entry point
```
