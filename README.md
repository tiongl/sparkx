# xpark — Advanced Spark UI Extension

xpark is a drop-in Spark plugin that detects common performance problems in your Spark jobs and surfaces them as additional tabs inside the Spark Web UI — for both live applications and the Spark History Server. No code changes are needed in your application.

---

## What is xpark?

xpark analyzes runtime metrics collected by Spark's built-in `AppStatusStore` and presents actionable performance diagnostics directly inside the Spark UI. It adds a dedicated **xpark** tab with sub-pages for each category of issue: data skew, GC pressure, shuffle spill, straggler tasks, and oversized broadcast variables.

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
  --jars /path/to/xpark-assembly-0.1.0.jar \
  --conf spark.extraListeners=com.xpark.XParkListener \
  --class com.example.MyApp \
  my-app.jar
```

Or add them permanently to `$SPARK_HOME/conf/spark-defaults.conf`:

```
spark.jars            /path/to/xpark-assembly-0.1.0.jar
spark.extraListeners  com.xpark.XParkListener
```

Once the application starts, navigate to the Spark Web UI (default: `http://driver-host:4040`) and click the **xpark** tab.

### Spark History Server

1. Copy the assembled JAR to Spark's jars directory:

   ```bash
   cp xpark-assembly-0.1.0.jar $SPARK_HOME/jars/
   ```

2. Restart the History Server:

   ```bash
   $SPARK_HOME/sbin/stop-history-server.sh
   $SPARK_HOME/sbin/start-history-server.sh
   ```

3. Open any completed application in the History Server UI — the **xpark** tab will appear automatically.

The plugin is registered via Java SPI (`META-INF/services/org.apache.spark.deploy.history.SparkHistoryServerPlugin`), so no configuration changes are needed.

---

## Configuration Reference

All thresholds are configurable via `SparkConf` or `spark-defaults.conf`:

| Key | Default | Description |
|---|---|---|
| `spark.xpark.skewMultiplier` | `3.0` | Flag a stage as skewed when `max task duration / median task duration` exceeds this multiplier. |
| `spark.xpark.gcRatioThreshold` | `0.10` | Flag a stage or executor when `JVM GC time / executor run time` exceeds this ratio (10% by default). |
| `spark.xpark.stragglerIQRFactor` | `1.5` | Multiplier for the IQR fence. Tasks exceeding `Q3 + factor × IQR` are flagged as stragglers. |
| `spark.xpark.broadcastSizeMB` | `200` | Broadcast variables whose in-memory size exceeds this threshold (in MB) are flagged. |

Example override:

```bash
spark-submit \
  --conf spark.xpark.skewMultiplier=5.0 \
  --conf spark.xpark.gcRatioThreshold=0.15 \
  --conf spark.xpark.broadcastSizeMB=500 \
  ...
```

---

## Building from Source

Requires Java 8+, Scala 2.12, and [sbt](https://www.scala-sbt.org/) 1.9+.

```bash
git clone https://github.com/your-org/xpark.git
cd xpark
sbt assembly
```

The fat JAR will be written to:

```
target/scala-2.12/xpark-assembly-0.1.0.jar
```

The JAR excludes the Scala standard library and all Spark dependencies (marked `provided`), so it is safe to add to `$SPARK_HOME/jars/` without version conflicts.

---

## How it Works

### Data Source: `AppStatusStore`

All metrics are read from Spark's internal `AppStatusStore`, which is populated in real time by Spark's event bus. This store is available in:

- **Live applications** — via `SparkContext.ui.store`
- **History Server** — via the replayed event log store

xpark never modifies or intercepts Spark's data pipeline; it is purely a read-only consumer.

### Live Application Integration: `SparkListener`

`XParkListener` implements `SparkListener` and is registered via `spark.extraListeners`. When the application starts (`onApplicationStart`), it retrieves the live `SparkUI` instance and attaches a new `XParkTab` to it. From that point forward, each request to an xpark page re-queries the `AppStatusStore` for the latest metrics.

### History Server Integration: `SparkHistoryServerPlugin`

`XParkHistoryPlugin` implements `SparkHistoryServerPlugin` and is discovered automatically via Java's `ServiceLoader` mechanism (SPI). When the History Server replays an event log and reconstructs the `SparkUI` for a completed application, it calls `setupUI(ui)` on each registered plugin. xpark attaches its `XParkTab` at that point.

### UI Architecture

Because Spark's `SparkUITab`, `WebUIPage`, and `UIUtils` are `private[spark]`, the UI classes must reside in the `org.apache.spark.*` package hierarchy. Business logic (issue detection, configuration, formatting utilities) lives in `com.xpark.*` and has no such restriction.

```
com.xpark.XParkListener          ← spark.extraListeners hook
com.xpark.XParkConfig            ← threshold configuration
com.xpark.Utils                  ← byte/duration formatting
com.xpark.analysis.IssueDetector ← queries AppStatusStore, returns issues
com.xpark.analysis.PerformanceIssue ← sealed ADT of issue types

org.apache.spark.ui.xpark.XParkTab      ← SparkUITab, owns sub-pages
org.apache.spark.ui.xpark.OverviewPage  ← /xpark
org.apache.spark.ui.xpark.SkewPage      ← /xpark/skew
org.apache.spark.ui.xpark.GCPage        ← /xpark/gc
org.apache.spark.ui.xpark.SpillPage     ← /xpark/spill
org.apache.spark.ui.xpark.StragglerPage ← /xpark/stragglers
org.apache.spark.ui.xpark.BroadcastPage ← /xpark/broadcast
org.apache.spark.deploy.history.XParkHistoryPlugin ← SPI entry point
```
