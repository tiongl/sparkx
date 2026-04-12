# Copilot Instructions — sparkx

## Build Commands

```bash
sbt compile              # compile the plugin
sbt assembly             # build fat JAR → target/scala-2.12/sparkx-assembly-0.1.0.jar
sbt "sample/assembly"    # build demo app JAR → sample/target/scala-2.12/sparkx-sample-assembly-0.1.0.jar
```

There are no tests in this project yet.

## Architecture

sparkx is a read-only Spark UI plugin that detects performance issues by querying Spark's `AppStatusStore` and rendering results as additional tabs in the Spark Web UI.

### Two-package split (critical constraint)

Spark's `SparkUITab`, `WebUIPage`, and `UIUtils` are `private[spark]`. This forces the codebase into two package hierarchies:

- **`com.sparkx.*`** — Business logic with no Spark visibility restrictions: `SparkXConfig`, `SparkXListener`, `Utils`, and the `analysis.PerformanceIssue` sealed trait hierarchy.
- **`org.apache.spark.ui.sparkx.*`** — UI layer that must live inside `org.apache.spark` to access private APIs: `SparkXTab`, all `*Page` classes, `IssueDetector`, `SparkXPageUtils`, and `SparkXPluginBridge`.

When adding new code, place it in `com.sparkx` unless it directly uses `private[spark]` APIs.

### Entry points

- **Live apps:** `SparkXListener` (registered via `spark.extraListeners`) calls `SparkXPluginBridge.registerForSparkContext()` on app start.
- **History Server:** `SparkXHistoryPlugin` is discovered via Java SPI (`META-INF/services/org.apache.spark.status.AppHistoryServerPlugin`) and calls `SparkXPluginBridge.setupHistoryUI()`.

### Data flow

`IssueDetector.detect()` queries `AppStatusStore` → returns `Seq[PerformanceIssue]` → each `*Page` renders issues as HTML tables using Spark's `UIUtils`. Results are cached with a 30-second TTL. Individual detection methods (e.g., `skewStages()`, `failedTaskStages()`) are called directly by detail pages.

### Adding a new detection

1. Add a case class extending `PerformanceIssue` in `com.sparkx.analysis.PerformanceIssue.scala` — include `severity`, `detailPath`, `estimatedSavingsMs`, and `savingsType`.
2. Add config thresholds to `SparkXConfig` with `spark.sparkx.*` keys.
3. Add the detection logic in `IssueDetector` (in the `org.apache.spark.ui.sparkx` package).
4. Create or update a `*Page` to render the new issue type, and register it in `SparkXTab`.
5. If the new issue belongs to a known root-cause cluster, update `RootCauseAnalyzer.clusters` in `com.sparkx.analysis`.

## Key Conventions

- **Severity model:** `Critical` / `Warning` / `Info` — severity is determined by thresholds in each `PerformanceIssue` subclass, not by the detector.
- **Configuration:** All thresholds use `SparkConf` keys prefixed with `spark.sparkx.*`, parsed in `SparkXConfig.fromConf()`. Add new thresholds there.
- **Estimated savings:** Every `PerformanceIssue` reports `estimatedSavingsMs: Option[Long]` and `savingsType: String` (`"Wall-clock"`, `"Compute"`, `"I/O"`, `"Scheduling overhead"`, or `""`). The overview page aggregates these into a savings banner.
- **Root cause analysis:** `RootCauseAnalyzer` in `com.sparkx.analysis` groups co-occurring issues by stage into root-cause clusters (e.g., "Uneven Data Distribution", "Memory Pressure"). Uses conservative savings aggregation (`max` within a cluster to avoid double-counting). Cluster definitions are in `RootCauseAnalyzer.clusters`.
- **UI pages** use Scala XML literals to produce HTML and call `UIUtils.headerSparkPage()` to wrap content in Spark's standard chrome. Tables use the `sortable` CSS class for client-side sorting.
- **URL helpers:** Use `SparkXPageUtils` for generating links to stages, jobs, SQL DAGs, and sparkx sub-pages — it handles both live and History Server URL bases.
- **Formatting:** Use `com.sparkx.Utils.formatBytes()` and `formatDuration()` for human-readable display of sizes and durations.

## Tech Stack

- Scala 2.12, Spark 3.5.0 (`provided` scope), sbt 1.9.8, sbt-assembly for fat JAR packaging.
