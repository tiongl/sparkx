# SparkX Integration Guide

This guide covers how to deploy SparkX across different Spark environments — per-job, per-workspace, or globally.

SparkX requires **two things** to work:
1. The `sparkx-assembly-0.1.0.jar` on the classpath
2. `spark.extraListeners=com.sparkx.SparkXListener` for live apps (History Server uses SPI auto-discovery)

---

## Azure Synapse Analytics

### Per-Job (Synapse Notebook / Pipeline)

Add SparkX to a single notebook or pipeline activity:

```python
%%configure
{
    "conf": {
        "spark.jars": "abfss://<container>@<storage>.dfs.core.windows.net/jars/sparkx-assembly-0.1.0.jar",
        "spark.extraListeners": "com.sparkx.SparkXListener",
        "spark.sparkx.skewMultiplier": "3.0",
        "spark.sparkx.gcRatioThreshold": "0.10"
    }
}
```

Or in a pipeline Spark activity, set these under **Spark configuration → Additional spark properties**.

### Per-Pool (Synapse Spark Pool)

Apply SparkX to all jobs running on a Spark pool:

1. Upload `sparkx-assembly-0.1.0.jar` to your linked ADLS Gen2 storage or workspace packages
2. Go to **Synapse Studio → Manage → Apache Spark pools → [your pool] → Packages**
3. Upload the JAR as a **Workspace package**, then attach it to the pool
4. Go to **Apache Spark configuration** for the pool, add:
   ```
   spark.extraListeners    com.sparkx.SparkXListener
   ```
5. All notebooks and jobs on that pool will now have SparkX enabled

### Per-Workspace (Synapse)

1. Go to **Manage → Apache Spark configurations → New**
2. Create a named Spark configuration with:
   ```
   spark.jars              abfss://<path>/sparkx-assembly-0.1.0.jar
   spark.extraListeners    com.sparkx.SparkXListener
   ```
3. Attach this configuration to all Spark pools in the workspace
4. Every Spark session in the workspace will load SparkX

### Viewing the UI

- **Live session**: In Synapse Studio, open a running notebook → click **Monitor → Spark application → Spark UI** → click the **SparkX** tab
- **Completed jobs**: Go to **Monitor → Apache Spark applications → [app] → Spark UI** (uses Spark History Server internally)

---

## Microsoft Fabric

### Per-Notebook

Add a cell at the top of your Fabric notebook:

```python
%%configure
{
    "conf": {
        "spark.jars": "abfss://<workspace-onelake-path>/sparkx-assembly-0.1.0.jar",
        "spark.extraListeners": "com.sparkx.SparkXListener"
    }
}
```

Or use the Fabric **Environment** approach (recommended):

### Per-Environment (Recommended)

Fabric uses **Environments** to manage libraries and Spark configuration:

1. In your Fabric workspace, go to **+ New → Environment**
2. Under **Libraries → Custom libraries**, upload `sparkx-assembly-0.1.0.jar`
3. Under **Spark properties**, add:
   ```
   spark.extraListeners    com.sparkx.SparkXListener
   ```
4. Click **Publish** to save the environment
5. Attach the environment to notebooks or Spark Job Definitions:
   - Notebook: **Settings gear → Environment → select your environment**
   - Spark Job Definition: set the environment in the job configuration

### Per-Workspace (Global Default)

1. Go to **Workspace settings → Data Engineering/Science → Spark settings**
2. Under **Environment**, set your SparkX-enabled environment as the **default environment**
3. All notebooks and Spark jobs in the workspace will inherit SparkX automatically

### Viewing the UI

- **Running notebook**: Click the **Spark jobs** progress bar at the bottom of a running cell → **View in Monitoring Hub → Spark UI** → **SparkX** tab
- **Completed jobs**: **Monitoring Hub → [Spark application] → Spark UI** → **SparkX** tab

---

## Databricks

### Per-Job (Notebook)

```python
# In a notebook cell (before starting SparkSession):
spark.conf.set("spark.extraListeners", "com.sparkx.SparkXListener")
```

Or pass it when creating a job via the Jobs UI / API:

```json
{
  "spark_conf": {
    "spark.extraListeners": "com.sparkx.SparkXListener"
  },
  "libraries": [
    { "jar": "dbfs:/libs/sparkx-assembly-0.1.0.jar" }
  ]
}
```

### Per-Cluster

1. Upload `sparkx-assembly-0.1.0.jar` to DBFS or Unity Catalog Volumes:
   ```bash
   databricks fs cp sparkx-assembly-0.1.0.jar dbfs:/libs/sparkx-assembly-0.1.0.jar
   ```
2. Go to **Compute → [cluster] → Configuration → Advanced options → Spark config**, add:
   ```
   spark.extraListeners com.sparkx.SparkXListener
   ```
3. Under **Libraries**, click **Install New → JAR → DBFS** and select the JAR
4. Restart the cluster

### Per-Workspace (Init Script)

For all clusters in a workspace, use a global init script:

```bash
#!/bin/bash
cp /dbfs/libs/sparkx-assembly-0.1.0.jar /databricks/jars/
```

1. Upload this as a global init script via **Admin Settings → Global init scripts**
2. Add `spark.extraListeners=com.sparkx.SparkXListener` to a cluster policy applied to all clusters

### Viewing the UI

- **Running cluster**: Cluster page → **Spark UI** tab → **SparkX** tab
- **Completed jobs**: Cluster page → **Spark UI → Event logs** (if logging enabled)

---

## Standalone / On-Premises Spark

### Per-Job

```bash
spark-submit \
  --jars /path/to/sparkx-assembly-0.1.0.jar \
  --conf spark.extraListeners=com.sparkx.SparkXListener \
  --class com.example.MyApp \
  my-app.jar
```

### Per-Cluster (Global)

1. Copy the JAR:
   ```bash
   cp sparkx-assembly-0.1.0.jar $SPARK_HOME/jars/
   ```

2. Add to `$SPARK_HOME/conf/spark-defaults.conf`:
   ```properties
   spark.extraListeners    com.sparkx.SparkXListener
   ```

3. Restart workers / History Server

### History Server (Post-Hoc Analysis)

The History Server discovers SparkX via SPI — just drop the JAR in `$SPARK_HOME/jars/` and restart:

```bash
cp sparkx-assembly-0.1.0.jar $SPARK_HOME/jars/
$SPARK_HOME/sbin/stop-history-server.sh
$SPARK_HOME/sbin/start-history-server.sh
```

No configuration needed — the `META-INF/services` file triggers auto-registration.

---

## YARN / Kubernetes

### YARN

```bash
spark-submit --master yarn \
  --jars hdfs:///libs/sparkx-assembly-0.1.0.jar \
  --conf spark.extraListeners=com.sparkx.SparkXListener \
  ...
```

For cluster-wide deployment, place the JAR on every node's `$SPARK_HOME/jars/` directory.

### Kubernetes

```bash
spark-submit --master k8s://https://<k8s-api>:443 \
  --conf spark.jars=https://<artifact-store>/sparkx-assembly-0.1.0.jar \
  --conf spark.extraListeners=com.sparkx.SparkXListener \
  --conf spark.kubernetes.file.upload.path=s3a://<bucket>/spark-uploads \
  ...
```

Or bake the JAR into a custom Spark Docker image:

```dockerfile
FROM apache/spark:3.5.0
COPY sparkx-assembly-0.1.0.jar /opt/spark/jars/
```

---

## Configuration Reference

All thresholds work across all environments:

| Key | Default | Description |
|---|---|---|
| `spark.sparkx.skewMultiplier` | `3.0` | Data skew detection threshold |
| `spark.sparkx.gcRatioThreshold` | `0.10` | GC pressure threshold (10%) |
| `spark.sparkx.stragglerIQRFactor` | `1.5` | Straggler IQR fence multiplier |
| `spark.sparkx.broadcastSizeMB` | `200` | Broadcast size alert threshold |
| `spark.sparkx.lowCpuRatio` | `0.20` | Low CPU utilization threshold |
| `spark.sparkx.lowCpuMinRunTimeMs` | `60000` | Min run time before low CPU check |
| `spark.sparkx.diskShuffleReadMinMB` | `100` | Disk shuffle read threshold |
| `spark.sparkx.highSchedulerDelayMs` | `500` | Scheduler delay threshold |
| `spark.sparkx.schedulerDelayRatio` | `0.50` | Scheduler delay vs task time ratio |
| `spark.sparkx.resultSerializationMs` | `500` | Slow result serialization threshold |
| `spark.sparkx.executorMemoryCov` | `0.30` | Executor memory skew CoV threshold |
| `spark.sparkx.executorMemoryMinCount` | `3` | Min executors for memory skew check |

> **Optional: closed-loop auto-fix.** The read-only diagnostics above need only the
> `spark.extraListeners` listener. To additionally have SparkX *learn and inject* performance hints,
> register the separate extension (`spark.sql.extensions=org.apache.spark.sql.sparkx.SparkXAutoFixExtension`)
> and set `spark.sparkx.autofix.enabled=true`. It works on every platform in the table below. See
> **[autofix.md](autofix.md)** for setup, modes, and its configuration keys.

---

## Compatibility

| Platform | Spark Versions | Live UI | History Server |
|---|---|---|---|
| Standalone | 3.x | ✅ | ✅ |
| YARN | 3.x | ✅ | ✅ |
| Kubernetes | 3.x | ✅ | ✅ |
| Azure Synapse | 3.3, 3.4 | ✅ | ✅ |
| Microsoft Fabric | 3.4, 3.5 | ✅ | ✅ |
| Databricks | 13.x+ (Spark 3.4+) | ✅ | ✅ |

> **Note:** SparkX is compiled against Spark 3.5.0. It should work with any Spark 3.x release that exposes the `AppStatusStore` API. Older Spark 2.x is not supported.
