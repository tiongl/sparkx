#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run-demo.sh — Launch the sparkx showcase in Spark local mode
#
# Spark is downloaded and set up automatically if SPARK_HOME is not set.
#
# Usage:
#   ./run-demo.sh                          run all 5 scenarios
#   ./run-demo.sh skew                     run only the data-skew scenario
#   ./run-demo.sh skew --pause             run scenario then hold the Spark UI open
#   ./run-demo.sh history-server           start History Server to replay past runs
#
# Supported scenario names: skew | straggler | gc | spill | broadcast | suggestions | all
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# The sample assembly already bundles all sparkx classes — no separate --jars needed.
SAMPLE_JAR="$SCRIPT_DIR/sample/target/scala-2.12/sparkx-sample-assembly-0.1.0.jar"

SPARK_VERSION="3.5.0"
SPARK_PKG="spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_CACHE_DIR="$SCRIPT_DIR/.spark-dist"
SPARK_DOWNLOAD_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PKG}.tgz"

# ── 1. Locate or download Spark ───────────────────────────────────────────────
ensure_spark() {
  if [[ -n "${SPARK_HOME:-}" && -x "${SPARK_HOME}/bin/spark-submit" ]]; then
    echo "✓ Using existing Spark: $SPARK_HOME"
    return
  fi

  local cached="$SPARK_CACHE_DIR/$SPARK_PKG"
  if [[ -x "$cached/bin/spark-submit" ]]; then
    echo "✓ Using cached Spark: $cached"
    SPARK_HOME="$cached"
    return
  fi

  echo "─────────────────────────────────────────────────────────────────────"
  echo "  Spark ${SPARK_VERSION} not found. Downloading now (~350 MB) …"
  echo "  Source: $SPARK_DOWNLOAD_URL"
  echo "─────────────────────────────────────────────────────────────────────"

  mkdir -p "$SPARK_CACHE_DIR"
  local tgz="$SPARK_CACHE_DIR/${SPARK_PKG}.tgz"

  if command -v curl &>/dev/null; then
    curl -L --progress-bar -o "$tgz" "$SPARK_DOWNLOAD_URL"
  elif command -v wget &>/dev/null; then
    wget -q --show-progress -O "$tgz" "$SPARK_DOWNLOAD_URL"
  else
    echo "ERROR: Neither curl nor wget found. Please install one and retry." >&2
    exit 1
  fi

  echo "  Extracting to $SPARK_CACHE_DIR …"
  tar -xzf "$tgz" -C "$SPARK_CACHE_DIR"
  rm -f "$tgz"
  SPARK_HOME="$cached"
  echo "✓ Spark ${SPARK_VERSION} ready at: $SPARK_HOME"
}

# ── 2. Verify Java ────────────────────────────────────────────────────────────
check_java() {
  if ! command -v java &>/dev/null; then
    echo "ERROR: Java not found. Spark requires Java 8 or later." >&2
    echo "  Install from: https://adoptium.net" >&2
    exit 1
  fi
  local ver
  ver=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
  if [[ "$ver" -lt 8 ]] 2>/dev/null; then
    echo "ERROR: Java 8+ required (found Java $ver)." >&2
    exit 1
  fi
  echo "✓ Java OK ($(java -version 2>&1 | head -1))"
}

# ── 3. Verify sparkx sample JAR ───────────────────────────────────────────────
check_jars() {
  if [[ ! -f "$SAMPLE_JAR" ]]; then
    echo "ERROR: sparkx sample JAR not found." >&2
    echo "  Run: sbt assembly && sbt \"sample/assembly\"" >&2
    exit 1
  fi
  echo "✓ sparkx sample JAR found"
}

# ── Main ──────────────────────────────────────────────────────────────────────
check_java
ensure_spark

COMMAND="${1:-all}"
shift || true   # consume the command arg; remaining args are flags/extras

# Only need the sample JAR for demo scenarios (not history-server)
if [[ "$COMMAND" != "history-server" ]]; then
  check_jars
fi

EVENTS_DIR="${SPARK_EVENTS_DIR:-${TMPDIR:-/tmp}/spark-events}"
mkdir -p "$EVENTS_DIR"

# ── Branch on command ─────────────────────────────────────────────────────────
if [[ "$COMMAND" == "history-server" ]]; then
  XPARK_JAR="$SCRIPT_DIR/target/scala-2.12/sparkx-assembly-0.1.0.jar"
  if [[ ! -f "$XPARK_JAR" ]]; then
    echo "ERROR: sparkx plugin JAR not found." >&2
    echo "  Run: sbt assembly" >&2
    exit 1
  fi
  echo "✓ sparkx plugin JAR found"
  echo ""
  echo "  Starting Spark History Server"
  echo "  Log directory : $EVENTS_DIR"
  echo "  UI will be at : http://localhost:18080"
  echo "  sparkx tab     : http://localhost:18080  (visible after selecting an app)"
  echo "  Press Ctrl+C to stop."
  echo ""
  export SPARK_CLASSPATH="$XPARK_JAR"
  export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=$EVENTS_DIR -Dspark.history.ui.port=18080"
  exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.history.HistoryServer
fi

# ── Run demo scenarios ────────────────────────────────────────────────────────
SCENARIO="$COMMAND"
EXTRA_ARGS=("$@")   # remaining args (e.g. --pause)

echo ""
echo "  Launching sparkx demo (scenario: $SCENARIO)"
echo "  Spark UI will be at  : http://localhost:4040"
echo "  sparkx tab will be at : http://localhost:4040/sparkx"
[[ ${#EXTRA_ARGS[@]} -gt 0 ]] && echo "  Flags: ${EXTRA_ARGS[*]}"
echo ""

"$SPARK_HOME/bin/spark-submit" \
  --master "local[*]" \
  --conf "spark.extraListeners=com.sparkx.SparkXListener" \
  --conf "spark.ui.enabled=true" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=$EVENTS_DIR" \
  --conf "spark.executor.memory=1g" \
  --class "com.sparkx.sample.SparkXDemo" \
  "$SAMPLE_JAR" \
  "$SCENARIO" "${EXTRA_ARGS[@]}"

