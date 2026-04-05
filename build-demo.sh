#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# build-demo.sh — Build the sparkx plugin and sample demo JARs
#
# Requires: Java 8+, sbt (https://www.scala-sbt.org/download.html)
#
# Produces:
#   target/scala-2.12/sparkx-assembly-0.1.0.jar              (plugin JAR)
#   sample/target/scala-2.12/sparkx-sample-assembly-0.1.0.jar (demo JAR)
#
# Usage:
#   ./build-demo.sh            build both JARs
#   ./build-demo.sh --clean    clean before building
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PLUGIN_JAR="target/scala-2.12/sparkx-assembly-0.1.0.jar"
SAMPLE_JAR="sample/target/scala-2.12/sparkx-sample-assembly-0.1.0.jar"

# ── 1. Check prerequisites ───────────────────────────────────────────────────
if ! command -v java &>/dev/null; then
  echo "ERROR: Java not found. Install Java 8+ from https://adoptium.net" >&2
  exit 1
fi
echo "✓ Java OK ($(java -version 2>&1 | head -1))"

if ! command -v sbt &>/dev/null; then
  echo "ERROR: sbt not found. Install from https://www.scala-sbt.org/download.html" >&2
  exit 1
fi
echo "✓ sbt found"

# ── 2. Optional clean ────────────────────────────────────────────────────────
if [[ "${1:-}" == "--clean" ]]; then
  echo "  Cleaning previous build artifacts …"
  sbt clean
fi

# ── 3. Build plugin assembly ─────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo "  Building sparkx plugin JAR …"
echo "══════════════════════════════════════════════════════════════════════"
sbt assembly

if [[ ! -f "$PLUGIN_JAR" ]]; then
  echo "ERROR: Plugin JAR was not produced at $PLUGIN_JAR" >&2
  exit 1
fi
echo "✓ Plugin JAR: $PLUGIN_JAR"

# ── 4. Build sample assembly ─────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo "  Building sparkx sample (demo) JAR …"
echo "══════════════════════════════════════════════════════════════════════"
sbt "sample/assembly"

if [[ ! -f "$SAMPLE_JAR" ]]; then
  echo "ERROR: Sample JAR was not produced at $SAMPLE_JAR" >&2
  exit 1
fi
echo "✓ Sample JAR: $SAMPLE_JAR"

# ── 5. Done ──────────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo "  Build complete!"
echo ""
echo "  Plugin JAR : $PLUGIN_JAR"
echo "  Sample JAR : $SAMPLE_JAR"
echo ""
echo "  Next steps:"
echo "    ./run-demo.sh                   run all demo scenarios"
echo "    ./run-demo.sh skew --pause      run one scenario and keep UI open"
echo "    ./run-demo.sh history-server    replay past runs in History Server"
echo "══════════════════════════════════════════════════════════════════════"
