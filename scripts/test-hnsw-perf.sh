#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT_DIR"

# Enable benchmark tests that are intentionally skipped in fast local runs.
export DATAVO_RUN_HNSW_PERF_TESTS=1

RUN_LONG=0
if [[ "${1:-}" == "--long" ]]; then
  RUN_LONG=1
  export DATAVO_RUN_LONG_PERF=1
else
  unset DATAVO_RUN_LONG_PERF
fi

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
ARTIFACT_ROOT="$ROOT_DIR/artifacts/perf/hnsw/$TIMESTAMP"
mkdir -p "$ARTIFACT_ROOT"

RAW_LOG="$ARTIFACT_ROOT/raw.log"
BUILD_SCALING_CSV="$ARTIFACT_ROOT/build-scaling-summary.csv"
INSERT_CHECKPOINT_CSV="$ARTIFACT_ROOT/insert-checkpoints.csv"
PROFILE_LINES="$ARTIFACT_ROOT/profile-lines.log"

echo "Running HNSW performance lane (benchmarks enabled)..."
dotnet test "$ROOT_DIR/DataVo.Tests/DataVo.Tests.csproj" -c Release --filter "FullyQualifiedName~HNSWIndexTests" --logger "console;verbosity=detailed" | tee "$RAW_LOG"
TEST_EXIT=${PIPESTATUS[0]}

grep -E "\[CHECKPOINT\]\[BuildScaling\]|\[PROFILE\]\[(BuildScaling|EfScaling|Connectivity)\]" "$RAW_LOG" > "$PROFILE_LINES" || true

echo "vectors,genMs,insertMs,totalMs,count,memDeltaMb" > "$BUILD_SCALING_CSV"
grep "\[PROFILE\]\[BuildScaling\]\[Summary\]" "$RAW_LOG" | \
  sed -E 's/.*vectors=([0-9]+), genMs=([0-9.]+), insertMs=([0-9.]+), totalMs=([0-9.]+), count=([0-9]+), memDeltaMb=([-0-9.]+).*/\1,\2,\3,\4,\5,\6/' \
  >> "$BUILD_SCALING_CSV" || true

echo "vectors,inserted,totalMs,ips,dop" > "$INSERT_CHECKPOINT_CSV"
grep "\[CHECKPOINT\]\[BuildScaling\]\[Insert\]" "$RAW_LOG" | \
  sed -E 's/.*vectors=([0-9]+), inserted=([0-9]+), totalMs=([0-9.]+), ips=([0-9.]+), dop=([0-9]+).*/\1,\2,\3,\4,\5/' \
  >> "$INSERT_CHECKPOINT_CSV" || true

cp -f "$RAW_LOG" "$ROOT_DIR/artifacts/perf/hnsw/latest-raw.log"
cp -f "$BUILD_SCALING_CSV" "$ROOT_DIR/artifacts/perf/hnsw/latest-build-scaling-summary.csv"
cp -f "$INSERT_CHECKPOINT_CSV" "$ROOT_DIR/artifacts/perf/hnsw/latest-insert-checkpoints.csv"

echo "Artifacts written to: $ARTIFACT_ROOT"
echo "Latest summary: $ROOT_DIR/artifacts/perf/hnsw/latest-build-scaling-summary.csv"

if [[ $TEST_EXIT -ne 0 ]]; then
  exit $TEST_EXIT
fi

echo "DONE: HNSW performance lane passed."
