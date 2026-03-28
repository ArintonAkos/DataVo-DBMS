#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT_DIR"

RUN_LONG=0
if [[ "${1:-}" == "--long" ]]; then
  RUN_LONG=1
fi

echo "[1/2] Fast HNSW correctness lane..."
bash "$ROOT_DIR/scripts/test-hnsw-fast.sh"

echo "[2/2] HNSW performance lane..."
if [[ $RUN_LONG -eq 1 ]]; then
  bash "$ROOT_DIR/scripts/test-hnsw-perf.sh" --long
else
  bash "$ROOT_DIR/scripts/test-hnsw-perf.sh"
fi

echo "DONE: Final-mile validation completed."
