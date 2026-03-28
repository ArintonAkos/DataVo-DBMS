#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT_DIR"

echo "[1/5] Publishability hardening lane..."
bash "$ROOT_DIR/scripts/verify-local-packages.sh"

echo "[2/5] Relational hardening lane..."
bash "$ROOT_DIR/scripts/test-relational-hardening.sh"

echo "[3/5] Vector/ANN final-mile lane..."
bash "$ROOT_DIR/scripts/final-mile-validate.sh"

echo "[4/5] Browser strict stress lane..."
bash "$ROOT_DIR/scripts/test-browser-strict-stress.sh"

echo "[5/5] Final report pointers"
echo "- HNSW perf artifacts: $ROOT_DIR/artifacts/perf/hnsw/latest-build-scaling-summary.csv"
echo "- Browser strict artifacts: $ROOT_DIR/artifacts/perf/browser-strict/latest-raw.log"

echo "DONE: Phase closeout lanes completed."
