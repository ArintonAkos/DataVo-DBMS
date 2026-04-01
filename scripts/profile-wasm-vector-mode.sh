#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

MODE="${1:-fallback}" # fallback|hnsw

if [[ "$MODE" != "fallback" && "$MODE" != "hnsw" ]]; then
  echo "usage: $0 [fallback|hnsw]"
  exit 1
fi

echo "[wasm-vector] deploying browser wasm assets"
bash scripts/deploy-browser-wasm.sh

echo "[wasm-vector] running docs browser tests in mode=$MODE"
cd docs
npx playwright test tests/browser/datavo-wasm.spec.ts \
  -g "generated .NET WASM scenarios execute in browser" \
  --workers=1 \
  --project=chromium

echo "[wasm-vector] tip: set query param datavoVectorMode=$MODE in hosted docs URL for manual profiling"
