#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
ARTIFACT_ROOT="$ROOT_DIR/artifacts/perf/browser-strict/$TIMESTAMP"

mkdir -p "$ARTIFACT_ROOT"

echo "Running browser strict stress lane..."

echo "[1/4] Deploying browser WASM assets..."
bash "$ROOT_DIR/scripts/deploy-browser-wasm.sh"

echo "[2/4] Ensuring docs dependencies are installed..."
cd "$ROOT_DIR/docs"
if [[ ! -d node_modules ]]; then
  npm install
fi

echo "[3/4] Ensuring Chromium is installed for Playwright..."
npm run test:browser:install

echo "[4/5] Generating browser scenarios from DataVo.Tests..."
npm run test:browser:generate

echo "[5/5] Running strict browser parity stress tests..."
npm run test:browser:strict:stress | tee "$ARTIFACT_ROOT/raw.log"

cp -f "$ARTIFACT_ROOT/raw.log" "$ROOT_DIR/artifacts/perf/browser-strict/latest-raw.log"

echo "Artifacts written to: $ARTIFACT_ROOT"
echo "DONE: Browser strict stress lane passed."
