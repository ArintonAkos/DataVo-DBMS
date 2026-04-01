#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT_DIR"

echo "Generating browser test scenarios from DataVo.Tests..."
node "$ROOT_DIR/scripts/generate-wasm-browser-scenarios.cjs"

echo "DONE: Generated browser scenarios."
