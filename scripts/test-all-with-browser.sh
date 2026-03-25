#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT_DIR"

echo "[1/4] Running .NET tests..."
dotnet test "$ROOT_DIR/DataVo.Tests/DataVo.Tests.csproj"

echo "[2/4] Publishing RELEASE browser WASM bundle into docs/public for browser tests..."
dotnet publish "$ROOT_DIR/DataVo.Browser/DataVo.Browser.csproj" -c Release
mkdir -p "$ROOT_DIR/docs/public/datavo-wasm"
rsync -a --delete "$ROOT_DIR/DataVo.Browser/bin/Release/net10.0/browser-wasm/AppBundle/" "$ROOT_DIR/docs/public/datavo-wasm/"
cp "$ROOT_DIR/DataVo.Browser/datavo.interop.js" "$ROOT_DIR/docs/public/datavo-wasm/"
cp "$ROOT_DIR/DataVo.Browser/datavo.storage.worker.js" "$ROOT_DIR/docs/public/datavo-wasm/"

echo "[3/4] Ensuring Chromium is installed for Playwright..."
cd "$ROOT_DIR/docs"
npm run test:browser:install

echo "[4/4] Running browser E2E suite in Chromium..."
npm run test:browser

echo "DONE: .NET + browser test suites passed."
