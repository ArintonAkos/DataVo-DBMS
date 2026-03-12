#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BROWSER_DIR="$ROOT_DIR/DataVo.Browser"
DOCS_WASM_DIR="$ROOT_DIR/docs/public/datavo-wasm"
APP_BUNDLE_DIR="$BROWSER_DIR/bin/Release/net10.0/browser-wasm/AppBundle"
PUBLISH_DIR="$BROWSER_DIR/bin/Release/net10.0/browser-wasm/publish"

cd "$ROOT_DIR"

echo "Publishing DataVo.Browser (Release)..."
dotnet publish "$BROWSER_DIR/DataVo.Browser.csproj" -c Release

echo "Refreshing docs/public/datavo-wasm..."
rm -rf "$DOCS_WASM_DIR"
mkdir -p "$DOCS_WASM_DIR"
cp -R "$APP_BUNDLE_DIR"/* "$DOCS_WASM_DIR/"
cp "$BROWSER_DIR/datavo.interop.js" "$DOCS_WASM_DIR/"

if [[ -f "$PUBLISH_DIR/DataVo.Browser.deps.json" ]]; then
  cp "$PUBLISH_DIR/DataVo.Browser.deps.json" "$DOCS_WASM_DIR/"
fi

mkdir -p "$DOCS_WASM_DIR/_framework"
if [[ -f "$PUBLISH_DIR/dotnet.diagnostics.js" ]]; then
  cp "$PUBLISH_DIR/dotnet.diagnostics.js" "$DOCS_WASM_DIR/_framework/"
fi

echo "DEPLOY DONE"
