#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PACKAGES_DIR="$ROOT_DIR/artifacts/packages"
TEMP_DIR="$(mktemp -d)"
APP_DIR="$TEMP_DIR/DataVo.PackageSmoke"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

echo "[1/5] Packing DataVo.Core and DataVo.Data..."
dotnet pack "$ROOT_DIR/DataVo.Core/DataVo.Core.csproj" -c Release >/dev/null
dotnet pack "$ROOT_DIR/DataVo.Data/DataVo.Data.csproj" -c Release >/dev/null

echo "[2/5] Verifying package artifacts exist..."
ls "$PACKAGES_DIR"/DataVo.Core.*.nupkg >/dev/null
ls "$PACKAGES_DIR"/DataVo.Data.*.nupkg >/dev/null

echo "[3/5] Creating temporary consumer app..."
dotnet new console -n DataVo.PackageSmoke -o "$APP_DIR" --force >/dev/null

echo "[4/5] Installing local packages from artifacts..."
dotnet add "$APP_DIR/DataVo.PackageSmoke.csproj" package DataVo.Core --source "$PACKAGES_DIR" --prerelease >/dev/null
dotnet add "$APP_DIR/DataVo.PackageSmoke.csproj" package DataVo.Data --source "$PACKAGES_DIR" --prerelease >/dev/null

echo "[5/5] Building consumer app with local packages..."
dotnet build "$APP_DIR/DataVo.PackageSmoke.csproj" -c Release >/dev/null

echo "SUCCESS: local DataVo package smoke test passed."
