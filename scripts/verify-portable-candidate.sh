#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd -P)"
TEMP_ROOT="$(mktemp -d)"
RUN_ID="$(basename "$TEMP_ROOT" | tr -cd '[:alnum:]' | tr '[:upper:]' '[:lower:]')"
PACKAGE_VERSION="0.0.0-stage2a.$RUN_ID"

PACKAGES_DIR="$TEMP_ROOT/packages"
EXTRACT_DIR="$TEMP_ROOT/extracted"
PACK_CACHE="$TEMP_ROOT/pack-packages"
CORE_EXTENSIONS="$TEMP_ROOT/core-extensions/"
CONSUMER_CACHE="$TEMP_ROOT/consumer-packages"
CONSUMER_HTTP_CACHE="$TEMP_ROOT/consumer-http-cache"
CONSUMER_EXTENSIONS="$TEMP_ROOT/consumer-extensions/"
SMOKE_CACHE="$TEMP_ROOT/smoke-packages"
SMOKE_HTTP_CACHE="$TEMP_ROOT/smoke-http-cache"
SMOKE_EXTENSIONS="$TEMP_ROOT/smoke-extensions/"
TEST_ROOT="$TEMP_ROOT/test-storage"
NUGET_SCRATCH="$TEMP_ROOT/nuget-scratch"
DOTNET_CLI_HOME="$TEMP_ROOT/dotnet-home"
export NUGET_SCRATCH DOTNET_CLI_HOME
unset NUGET_FALLBACK_PACKAGES || true

cleanup() { rm -rf "$TEMP_ROOT"; }
trap cleanup EXIT HUP INT TERM
mkdir -p "$PACKAGES_DIR" "$EXTRACT_DIR" "$PACK_CACHE" \
  "$CORE_EXTENSIONS" "$CONSUMER_CACHE" "$CONSUMER_HTTP_CACHE" "$CONSUMER_EXTENSIONS" \
  "$SMOKE_CACHE" "$SMOKE_HTTP_CACHE" "$SMOKE_EXTENSIONS" "$TEST_ROOT" "$DOTNET_CLI_HOME"

NUGET_PACKAGES="$PACK_CACHE" dotnet restore "$ROOT_DIR/DataVo.Core/DataVo.Core.csproj" \
  -p:DataVoEnablePortableTarget=true \
  -p:MSBuildProjectExtensionsPath="$CORE_EXTENSIONS" \
  --force --no-cache
NUGET_PACKAGES="$PACK_CACHE" dotnet pack "$ROOT_DIR/DataVo.Core/DataVo.Core.csproj" \
  -c Release --no-restore \
  -p:DataVoEnablePortableTarget=true \
  -p:MSBuildProjectExtensionsPath="$CORE_EXTENSIONS" \
  -p:EnablePackageValidation=true \
  -p:PackageVersion="$PACKAGE_VERSION" \
  -o "$PACKAGES_DIR"

CORE_PACKAGE="$PACKAGES_DIR/DataVo.Core.$PACKAGE_VERSION.nupkg"
[[ -f "$CORE_PACKAGE" ]] || { echo "Missing candidate package: $CORE_PACKAGE" >&2; exit 1; }

entries="$(unzip -Z1 "$CORE_PACKAGE")"
core_dlls="$(printf '%s\n' "$entries" \
  | awk '$0 ~ /(^|\/)DataVo\.Core\.dll$/ { print }' \
  | LC_ALL=C sort)"
expected_dlls=$'lib/net10.0/DataVo.Core.dll\nlib/netstandard2.1/DataVo.Core.dll'
if [[ "$core_dlls" != "$expected_dlls" ]]; then
  echo "Candidate package has the wrong DataVo.Core DLL entries:" >&2
  printf '%s\n' "${core_dlls:-<none>}" >&2
  exit 1
fi

PORTABLE_DLL="$EXTRACT_DIR/DataVo.Core.dll"
unzip -p "$CORE_PACKAGE" 'lib/netstandard2.1/DataVo.Core.dll' > "$PORTABLE_DLL"
[[ -s "$PORTABLE_DLL" ]] || { echo "Extracted portable DLL is empty." >&2; exit 1; }

NUGET_PACKAGES="$CONSUMER_CACHE" NUGET_HTTP_CACHE_PATH="$CONSUMER_HTTP_CACHE" \
dotnet restore "$ROOT_DIR/tools/DataVo.PortableConsumer/DataVo.PortableConsumer.csproj" \
  --source "$PACKAGES_DIR" \
  --source "https://api.nuget.org/v3/index.json" \
  --force --no-cache \
  -p:MSBuildProjectExtensionsPath="$CONSUMER_EXTENSIONS" \
  -p:DataVoCandidateVersion="$PACKAGE_VERSION"

NUGET_PACKAGES="$CONSUMER_CACHE" NUGET_HTTP_CACHE_PATH="$CONSUMER_HTTP_CACHE" \
dotnet build "$ROOT_DIR/tools/DataVo.PortableConsumer/DataVo.PortableConsumer.csproj" \
  -c Release --no-restore \
  -p:MSBuildProjectExtensionsPath="$CONSUMER_EXTENSIONS" \
  -p:DataVoCandidateVersion="$PACKAGE_VERSION"

ASSETS="${CONSUMER_EXTENSIONS}project.assets.json"
grep -Fq 'lib/netstandard2.1/DataVo.Core.dll' "$ASSETS" || {
  echo "Portable consumer did not select the netstandard2.1 candidate asset." >&2
  exit 1
}

CACHED_NUPKG="$CONSUMER_CACHE/datavo.core/$PACKAGE_VERSION/datavo.core.$PACKAGE_VERSION.nupkg"
cmp -s "$CORE_PACKAGE" "$CACHED_NUPKG" || {
  echo "Consumer cache does not contain the exact packed candidate nupkg." >&2
  exit 1
}

NUGET_PACKAGES="$SMOKE_CACHE" NUGET_HTTP_CACHE_PATH="$SMOKE_HTTP_CACHE" \
dotnet restore "$ROOT_DIR/tools/DataVo.PortablePackageSmoke/DataVo.PortablePackageSmoke.csproj" \
  --source "https://api.nuget.org/v3/index.json" \
  --force --no-cache \
  -p:MSBuildProjectExtensionsPath="$SMOKE_EXTENSIONS" \
  -p:DataVoPortableAssembly="$PORTABLE_DLL"

NUGET_PACKAGES="$SMOKE_CACHE" NUGET_HTTP_CACHE_PATH="$SMOKE_HTTP_CACHE" \
dotnet build "$ROOT_DIR/tools/DataVo.PortablePackageSmoke/DataVo.PortablePackageSmoke.csproj" \
  -c Release --no-restore \
  -p:MSBuildProjectExtensionsPath="$SMOKE_EXTENSIONS" \
  -p:DataVoPortableAssembly="$PORTABLE_DLL"

DATAVO_PORTABLE_ASSEMBLY="$PORTABLE_DLL" \
DATAVO_PORTABLE_TEST_ROOT="$TEST_ROOT" \
dotnet "$ROOT_DIR/tools/DataVo.PortablePackageSmoke/bin/Release/net10.0/DataVo.PortablePackageSmoke.dll"
