#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$ROOT_DIR"
export LC_ALL=C

fail() {
  printf 'Stage 2B platform-seam verification failed: %s\n' "$1" >&2
  exit 1
}

DIRECTIVE_PATTERN='^[[:space:]]*#[[:space:]]*(if|elif|else|endif)\b'

# These orchestration/algorithm files were changed by Stage 2B and must remain target-neutral.
# HNSW is intentionally absent: its existing branches are explicitly deferred to Stage 2C.
COMMON_FILES=(
  DataVo.Core/Transactions/WalAppender.cs
  DataVo.Core/BTree/IndexKeyEncoder.cs
  DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs
  DataVo.Core/Runtime/Reactive/CellValue.cs
  DataVo.Core/Transactions/GroupCommitter.cs
  DataVo.Core/Parser/DQL/Select.cs
  DataVo.Core/Execution/Volcano/SortOperator.cs
  DataVo.Core/StorageEngine/Lsm/LsmCompactor.cs
  DataVo.Core/Indexing/IndexManager.cs
  DataVo.Core/StorageEngine/Lsm/LsmLatestRowStore.cs
  DataVo.Core/Runtime/Security/PasswordHasher.cs
  DataVo.Core/Runtime/Security/SessionSecurityStore.cs
  DataVo.Core/Runtime/Reactive/AggregateReactiveQuery.cs
  DataVo.Core/CompiledQueries/CompiledIntegerScan.cs
  DataVo.Core/Parser/DML/InsertRowService.cs
)

for file in "${COMMON_FILES[@]}"; do
  [[ -f "$file" ]] || fail "missing expected common file: $file"
  if matches="$(rg -n "$DIRECTIVE_PATTERN" "$file")"; then
    printf '%s\n' "$matches" >&2
    fail "target directive found in common file: $file"
  fi
done

validate_complete_target_file() {
  local file="$1"
  local expected_opening="$2"
  local first_nonblank last_nonblank directive_count

  first_nonblank="$(awk 'NF { print; exit }' "$file")"
  last_nonblank="$(awk 'NF { line = $0 } END { print line }' "$file")"
  directive_count="$(rg -c "$DIRECTIVE_PATTERN" "$file")"

  [[ "$first_nonblank" == "$expected_opening" ]] ||
    fail "$file is not wholly guarded by $expected_opening"
  [[ "$last_nonblank" == '#endif' ]] ||
    fail "$file does not end with its target guard"
  [[ "$directive_count" == '2' ]] ||
    fail "$file contains nested or interleaved target directives"
  if rg -n '^[[:space:]]*#[[:space:]]*(elif|else)\b' "$file" >/dev/null; then
    fail "$file contains an interleaved target branch"
  fi
}

directive_files="$(rg -l "$DIRECTIVE_PATTERN" DataVo.Core --glob '*.cs' | LC_ALL=C sort || true)"
while IFS= read -r file; do
  [[ -n "$file" ]] || continue
  case "$file" in
    *.Modern.cs)
      validate_complete_target_file "$file" '#if NET10_0_OR_GREATER'
      ;;
    *.Portable.cs)
      validate_complete_target_file "$file" '#if NETSTANDARD2_1'
      ;;
    DataVo.Core/Compat/NetStandard21Polyfills.cs)
      validate_complete_target_file "$file" '#if NETSTANDARD2_1'
      ;;
    DataVo.Core/Indexing/HNSW/HNSWIndex.cs|\
    DataVo.Core/Indexing/Flat/FlatVectorIndex.cs|\
    DataVo.Core/Utils/SimdDistanceKernels.cs)
      # Existing HNSW, flat-vector, distance, and norm routing is a Stage 2C concern.
      ;;
    *)
      fail "target directive found outside an allowed complete target/deferred file: $file"
      ;;
  esac
done <<< "$directive_files"

bcl_shims="$(rg -n \
  '^[[:space:]]*namespace[[:space:]]+System\.(Collections|Threading\.Tasks)([.;[:space:]]|$)' \
  DataVo.Core --glob '*.cs' || true)"
if [[ -n "$bcl_shims" ]]; then
  printf '%s\n' "$bcl_shims" >&2
  fail 'DataVo collection/task compatibility code is declared under a BCL namespace'
fi

TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/datavo-stage2b-layout.XXXXXX")"
SOURCE_ROOT="$TEMP_ROOT/source"
PACKAGE_DIR="$TEMP_ROOT/packages"
EXTENSIONS_DIR="$TEMP_ROOT/project-extensions/"
DOTNET_CLI_HOME="$TEMP_ROOT/dotnet-home"
GLOBAL_PACKAGES="${NUGET_PACKAGES:-$HOME/.nuget/packages}"

cleanup() { rm -rf "$TEMP_ROOT"; }
trap cleanup EXIT HUP INT TERM

mkdir -p "$SOURCE_ROOT" "$PACKAGE_DIR" "$EXTENSIONS_DIR" "$DOTNET_CLI_HOME"
cp Directory.Build.props README.md LICENSE "$SOURCE_ROOT/"
cp -R DataVo.Core "$SOURCE_ROOT/"
find "$SOURCE_ROOT/DataVo.Core" -type d \( -name bin -o -name obj \) -prune -exec rm -rf '{}' +

export DOTNET_CLI_HOME
export DOTNET_NOLOGO=1
export DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
export NUGET_PACKAGES="$GLOBAL_PACKAGES"
unset NUGET_FALLBACK_PACKAGES || true

dotnet restore "$SOURCE_ROOT/DataVo.Core/DataVo.Core.csproj" \
  --nologo --ignore-failed-sources \
  -p:NuGetAudit=false \
  -p:DataVoEnablePortableTarget=false \
  -p:MSBuildProjectExtensionsPath="$EXTENSIONS_DIR"

PACKAGE_VERSION='0.0.0-stage2b-layout'
dotnet pack "$SOURCE_ROOT/DataVo.Core/DataVo.Core.csproj" \
  -c Release --no-restore --nologo \
  -p:DataVoEnablePortableTarget=false \
  -p:NuGetAudit=false \
  -p:MSBuildProjectExtensionsPath="$EXTENSIONS_DIR" \
  -p:EnablePackageValidation=false \
  -p:IncludeSymbols=false \
  -p:PackageVersion="$PACKAGE_VERSION" \
  -p:PackageOutputPath="$PACKAGE_DIR"

CORE_PACKAGE="$PACKAGE_DIR/DataVo.Core.$PACKAGE_VERSION.nupkg"
[[ -f "$CORE_PACKAGE" ]] || fail "normal Core package was not created: $CORE_PACKAGE"

package_entries="$(unzip -Z1 "$CORE_PACKAGE")"

portable_entries="$(printf '%s\n' "$package_entries" \
  | awk '$0 ~ /(^|\/)netstandard2\.1(\/|$)/ { print }' \
  | LC_ALL=C sort)"
[[ -z "$portable_entries" ]] || {
  printf 'Normal Core package portable assets:\n%s\n' "$portable_entries" >&2
  fail 'normal Core package contains a quarantined netstandard2.1 asset'
}

non_net10_framework_entries="$(printf '%s\n' "$package_entries" \
  | awk -F/ '
      ($1 == "lib" || $1 == "ref") && $2 != "net10.0" { print; next }
      $1 == "runtimes" && ($3 == "lib" || $3 == "ref") && $4 != "net10.0" { print }
    ' \
  | LC_ALL=C sort)"
[[ -z "$non_net10_framework_entries" ]] || {
  printf 'Normal Core package non-net10 framework assets:\n%s\n' "$non_net10_framework_entries" >&2
  fail 'normal Core package contains an asset for a framework other than net10.0'
}

core_dlls="$(printf '%s\n' "$package_entries" \
  | awk '$0 ~ /(^|\/)DataVo\.Core\.dll$/ { print }' \
  | LC_ALL=C sort)"
[[ "$core_dlls" == 'lib/net10.0/DataVo.Core.dll' ]] || {
  printf 'Normal Core package DLL assets:\n%s\n' "${core_dlls:-<none>}" >&2
  fail 'normal Core package contains a missing, extra, or portable DataVo.Core asset'
}

printf '%s\n' \
  'Stage 2B platform seams verified: common files are target-neutral, BCL namespaces are clean, and the normal Core package is net10.0-only.'
