#!/usr/bin/env bash
set -euo pipefail

# AOT/trim warning ratchet for the Native AOT initiative (Phase 1).
#
# DataVo.Data is LOCKED (IL warnings are errors in its csproj) so any regression there fails its build.
# DataVo.Core still has trim/AOT warnings while the STJ migration + dynamic removal are in progress; this
# script is the durable fence that ensures that count only ever goes DOWN. Lower CORE_BASELINE as each
# phase lands. When it reaches 0, move DataVo.Core to WarningsAsErrors (like DataVo.Data) and retire this.
#
# Analog of the GC program's InsertAllocationGuardTests ceiling: tighten, never loosen.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Ratchet baseline — the maximum allowed IL trim/AOT warnings in DataVo.Core. LOWER THIS as work lands.
CORE_BASELINE=184

build_il_warnings() {
  local proj="$1"
  dotnet build "$proj" -c Release -t:Rebuild 2>&1 | grep -cE "warning IL[0-9]+" || true
}

echo "== AOT ratchet =="

echo "-- DataVo.Data (locked, expect 0) --"
if ! dotnet build "$ROOT_DIR/DataVo.Data/DataVo.Data.csproj" -c Release -t:Rebuild >/dev/null 2>&1; then
  echo "FAIL: DataVo.Data build failed — an AOT/trim regression turned an IL warning into an error."
  exit 1
fi
echo "OK: DataVo.Data builds clean and locked."

echo "-- DataVo.Core (ratchet, baseline ${CORE_BASELINE}) --"
core_count="$(build_il_warnings "$ROOT_DIR/DataVo.Core/DataVo.Core.csproj")"
echo "Current DataVo.Core IL trim/AOT warnings: ${core_count} (baseline ${CORE_BASELINE})"

if [ "$core_count" -gt "$CORE_BASELINE" ]; then
  echo "FAIL: DataVo.Core AOT warnings increased (${core_count} > ${CORE_BASELINE}). Fix the regression or justify it."
  exit 1
fi

if [ "$core_count" -lt "$CORE_BASELINE" ]; then
  echo "PROGRESS: DataVo.Core is below baseline (${core_count} < ${CORE_BASELINE}). Lower CORE_BASELINE to ${core_count} in this script to ratchet."
fi

echo "OK: AOT ratchet holds."
