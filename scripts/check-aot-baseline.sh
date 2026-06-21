#!/usr/bin/env bash
set -euo pipefail

# Native AOT durable fence (Phase 1 — COMPLETE).
#
# DataVo.Data and DataVo.Core are both LOCKED: their csproj turns the IL trim/AOT diagnostics into errors,
# so any Native-AOT regression in the engine core fails the build. This script is the CI gate that proves
# both library projects still build clean. (Companion gate: publish DataVo.AotSmoke with PublishAot and run
# the native binary — it must print "ALL SMOKE CHECKS PASSED".)
#
# History of the DataVo.Core ratchet that got us here: 184 (fence) -> 144 (T1 catalog XmlSerializer)
# -> 100 (T2 Newtonsoft->STJ) -> 76 (T3 Volcano/Select->STJ) -> 0 (T4 dynamic/DLR) + T5 Activator factory.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "== AOT fence (locked library projects) =="

for project in DataVo.Data DataVo.Core; do
  echo "-- $project (locked: IL trim/AOT warnings are errors) --"
  if ! dotnet build "$ROOT_DIR/$project/$project.csproj" -c Release -t:Rebuild >/dev/null 2>&1; then
    echo "FAIL: $project build failed — a Native-AOT/trim regression turned an IL diagnostic into an error."
    exit 1
  fi
  echo "OK: $project builds clean and AOT-locked."
done

echo "OK: AOT fence holds — DataVo.Core + DataVo.Data are Native-AOT clean and locked."
