#!/usr/bin/env bash
set -euo pipefail

# Native AOT durable fence (Phase 1 — COMPLETE).
#
# DataVo.Data, DataVo.Core and DataVo.VectorData are LOCKED: each csproj turns the IL trim/AOT diagnostics listed
# once in Directory.Build.props ($(DataVoAotWarningsAsErrors)) into errors, so any Native-AOT regression fails the
# build. This script is the CI gate that proves all three library projects still build clean. (Companion gate: publish DataVo.AotSmoke with PublishAot and run
# the native binary — it must print "ALL SMOKE CHECKS PASSED".)
#
# History of the DataVo.Core ratchet that got us here: 184 (fence) -> 144 (T1 catalog XmlSerializer)
# -> 100 (T2 Newtonsoft->STJ) -> 76 (T3 Volcano/Select->STJ) -> 0 (T4 dynamic/DLR) + T5 Activator factory.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "== AOT fence (locked library projects) =="

for project in DataVo.Data DataVo.Core DataVo.VectorData; do
  name="$(basename "$project")"
  echo "-- $name (locked: IL trim/AOT warnings are errors) --"
  log="$(mktemp)"
  if ! dotnet build "$ROOT_DIR/$project/$name.csproj" -c Release -t:Rebuild >"$log" 2>&1; then
    # Show the errors (IL codes included) rather than hiding which diagnostic broke the fence.
    grep -E ': error ' "$log" | sort -u || tail -n 60 "$log"
    rm -f "$log"
    echo "FAIL: $name build failed — a Native-AOT/trim regression turned an IL diagnostic into an error (see above)."
    exit 1
  fi
  rm -f "$log"
  echo "OK: $name builds clean and AOT-locked."
done

echo "OK: AOT fence holds — DataVo.Core, DataVo.Data and DataVo.VectorData are Native-AOT clean and locked."
