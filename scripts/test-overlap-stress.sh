#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ITERATIONS="${1:-100}"
OUT_DIR="$ROOT_DIR/artifacts/test-logs/overlap-stress"

if ! [[ "$ITERATIONS" =~ ^[0-9]+$ ]] || [[ "$ITERATIONS" -le 0 ]]; then
  echo "ERROR: iterations must be a positive integer."
  exit 2
fi

mkdir -p "$OUT_DIR"

FILTER="FullyQualifiedName~SeededFuzzLite_WithUpdateDeleteOverlap_MultiSeed_PreservesOverlapInvariants"
PROJECT="$ROOT_DIR/DataVo.Tests/DataVo.Tests.csproj"

echo "Running overlap stress: iterations=$ITERATIONS"
echo "Output directory: $OUT_DIR"

for i in $(seq 1 "$ITERATIONS"); do
  LOG="$OUT_DIR/iter-${i}.log"
  echo "===== ITERATION $i =====" > "$LOG"

  set +e
  dotnet test "$PROJECT" \
  --no-build \
  --filter "$FILTER" \
  --logger "console;verbosity=detailed" \
  -v normal >> "$LOG" 2>&1
  CODE=$?
  set -e

  echo "EXIT_CODE=$CODE" >> "$LOG"

  if [[ "$CODE" -ne 0 ]]; then
  echo "FAILED_AT_ITERATION=$i" | tee "$OUT_DIR/summary.txt"
  break
  fi
done

python - "$OUT_DIR" "$ITERATIONS" <<'PY'
import glob
import json
import os
import re
import sys

out_dir = sys.argv[1]
iterations = int(sys.argv[2])
logs = sorted(glob.glob(os.path.join(out_dir, "iter-*.log")))

patterns = {
  "index_out_of_range": r"Index was out of range",
  "deadlock_detected": r"Deadlock detected",
  "test_run_failed": r"Test Run Failed\.",
  "assert_failure": r"Assert\.",
  "stack_trace": r"Stack Trace:",
  "unhandled_exception": r"Unhandled exception|UnhandledException",
  "error_lines": r"\bError:",
}

summary = {
  "iterations_requested": iterations,
  "iterations_found": len(logs),
  "iterations_with_nonzero_exit": 0,
  "pattern_counts": {k: 0 for k in patterns},
  "nonzero_exit_iterations": [],
}

for path in logs:
  name = os.path.basename(path)
  m = re.search(r"iter-(\d+)\.log$", name)
  idx = int(m.group(1)) if m else None

  with open(path, "r", encoding="utf-8", errors="ignore") as f:
    txt = f.read()

  ec_match = re.findall(r"EXIT_CODE=(\d+)", txt)
  ec = int(ec_match[-1]) if ec_match else 999
  if ec != 0:
    summary["iterations_with_nonzero_exit"] += 1
    summary["nonzero_exit_iterations"].append(idx if idx is not None else name)

  for key, pat in patterns.items():
    summary["pattern_counts"][key] += len(re.findall(pat, txt))

summary_path = os.path.join(out_dir, "anomaly-summary.json")
with open(summary_path, "w", encoding="utf-8") as f:
  json.dump(summary, f, indent=2)

text_path = os.path.join(out_dir, "anomaly-summary.txt")
with open(text_path, "w", encoding="utf-8") as f:
  f.write(f"iterations_requested={summary['iterations_requested']}\n")
  f.write(f"iterations_found={summary['iterations_found']}\n")
  f.write(f"iterations_with_nonzero_exit={summary['iterations_with_nonzero_exit']}\n")
  if summary["nonzero_exit_iterations"]:
    f.write("nonzero_exit_iterations=" + ",".join(map(str, summary["nonzero_exit_iterations"])) + "\n")
  for key, value in summary["pattern_counts"].items():
    f.write(f"{key}={value}\n")

print("Wrote:", summary_path)
print("Wrote:", text_path)

if summary["iterations_with_nonzero_exit"] > 0:
  sys.exit(1)
PY

echo "DONE: overlap stress completed."
