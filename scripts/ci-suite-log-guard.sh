#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT="$ROOT_DIR/DataVo.Tests/DataVo.Tests.csproj"
OUT_DIR="$ROOT_DIR/artifacts/test-logs/ci-suite-guard"

mkdir -p "$OUT_DIR"

SUITES=(ADO AuditFixes BrowserParity BTree E2E EntityFramework Execution Indexing MVCC StorageEngine Transactions)

echo "Running suite-by-suite detailed checks..."

for suite in "${SUITES[@]}"; do
  log="$OUT_DIR/${suite}.log"
  echo "===== SUITE ${suite} =====" > "$log"

  set +e
  dotnet test "$PROJECT" \
    --no-build \
    --filter "FullyQualifiedName~DataVo.Tests.${suite}." \
    --logger "console;verbosity=detailed" \
    -v normal >> "$log" 2>&1
  code=$?
  set -e

  echo "EXIT_CODE=$code" >> "$log"
  echo "SUITE ${suite}: exit_code=${code}"
done

python - "$OUT_DIR" <<'PY'
import glob
import json
import os
import re
import sys

out_dir = sys.argv[1]
logs = sorted(glob.glob(os.path.join(out_dir, "*.log")))

suspicious_patterns = {
    "index_out_of_range": r"Index was out of range",
    "deadlock_detected": r"Deadlock detected",
    "test_run_failed": r"Test Run Failed\\.",
    "unhandled_exception": r"Unhandled exception|UnhandledException",
}

report = {
    "suite_count": len(logs),
    "suites": [],
    "totals": {
        "nonzero_exit": 0,
        **{k: 0 for k in suspicious_patterns}
    }
}

failed = False

for path in logs:
    suite = os.path.basename(path).replace('.log', '')
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        txt = f.read()

    ec_match = re.findall(r"EXIT_CODE=(\d+)", txt)
    ec = int(ec_match[-1]) if ec_match else 999

    total_match = re.findall(r"Total tests:\s*(\d+)", txt)
    passed_match = re.findall(r"Passed:\s*(\d+)", txt)
    failed_match = re.findall(r"Failed:\s*(\d+)", txt)

    entry = {
        "suite": suite,
        "exit_code": ec,
        "total_tests": int(total_match[-1]) if total_match else 0,
        "passed": int(passed_match[-1]) if passed_match else 0,
        "failed": int(failed_match[-1]) if failed_match else 0,
        "suspicious": {}
    }

    if ec != 0:
        report["totals"]["nonzero_exit"] += 1
        failed = True

    for key, pattern in suspicious_patterns.items():
        count = len(re.findall(pattern, txt))
        entry["suspicious"][key] = count
        report["totals"][key] += count
        if count > 0:
            failed = True

    report["suites"].append(entry)

json_path = os.path.join(out_dir, "suite-guard-report.json")
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)

text_path = os.path.join(out_dir, "suite-guard-report.txt")
with open(text_path, "w", encoding="utf-8") as f:
    f.write(f"suite_count={report['suite_count']}\n")
    f.write(f"nonzero_exit={report['totals']['nonzero_exit']}\n")
    for key in suspicious_patterns:
        f.write(f"{key}={report['totals'][key]}\n")
    f.write("\n")
    for s in report["suites"]:
        f.write(
            f"{s['suite']}: exit={s['exit_code']} total={s['total_tests']} passed={s['passed']} failed={s['failed']} "
            f"index_out_of_range={s['suspicious']['index_out_of_range']} "
            f"deadlock_detected={s['suspicious']['deadlock_detected']} "
            f"test_run_failed={s['suspicious']['test_run_failed']} "
            f"unhandled_exception={s['suspicious']['unhandled_exception']}\n"
        )

print("Wrote:", json_path)
print("Wrote:", text_path)

if failed:
    print("CI SUITE LOG GUARD FAILED: suspicious signatures detected.")
    sys.exit(1)

print("CI SUITE LOG GUARD PASSED: no suspicious signatures detected.")
PY

echo "DONE: suite log guard complete."
