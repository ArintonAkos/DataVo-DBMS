#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
editor="/Applications/Unity/Hub/Editor/6000.5.4f1/Unity.app/Contents/MacOS/Unity"
project="$root/tests/unity/DataVo.UnitySmoke"
lock_file="$project/Packages/packages-lock.json"

if [[ ! -x "$editor" ]]; then
  echo "Pinned Unity Editor is missing or not executable: $editor" >&2
  exit 1
fi

if [[ ! -f "$lock_file" ]]; then
  echo "Unity smoke package lock is missing: $lock_file" >&2
  exit 1
fi

lock_before="$(shasum -a 256 "$lock_file" | awk '{print $1}')"
output_root="$(mktemp -d /private/tmp/datavo-unity-b2b.XXXXXX)"
results="$output_root/editmode-results.xml"
log="$output_root/editor.log"

bash "$root/scripts/check-unity-runtime-bcl.sh"
bash "$root/scripts/prepare-unity-smoke-candidate.sh"

set +e
"$editor" \
  -runTests -batchmode \
  -projectPath "$project" \
  -testPlatform EditMode \
  -testResults "$results" \
  -logFile "$log"
editor_exit=$?
set -e

if [[ $editor_exit -ne 0 ]]; then
  echo "Unity EditMode test run failed with exit code $editor_exit. Log: $log" >&2
  exit "$editor_exit"
fi

if [[ ! -f "$results" ]]; then
  echo "Unity EditMode test result XML is missing: $results" >&2
  exit 1
fi

node - "$results" <<'NODE'
const fs = require('node:fs');

const resultPath = process.argv[2];
const xml = fs.readFileSync(resultPath, 'utf8');
const testRun = xml.match(/<test-run\b[^>]*>/)?.[0];

if (!testRun) {
  throw new Error('Unity test XML has no test-run root element.');
}

function readAttribute(name) {
  return testRun.match(new RegExp(`\\b${name}="([^"]*)"`))?.[1];
}

if (readAttribute('result') !== 'Passed') {
  throw new Error(`Unity test run result must be Passed; received ${readAttribute('result') ?? 'missing'}.`);
}

if (readAttribute('failed') !== '0') {
  throw new Error(`Unity test run failed count must be 0; received ${readAttribute('failed') ?? 'missing'}.`);
}

const passed = Number(readAttribute('passed'));
if (!Number.isInteger(passed) || passed < 1) {
  throw new Error(`Unity test run passed count must be at least 1; received ${readAttribute('passed') ?? 'missing'}.`);
}
NODE

bash "$root/scripts/check-unity-smoke-project.sh" --require-candidate

lock_after="$(shasum -a 256 "$lock_file" | awk '{print $1}')"
if [[ "$lock_before" != "$lock_after" ]]; then
  echo "Unity EditMode run changed the tracked package lock: $lock_file" >&2
  exit 1
fi

echo "Unity EditMode tests passed. Results: $results"
echo "Unity Editor log: $log"
