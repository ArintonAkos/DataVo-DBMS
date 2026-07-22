#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
editor="/Applications/Unity/Hub/Editor/6000.5.4f1/Unity.app/Contents/MacOS/Unity"
project="$root/demos/unity/DataVo.UnityArenaDemo"
lock_file="$project/Packages/packages-lock.json"
settings_file="$project/ProjectSettings/ProjectSettings.asset"
output_root="$(mktemp -d /private/tmp/datavo-unity-arena-cross-engine-il2cpp.XXXXXX)"
app="$output_root/DataVoUnityArenaCrossEngine.app"
editor_log="$output_root/editor.log"
records_file="$output_root/reports.ndjson"
summary_path="$output_root/summary.json"
player_logs=()

print_diagnostics() {
  echo "Unity Editor log: $editor_log" >&2
  if [[ -f "$editor_log" ]]; then
    rg -n -C 2 -- "mac-il2cpp|IL2CPP Player build failed|InvalidOperationException|Exception|error" "$editor_log" >&2 || true
    tail -n 40 "$editor_log" >&2
  fi

  for player_log in "${player_logs[@]}"; do
    echo "Unity Player log: $player_log" >&2
    if [[ -f "$player_log" ]]; then
      rg -n -C 2 -- "DATAVO_ARENA_CROSS_ENGINE_RESULT|Exception|error" "$player_log" >&2 || true
      tail -n 40 "$player_log" >&2
    fi
  done
}

fail() {
  echo "$1" >&2
  print_diagnostics
  exit 1
}

if [[ ! -x "$editor" ]]; then
  fail "Pinned Unity Editor is missing or not executable: $editor"
fi

if [[ ! -f "$lock_file" ]]; then
  fail "Unity arena package lock is missing: $lock_file"
fi

if [[ ! -f "$settings_file" ]]; then
  fail "Unity arena Player settings file is missing: $settings_file"
fi

lock_before="$(shasum -a 256 "$lock_file" | awk '{print $1}')"
settings_before="$(shasum -a 256 "$settings_file" | awk '{print $1}')"

verify_tracked_files_unchanged() {
  local after

  if [[ ! -f "$lock_file" ]]; then
    echo "Unity arena package lock disappeared: $lock_file" >&2
    return 1
  fi

  after="$(shasum -a 256 "$lock_file" | awk '{print $1}')"
  if [[ "$lock_before" != "$after" ]]; then
    echo "Unity macOS IL2CPP cross-engine arena changed tracked packages-lock.json: $lock_file" >&2
    echo "Package lock SHA-256 before: $lock_before" >&2
    echo "Package lock SHA-256 after:  $after" >&2
    return 1
  fi

  if [[ ! -f "$settings_file" ]]; then
    echo "Unity arena Player settings file disappeared: $settings_file" >&2
    return 1
  fi

  after="$(shasum -a 256 "$settings_file" | awk '{print $1}')"
  if [[ "$settings_before" != "$after" ]]; then
    echo "Unity macOS IL2CPP cross-engine arena changed tracked ProjectSettings.asset: $settings_file" >&2
    echo "ProjectSettings SHA-256 before: $settings_before" >&2
    echo "ProjectSettings SHA-256 after:  $after" >&2
    return 1
  fi
}

on_exit() {
  local exit_code=$?
  trap - EXIT
  if ! verify_tracked_files_unchanged; then
    exit 1
  fi

  exit "$exit_code"
}
trap on_exit EXIT

bash "$root/scripts/check-unity-runtime-bcl.sh"
bash "$root/scripts/prepare-unity-arena-candidate.sh"
bash "$root/scripts/test-unity-arena-project.sh"
bash "$root/scripts/check-unity-arena-project.sh" --require-candidate

set +e
"$editor" \
  -batchmode -quit \
  -projectPath "$project" \
  -executeMethod DataVo.UnityArena.Editor.ArenaPlayerBuild.BuildMacOSIl2Cpp \
  -datavoArenaBuildPath "$app" \
  -logFile "$editor_log"
editor_exit=$?
set -e

if [[ $editor_exit -ne 0 ]]; then
  fail "Unity macOS IL2CPP cross-engine arena Player build failed with exit code $editor_exit."
fi

player_directory="$app/Contents/MacOS"
if [[ ! -d "$player_directory" ]]; then
  fail "Unity macOS Player executable directory is missing: $player_directory"
fi

player_candidates=()
while IFS= read -r candidate; do
  player_candidates+=("$candidate")
done < <(find "$player_directory" -maxdepth 1 -type f -perm -111 -print | LC_ALL=C sort)

if [[ ${#player_candidates[@]} -ne 1 ]]; then
  fail "Unity macOS Player must contain exactly one executable in $player_directory; found ${#player_candidates[@]}."
fi

player="${player_candidates[0]}"
run_player() {
  local engine="$1"
  local profile="$2"
  local attempt="$3"
  local run_id="$4"
  local player_log="$output_root/player-${profile}-${engine}-${attempt}.log"
  local player_pid
  local deadline
  local player_exit
  local result_lines
  local result_count

  player_logs+=("$player_log")
  "$player" \
    -batchmode -nographics \
    -datavoArenaEngine "$engine" \
    -datavoArenaProfile "$profile" \
    -datavoArenaRunId "$run_id" \
    -datavoArenaRepetitions 1 \
    -logFile "$player_log" &
  player_pid=$!

  deadline=$((SECONDS + 120))
  while kill -0 "$player_pid" 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      kill -KILL "$player_pid" 2>/dev/null || true
      wait "$player_pid" 2>/dev/null || true
      fail "Unity macOS IL2CPP cross-engine arena $profile/$engine repetition $attempt Player timed out after 120 seconds."
    fi

    sleep 1
  done

  set +e
  wait "$player_pid"
  player_exit=$?
  set -e

  if [[ $player_exit -ne 0 ]]; then
    fail "Unity macOS IL2CPP cross-engine arena $profile/$engine repetition $attempt Player exited with code $player_exit."
  fi

  if [[ ! -f "$player_log" ]]; then
    fail "Unity macOS IL2CPP cross-engine arena $profile/$engine repetition $attempt Player log is missing: $player_log"
  fi

  result_lines="$(rg -o 'DATAVO_ARENA_CROSS_ENGINE_RESULT=.*' "$player_log" || true)"
  result_count="$(printf '%s\n' "$result_lines" | sed '/^$/d' | wc -l | tr -d ' ')"
  if [[ "$result_count" != "1" ]]; then
    fail "Unity macOS IL2CPP cross-engine arena $profile/$engine repetition $attempt Player must log exactly one DATAVO_ARENA_CROSS_ENGINE_RESULT path; found $result_count."
  fi

  RUN_REPORT_PATH="${result_lines#DATAVO_ARENA_CROSS_ENGINE_RESULT=}"
  if [[ ! -f "$RUN_REPORT_PATH" ]]; then
    fail "Unity macOS IL2CPP cross-engine arena $profile/$engine repetition $attempt report is missing: $RUN_REPORT_PATH"
  fi

  if ! node - "$RUN_REPORT_PATH" "$engine" "$profile" "$run_id" "$attempt" "$records_file" <<'NODE'
const fs = require('node:fs');

const [reportPath, expectedEngine, expectedProfile, expectedRunId, attemptText, recordsPath] = process.argv.slice(2);
const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
const expectedMeasuredBatches = { gameplay: 300, 'ivm-stress': 1200 };
const checksumPattern = /^[a-f0-9]{64}$/;
const attempt = Number(attemptText);

function requireCondition(condition, message) {
  if (!condition) throw new Error(message);
}

requireCondition(report.schemaVersion === 1, `Unexpected arena report schema: ${report.schemaVersion}`);
requireCondition(report.schemaRevision === 'arena-live-views-v1', `Unexpected arena report revision: ${report.schemaRevision}`);
requireCondition(report.status === 'passed', `Arena status: ${report.status}`);
requireCondition(report.platform === 'OSXPlayer', `Arena platform: ${report.platform}`);
requireCondition(report.scriptingBackend === 'IL2CPP', `Arena backend: ${report.scriptingBackend}`);
requireCondition(report.engine === expectedEngine, `Arena engine: ${report.engine}; expected ${expectedEngine}`);
requireCondition(report.profile === expectedProfile, `Arena profile: ${report.profile}; expected ${expectedProfile}`);
requireCondition(
  Number.isInteger(report.measuredBatches) && report.measuredBatches > 0 &&
    report.measuredBatches === expectedMeasuredBatches[expectedProfile],
  `Arena measured batch count: ${report.measuredBatches}; expected ${expectedMeasuredBatches[expectedProfile]}`);
requireCondition(Array.isArray(report.repetitions) && report.repetitions.length === 1, 'Arena result must contain one repetition.');
requireCondition(report.repetitions[0]?.index === 1, `Arena repetition index: ${report.repetitions[0]?.index}`);
requireCondition(
  Number.isFinite(report.repetitions[0]?.totalMilliseconds) && report.repetitions[0].totalMilliseconds >= 0,
  `Arena total milliseconds: ${report.repetitions[0]?.totalMilliseconds}`);
requireCondition(typeof report.error === 'string' && report.error.length === 0, `Arena error: ${report.error}`);

for (const key of ['leaderboardChecksum', 'aggregateChecksum', 'zoneInventoryChecksum']) {
  requireCondition(checksumPattern.test(report[key]), `Arena ${key} must be a lowercase 64-character checksum.`);
}

if (expectedEngine === 'sqlite') {
  requireCondition(typeof report.sqliteVersion === 'string' && report.sqliteVersion.trim().length > 0, 'SQLite report must include sqliteVersion.');
} else {
  requireCondition(report.sqliteVersion === '', `Non-SQLite report has sqliteVersion: ${report.sqliteVersion}`);
}

requireCondition(Number.isInteger(attempt) && attempt >= 1 && attempt <= 7, `Invalid repetition attempt: ${attemptText}`);
fs.appendFileSync(recordsPath, JSON.stringify({
  reportPath,
  runId: expectedRunId,
  engine: expectedEngine,
  profile: expectedProfile,
  attempt,
  totalMilliseconds: report.repetitions[0].totalMilliseconds,
  leaderboardChecksum: report.leaderboardChecksum,
  aggregateChecksum: report.aggregateChecksum,
  zoneInventoryChecksum: report.zoneInventoryChecksum
}) + '\n');
NODE
  then
    fail "Unity macOS IL2CPP cross-engine arena $profile/$engine repetition $attempt result validation failed."
  fi
}

validate_profile_equivalence() {
  local profile="$1"

  if ! node - "$records_file" "$profile" <<'NODE'
const fs = require('node:fs');

const [recordsPath, profile] = process.argv.slice(2);
const records = fs.readFileSync(recordsPath, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse)
  .filter(record => record.profile === profile);
const engines = ['datavo-reactive', 'datavo-polling', 'sqlite'];
const checksumKeys = ['leaderboardChecksum', 'aggregateChecksum', 'zoneInventoryChecksum'];

function requireCondition(condition, message) {
  if (!condition) throw new Error(message);
}

requireCondition(records.length === 21, `Expected 21 reports for ${profile}; received ${records.length}.`);
for (let attempt = 1; attempt <= 7; attempt++) {
  const sameAttempt = engines.map(engine => records.find(record =>
    record.engine === engine && record.attempt === attempt));
  requireCondition(sameAttempt.every(Boolean), `Missing report for ${profile} repetition ${attempt}.`);
  for (const checksumKey of checksumKeys) {
    const expected = sameAttempt[0][checksumKey];
    requireCondition(
      sameAttempt.every(record => record[checksumKey] === expected),
      `${profile} repetition ${attempt} cross-engine ${checksumKey} values differ.`);
  }
}
NODE
  then
    fail "Unity macOS IL2CPP cross-engine arena $profile checksum equivalence validation failed."
  fi
}

run_suffix="$(uuidgen | tr '[:upper:]' '[:lower:]')"
for profile in gameplay ivm-stress; do
  for engine in datavo-reactive datavo-polling sqlite; do
    for attempt in 1 2 3 4 5 6 7; do
      run_id="${profile}_${engine//-/_}_${attempt}_${run_suffix}"
      run_player "$engine" "$profile" "$attempt" "$run_id"
    done
  done

  validate_profile_equivalence "$profile"
done

if ! node - "$records_file" "$summary_path" <<'NODE'
const fs = require('node:fs');

const [recordsPath, summaryPath] = process.argv.slice(2);
const records = fs.readFileSync(recordsPath, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse);
const profiles = ['gameplay', 'ivm-stress'];
const engines = ['datavo-reactive', 'datavo-polling', 'sqlite'];
const checksumKeys = ['leaderboardChecksum', 'aggregateChecksum', 'zoneInventoryChecksum'];

function requireCondition(condition, message) {
  if (!condition) throw new Error(message);
}

requireCondition(records.length === 42, `Expected 42 raw reports; received ${records.length}.`);
const seen = new Set();
for (const record of records) {
  const key = `${record.profile}/${record.engine}/${record.attempt}`;
  requireCondition(!seen.has(key), `Duplicate raw report: ${key}`);
  seen.add(key);
}

const medians = [];
for (const profile of profiles) {
  for (let attempt = 1; attempt <= 7; attempt++) {
    const sameAttempt = engines.map(engine => records.find(record =>
      record.profile === profile && record.engine === engine && record.attempt === attempt));
    requireCondition(sameAttempt.every(Boolean), `Missing report for ${profile} repetition ${attempt}.`);
    for (const checksumKey of checksumKeys) {
      const expected = sameAttempt[0][checksumKey];
      requireCondition(
        sameAttempt.every(record => record[checksumKey] === expected),
        `${profile} repetition ${attempt} cross-engine ${checksumKey} values differ.`);
    }
  }

  for (const engine of engines) {
    const pair = records.filter(record => record.profile === profile && record.engine === engine);
    requireCondition(pair.length === 7, `Expected seven reports for ${profile}/${engine}; received ${pair.length}.`);
    const totals = pair.map(record => record.totalMilliseconds).sort((left, right) => left - right);
    medians.push({
      profile,
      engine,
      repetitions: 7,
      medianTotalMilliseconds: totals[3]
    });
  }
}

fs.writeFileSync(summaryPath, JSON.stringify({
  schemaVersion: 1,
  reports: records,
  medians
}, null, 2) + '\n');
NODE
then
  fail "Unity macOS IL2CPP cross-engine arena checksum equivalence validation failed."
fi

echo "Unity macOS IL2CPP cross-engine arena benchmark passed."
echo "Player app: $app"
echo "Summary: $summary_path"
echo "Raw reports: $records_file"
echo "Unity backend: IL2CPP"
echo "Unity Editor log: $editor_log"
echo "Unity Player logs: $output_root/player-<profile>-<engine>-<repetition>.log"
