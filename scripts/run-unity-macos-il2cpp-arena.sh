#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
editor="/Applications/Unity/Hub/Editor/6000.5.4f1/Unity.app/Contents/MacOS/Unity"
project="$root/demos/unity/DataVo.UnityArenaDemo"
settings_file="$project/ProjectSettings/ProjectSettings.asset"
output_root="$(mktemp -d /private/tmp/datavo-unity-arena-il2cpp.XXXXXX)"
app="$output_root/DataVoUnityArena.app"
editor_log="$output_root/editor.log"
player_log=""
ticks="${DATAVO_ARENA_TICKS:-300}"

fail() {
  echo "$1" >&2
  echo "Unity Editor log: $editor_log" >&2
  if [[ -n "$player_log" ]]; then
    echo "Unity Player log: $player_log" >&2
  fi
  exit 1
}

print_diagnostics() {
  if [[ -f "$editor_log" ]]; then
    rg -n -C 2 -- "mac-il2cpp|IL2CPP Player build failed|InvalidOperationException" "$editor_log" >&2 || true
  fi

  if [[ -n "$player_log" && -f "$player_log" ]]; then
    rg -n -C 2 -- "DATAVO_ARENA_RESULT|Exception|error" "$player_log" >&2 || true
  fi
}

if [[ ! -x "$editor" ]]; then
  fail "Pinned Unity Editor is missing or not executable: $editor"
fi

if [[ ! -f "$settings_file" ]]; then
  fail "Unity arena Player settings file is missing: $settings_file"
fi

if ! [[ "$ticks" =~ ^[1-9][0-9]*$ ]] || (( ticks > 1000000 )); then
  fail "DATAVO_ARENA_TICKS must be an integer from 1 to 1000000."
fi

settings_before="$(shasum -a 256 "$settings_file" | awk '{print $1}')"
verify_settings_unchanged() {
  if [[ ! -f "$settings_file" ]]; then
    echo "Unity arena Player settings file disappeared: $settings_file" >&2
    return 1
  fi

  settings_after="$(shasum -a 256 "$settings_file" | awk '{print $1}')"
  if [[ "$settings_before" != "$settings_after" ]]; then
    echo "Unity macOS IL2CPP arena changed tracked ProjectSettings.asset: $settings_file" >&2
    echo "ProjectSettings SHA-256 before: $settings_before" >&2
    echo "ProjectSettings SHA-256 after:  $settings_after" >&2
    return 1
  fi
}

on_exit() {
  local exit_code=$?
  trap - EXIT
  if ! verify_settings_unchanged; then
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
  print_diagnostics
  fail "Unity macOS IL2CPP arena Player build failed with exit code $editor_exit."
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
  local mode="$1"
  local run_id="$2"
  local result_lines
  local result_count

  player_log="$output_root/player-$mode.log"

  set +e
  "$player" \
    -batchmode -nographics \
    -datavoArenaMode "$mode" \
    -datavoArenaRunId "$run_id" \
    -datavoArenaTicks "$ticks" \
    -logFile "$player_log" &
  player_pid=$!
  set -e

  deadline=$((SECONDS + 120))
  while kill -0 "$player_pid" 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      kill "$player_pid" 2>/dev/null || true
      wait "$player_pid" 2>/dev/null || true
      fail "Unity macOS IL2CPP arena $mode Player timed out after 120 seconds."
    fi

    sleep 1
  done

  set +e
  wait "$player_pid"
  player_exit=$?
  set -e

  if [[ $player_exit -ne 0 ]]; then
    print_diagnostics
    fail "Unity macOS IL2CPP arena $mode Player exited with code $player_exit."
  fi

  if [[ ! -f "$player_log" ]]; then
    fail "Unity macOS IL2CPP arena $mode Player log is missing: $player_log"
  fi

  result_lines="$(rg -o 'DATAVO_ARENA_RESULT=.*' "$player_log" || true)"
  result_count="$(printf '%s\n' "$result_lines" | sed '/^$/d' | wc -l | tr -d ' ')"

  if [[ "$result_count" != "1" ]]; then
    print_diagnostics
    fail "Unity macOS IL2CPP arena $mode Player must log exactly one DATAVO_ARENA_RESULT path; found $result_count."
  fi

  RUN_REPORT_PATH="${result_lines#DATAVO_ARENA_RESULT=}"

  if [[ ! -f "$RUN_REPORT_PATH" ]]; then
    fail "Unity macOS IL2CPP arena $mode report is missing: $RUN_REPORT_PATH"
  fi

  if ! node - "$RUN_REPORT_PATH" "$mode" <<'NODE'
const fs = require('node:fs');

const reportPath = process.argv[2];
const expectedMode = process.argv[3];
const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));

if (report.schemaVersion !== 1) throw new Error(`Unexpected arena report schema: ${report.schemaVersion}`);
if (report.status !== 'passed') throw new Error(`Arena status: ${report.status}`);
if (report.mode !== expectedMode) throw new Error(`Arena mode: ${report.mode}; expected ${expectedMode}`);
if (report.platform !== 'OSXPlayer') throw new Error(`Arena platform: ${report.platform}`);
if (report.scriptingBackend !== 'IL2CPP') throw new Error(`Arena backend: ${report.scriptingBackend}`);
if (!Number.isInteger(report.measuredTicks) || report.measuredTicks <= 0) {
  throw new Error(`Arena measured tick count: ${report.measuredTicks}`);
}
for (const key of ['leaderboardChecksum', 'aggregateChecksum']) {
  if (typeof report[key] !== 'string' || report[key].length !== 64) {
    throw new Error(`Arena ${key} must be a 64-character checksum.`);
  }
}
NODE
  then
    print_diagnostics
    fail "Unity macOS IL2CPP arena $mode result validation failed."
  fi
}

run_suffix="$(uuidgen | tr '[:upper:]' '[:lower:]')"
run_player reactive "reactive-$run_suffix"
reactive_report="$RUN_REPORT_PATH"
run_player polling "polling-$run_suffix"
polling_report="$RUN_REPORT_PATH"

if ! node - "$reactive_report" "$polling_report" <<'NODE'
const fs = require('node:fs');

const reactive = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const polling = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));

for (const key of ['leaderboardChecksum', 'aggregateChecksum']) {
  if (reactive[key] !== polling[key]) {
    throw new Error(`Reactive and polling ${key} values differ.`);
  }
}
NODE
then
  print_diagnostics
  fail "Unity macOS IL2CPP arena checksum equivalence validation failed."
fi

echo "Unity macOS IL2CPP arena benchmark passed."
echo "Player app: $app"
echo "Reactive report: $reactive_report"
echo "Polling report: $polling_report"
echo "Unity backend: IL2CPP"
echo "Unity Editor log: $editor_log"
echo "Unity Player logs: $output_root/player-reactive.log, $output_root/player-polling.log"
