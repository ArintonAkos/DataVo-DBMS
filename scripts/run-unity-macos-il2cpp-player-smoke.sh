#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
editor="/Applications/Unity/Hub/Editor/6000.5.4f1/Unity.app/Contents/MacOS/Unity"
# DATAVO_UNITY_SMOKE_PROJECT points these checks at a prepared copy that installs the real package.
project="${DATAVO_UNITY_SMOKE_PROJECT:-$root/tests/unity/DataVo.UnitySmoke}"
uses_tracked_project=$([[ -z "${DATAVO_UNITY_SMOKE_PROJECT:-}" ]] && echo true || echo false)
settings_file="$project/ProjectSettings/ProjectSettings.asset"
output_root="$(mktemp -d /private/tmp/datavo-unity-il2cpp.XXXXXX)"
app="$output_root/DataVoUnitySmoke.app"
editor_log="$output_root/editor.log"
player_log="$output_root/player.log"
run_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"

fail() {
  echo "$1" >&2
  echo "Unity Editor log: $editor_log" >&2
  echo "Unity Player log: $player_log" >&2
  exit 1
}

print_build_diagnostics() {
  if [[ -f "$editor_log" ]]; then
    rg -n -C 2 -- "mac-il2cpp|IL2CPP Player build failed|InvalidOperationException" "$editor_log" >&2 || true
  fi
}

if [[ ! -x "$editor" ]]; then
  fail "Pinned Unity Editor is missing or not executable: $editor"
fi

if [[ ! -f "$settings_file" ]]; then
  fail "Unity Player settings file is missing: $settings_file"
fi

settings_before="$(shasum -a 256 "$settings_file" | awk '{print $1}')"
verify_settings_unchanged() {
  if [[ ! -f "$settings_file" ]]; then
    echo "Unity Player settings file disappeared: $settings_file" >&2
    return 1
  fi

  settings_after="$(shasum -a 256 "$settings_file" | awk '{print $1}')"
  if [[ "$settings_before" != "$settings_after" ]]; then
    echo "Unity macOS IL2CPP Player smoke changed tracked ProjectSettings.asset: $settings_file" >&2
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
if [[ "$uses_tracked_project" == true ]]; then
  bash "$root/scripts/prepare-unity-smoke-candidate.sh"
  bash "$root/scripts/check-unity-smoke-project.sh" --require-candidate
fi

set +e
"$editor" \
  -batchmode -quit \
  -projectPath "$project" \
  -executeMethod DataVo.UnitySmoke.Editor.PlayerSmokeBuild.BuildMacOSIl2Cpp \
  -datavoSmokeBuildPath "$app" \
  -logFile "$editor_log"
editor_exit=$?
set -e

if [[ $editor_exit -ne 0 ]]; then
  print_build_diagnostics
  fail "Unity macOS IL2CPP Player build failed with exit code $editor_exit."
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

set +e
"$player" \
  -batchmode -nographics \
  -datavoSmokeRunId "$run_id" \
  -logFile "$player_log" &
player_pid=$!
set -e

deadline=$((SECONDS + 120))

while kill -0 "$player_pid" 2>/dev/null; do
  if (( SECONDS >= deadline )); then
    kill "$player_pid" 2>/dev/null || true
    wait "$player_pid" 2>/dev/null || true
    fail "Unity macOS IL2CPP Player smoke timed out after 120 seconds."
  fi

  sleep 1
done

set +e
wait "$player_pid"
player_exit=$?
set -e

if [[ $player_exit -ne 0 ]]; then
  fail "Unity macOS IL2CPP Player exited with code $player_exit."
fi

if [[ ! -f "$player_log" ]]; then
  fail "Unity macOS IL2CPP Player log is missing: $player_log"
fi

result_lines="$(rg -o 'DATAVO_SMOKE_RESULT=.*' "$player_log" || true)"
result_count="$(printf '%s\n' "$result_lines" | sed '/^$/d' | wc -l | tr -d ' ')"

if [[ "$result_count" != "1" ]]; then
  fail "Unity macOS IL2CPP Player must log exactly one DATAVO_SMOKE_RESULT path; found $result_count."
fi

report_path="${result_lines#DATAVO_SMOKE_RESULT=}"

if [[ ! -f "$report_path" ]]; then
  fail "Unity macOS IL2CPP Player report is missing: $report_path"
fi

node - "$report_path" <<'NODE'
const fs = require('node:fs');

const reportPath = process.argv[2];
const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));

if (report.schemaVersion !== 1) {
  throw new Error(`Unexpected Player smoke report schema: ${report.schemaVersion}`);
}

if (report.status !== 'passed') {
  throw new Error(`Player smoke status: ${report.status}`);
}

if (report.scriptingBackend !== 'IL2CPP') {
  throw new Error(`Player was not IL2CPP: ${report.scriptingBackend}`);
}

if (!Number.isInteger(report.scenarioCount) || report.scenarioCount < 3) {
  throw new Error(`Player smoke report has too few scenarios: ${report.scenarioCount}`);
}
NODE

echo "Unity macOS IL2CPP Player smoke passed."
echo "Player app: $app"
echo "Player report: $report_path"
echo "Unity backend: IL2CPP"
echo "Unity Editor log: $editor_log"
echo "Unity Player log: $player_log"
