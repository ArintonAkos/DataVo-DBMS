#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
editor="/Applications/Unity/Hub/Editor/6000.5.4f1/Unity.app/Contents/MacOS/Unity"
project="$root/tests/unity/DataVo.UnitySmoke"
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

bash "$root/scripts/prepare-unity-smoke-candidate.sh"
bash "$root/scripts/check-unity-smoke-project.sh" --require-candidate

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

player="$app/Contents/MacOS/DataVoUnitySmoke"

if [[ ! -x "$player" ]]; then
  fail "Unity macOS Player executable is missing or not executable: $player"
fi

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
