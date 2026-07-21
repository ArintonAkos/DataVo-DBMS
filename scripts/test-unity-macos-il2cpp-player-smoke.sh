#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runtime="$root/tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Runtime/PlayerSmokeBootstrap.cs"
asmdef="$root/tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Runtime/DataVo.UnitySmoke.Player.asmdef"
build="$root/tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Editor/PlayerSmokeBuild.cs"
runner="$root/scripts/run-unity-macos-il2cpp-player-smoke.sh"
candidate_prepare="$root/scripts/prepare-unity-smoke-candidate.sh"
editor_runner="$root/scripts/run-unity-editor-tests.sh"

require_text() {
  local file="$1"
  local expected="$2"

  if ! rg -Fq -- "$expected" "$file"; then
    echo "Missing required Player smoke contract: $expected ($file)" >&2
    exit 1
  fi
}

require_text "$runtime" "[RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.AfterSceneLoad)]"
require_text "$runtime" "context.UseUnityTypes();"
require_text "$runtime" "context.Insert(\"States\", source);"
require_text "$runtime" "context.Map<PlayerSmokeState>(row)"
require_text "$runtime" "context.Get<Vector3>(row, \"Position\")"
require_text "$runtime" "Application.Quit(exitCode);"
require_text "$runtime" "Application.persistentDataPath"
require_text "$asmdef" "DataVo.Unity.TypeBinding"
require_text "$asmdef" "DataVo.Core.dll"
require_text "$build" "ScriptingImplementation.IL2CPP"
require_text "$build" "ManagedStrippingLevel.Medium"
require_text "$build" "BuildTarget.StandaloneOSX"
require_text "$build" "mac-il2cpp"
require_text "$build" "EditorApplication.applicationContentsPath"
require_text "$runner" "prepare-unity-smoke-candidate.sh"
require_text "$runner" "-executeMethod DataVo.UnitySmoke.Editor.PlayerSmokeBuild.BuildMacOSIl2Cpp"
require_text "$runner" "-datavoSmokeRunId"
require_text "$runner" "DATAVO_SMOKE_RESULT="
require_text "$runner" "scriptingBackend !== 'IL2CPP'"
require_text "$runner" "status !== 'passed'"
require_text "$runner" "print_build_diagnostics"
require_text "$runner" "mac-il2cpp|IL2CPP Player build failed|InvalidOperationException"
require_text "$candidate_prepare" "check-unity-runtime-bcl.sh"
require_text "$editor_runner" "check-unity-runtime-bcl.sh"
require_text "$runner" "check-unity-runtime-bcl.sh"
require_text "$runner" "ProjectSettings/ProjectSettings.asset"
require_text "$runner" "settings_before"
require_text "$runner" "settings_after"
require_text "$runner" "player_candidates"
require_text "$runner" "-perm -111"

echo "Unity macOS IL2CPP Player smoke source contract passed."
