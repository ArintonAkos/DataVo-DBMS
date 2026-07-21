#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runtime="$root/tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Runtime/PlayerSmokeBootstrap.cs"
asmdef="$root/tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Runtime/DataVo.UnitySmoke.Player.asmdef"

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

echo "Unity macOS IL2CPP Player smoke source contract passed."
