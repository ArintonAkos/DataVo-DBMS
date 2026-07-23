#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project="$root/demos/unity/DataVo.UnityArenaDemo"

for file in \
  "$project/Packages/manifest.json" \
  "$project/ProjectSettings/ProjectVersion.txt" \
  "$project/ProjectSettings/SceneTemplateSettings.json" \
  "$project/Assets/DataVoArena/Runtime/DataVo.UnityArena.Runtime.asmdef" \
  "$project/Assets/DataVoArena/Editor/DataVo.UnityArena.EditorTests.asmdef" \
  "$root/scripts/prepare-unity-arena-candidate.sh" \
  "$root/scripts/check-unity-arena-project.sh" \
  "$root/scripts/run-unity-arena-editor-tests.sh"; do
  test -f "$file" || {
    echo "Missing Unity arena file: $file" >&2
    exit 1
  }
done

rg -Fq '"com.datavo.core.candidate": "file:../.generated"' "$project/Packages/manifest.json"
rg -Fq '"com.unity.mathematics": "1.4.0"' "$project/Packages/manifest.json"
rg -Fq 'check-unity-runtime-bcl.sh' "$root/scripts/prepare-unity-arena-candidate.sh"
rg -Fq 'DataVo.Unity.TypeBinding' "$project/Assets/DataVoArena/Runtime/DataVo.UnityArena.Runtime.asmdef"
rg -Fq 'DATAVO_ARENA_RESULT=' "$project/Assets/DataVoArena/Runtime/ArenaHeadlessBootstrap.cs"
rg -Fq -- '-datavoArenaMode' "$project/Assets/DataVoArena/Runtime/ArenaHeadlessBootstrap.cs"
rg -Fq -- '-datavoArenaRunId' "$project/Assets/DataVoArena/Runtime/ArenaHeadlessBootstrap.cs"
rg -Fq 'ProfilerRecorder' "$project/Assets/DataVoArena/Runtime/ArenaHeadlessBootstrap.cs"
rg -Fq 'ArenaCrossEngineBootstrap' "$project/Assets/DataVoArena/Runtime/ArenaCrossEngineBootstrap.cs"
rg -Fq 'sqlite3_open_v2' "$project/Assets/DataVoArena/Runtime/Sqlite/ArenaSqliteNative.cs"
rg -Fq 'sqlite3_reset' "$project/Assets/DataVoArena/Runtime/Sqlite/ArenaSqliteNative.cs"
rg -Fq 'sqlite3_clear_bindings' "$project/Assets/DataVoArena/Runtime/Sqlite/ArenaSqliteNative.cs"
rg -Fq -- '-datavoArenaEngine' "$project/Assets/DataVoArena/Runtime/ArenaCrossEngineBootstrap.cs"
rg -Fq -- '-datavoArenaProfile' "$project/Assets/DataVoArena/Runtime/ArenaCrossEngineBootstrap.cs"
test -x "$root/scripts/run-unity-macos-il2cpp-arena-cross-engine.sh"
rg -Fq 'kill -KILL "$player_pid"' "$root/scripts/run-unity-macos-il2cpp-arena-cross-engine.sh"
rg -Fq 'validate_profile_equivalence "$profile"' "$root/scripts/run-unity-macos-il2cpp-arena-cross-engine.sh"
rg -Fq 'tail -n 40 "$player_log"' "$root/scripts/run-unity-macos-il2cpp-arena-cross-engine.sh"
rg -Fq 'BuildMacOSIl2Cpp' "$project/Assets/DataVoArena/Editor/ArenaPlayerBuild.cs"
rg -Fq 'ScriptingImplementation.IL2CPP' "$project/Assets/DataVoArena/Editor/ArenaPlayerBuild.cs"
rg -Fq 'ManagedStrippingLevel.Medium' "$project/Assets/DataVoArena/Editor/ArenaPlayerBuild.cs"
test -f "$root/scripts/run-unity-macos-il2cpp-arena.sh"
rg -Fq 'report.mode !== expectedMode' "$root/scripts/run-unity-macos-il2cpp-arena.sh"
rg -Fq 'result validation failed.' "$root/scripts/run-unity-macos-il2cpp-arena.sh"
rg -Fq 'checksum equivalence validation failed.' "$root/scripts/run-unity-macos-il2cpp-arena.sh"

node - "$project/Assets/DataVoArena/Editor/DataVo.UnityArena.EditorTests.asmdef" <<'NODE'
const fs = require('node:fs');

const asmdef = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const references = asmdef.references ?? [];

if (references.length !== 1 || references[0] !== 'DataVo.UnityArena.Runtime') {
  throw new Error('Arena test asmdef must reference only DataVo.UnityArena.Runtime; TestAssemblies provides Unity test runners.');
}
NODE

for generated_path in \
  "$project/Library" \
  "$project/Temp/" \
  "$project/UserSettings" \
  "$project/.generated" \
  "$project/Assets/DataVoArena/.generated/" \
  "$project/ProjectSettings/PackageManagerSettings.asset"; do
  git -C "$root" check-ignore -q --no-index "$generated_path" || {
    echo "Unity arena generated path is not ignored: $generated_path" >&2
    exit 1
  }
done

echo "Unity arena project contract passed."
