#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project="$root/demos/unity/DataVo.UnityArenaDemo"

for file in \
  "$project/Packages/manifest.json" \
  "$project/ProjectSettings/ProjectVersion.txt" \
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
