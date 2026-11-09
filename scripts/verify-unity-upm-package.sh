#!/usr/bin/env bash
# Installs a built com.datavo.core tarball into a copy of the smoke project, then runs the EditMode suite
# and the macOS IL2CPP Player smoke against that installed package. Needs an activated Unity 6000.5.4f1.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
[[ $# -eq 1 && -f "$1" ]] || { echo "Usage: $0 <com.datavo.core-version.tgz>" >&2; exit 64; }
tarball="$(cd "$(dirname "$1")" && pwd -P)/$(basename "$1")"

project="$(mktemp -d /private/tmp/datavo-unity-upm.XXXXXX)/DataVo.UnitySmoke"
mkdir -p "$project"
for entry in Assets Packages ProjectSettings UnityRuntimeBclManifest.json; do
  cp -R "$root/tests/unity/DataVo.UnitySmoke/$entry" "$project/"
done
rm -f "$project/Packages/packages-lock.json"

node - "$project/Packages/manifest.json" "$tarball" "$root" <<'NODE'
const fs = require("node:fs");
const [manifestPath, tarball, root] = process.argv.slice(2);
const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
delete manifest.dependencies["com.datavo.core.candidate"];
manifest.dependencies["com.datavo.core"] = `file:${tarball}`;
manifest.dependencies["com.datavo.unity.mathematics"] = `file:${root}/integrations/unity/com.datavo.unity.mathematics`;
fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
NODE

printf '{"dependencies":{}}\n' > "$project/Packages/packages-lock.json"
export DATAVO_UNITY_SMOKE_PROJECT="$project"
bash "$root/scripts/run-unity-editor-tests.sh"
bash "$root/scripts/run-unity-macos-il2cpp-player-smoke.sh"
echo "com.datavo.core passed in the Editor and in a macOS IL2CPP Player: $tarball"
