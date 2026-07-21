#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
checker="$root/scripts/check-unity-runtime-bcl.sh"
manifest="$root/tests/unity/DataVo.UnitySmoke/UnityRuntimeBclManifest.json"
smoke_project_check="$root/scripts/check-unity-smoke-project.sh"

[[ -f "$checker" ]] || {
  echo "Missing Unity runtime BCL checker: $checker" >&2
  exit 1
}

[[ -f "$manifest" ]] || {
  echo "Missing Unity runtime BCL manifest: $manifest" >&2
  exit 1
}

bash "$checker"

temp_root="$(mktemp -d)"
cleanup() { rm -rf "$temp_root"; }
trap cleanup EXIT HUP INT TERM

tampered_hash="$temp_root/tampered-hash.json"
cp "$manifest" "$tampered_hash"
node - "$tampered_hash" <<'NODE'
const fs = require("node:fs");

const manifestPath = process.argv[2];
const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
const json = manifest.assemblies.find((assembly) => assembly.fileName === "System.Text.Json.dll");
if (!json) throw new Error("System.Text.Json.dll is missing from the test manifest.");
json.sha256 = "0" + json.sha256.slice(1);
fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
NODE

if bash "$checker" --manifest "$tampered_hash" >"$temp_root/hash.stdout" 2>"$temp_root/hash.stderr"; then
  echo "Unity runtime BCL checker accepted a tampered hash." >&2
  exit 1
fi

rg -Fq "System.Text.Json.dll" "$temp_root/hash.stderr"
rg -Fq "SHA-256 mismatch" "$temp_root/hash.stderr"

tampered_identity="$temp_root/tampered-identity.json"
cp "$manifest" "$tampered_identity"
node - "$tampered_identity" <<'NODE'
const fs = require("node:fs");

const manifestPath = process.argv[2];
const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
const json = manifest.assemblies.find((assembly) => assembly.fileName === "System.Text.Json.dll");
if (!json) throw new Error("System.Text.Json.dll is missing from the test manifest.");
json.identity = json.identity.replace("Version=8.0.0.0", "Version=8.0.0.1");
fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
NODE

if bash "$checker" --manifest "$tampered_identity" >"$temp_root/identity.stdout" 2>"$temp_root/identity.stderr"; then
  echo "Unity runtime BCL checker accepted a tampered identity." >&2
  exit 1
fi

rg -Fq "System.Text.Json.dll" "$temp_root/identity.stderr"
rg -Fq "Assembly identity mismatch" "$temp_root/identity.stderr"

rg -Fq "UnityRuntimeBclManifest.json" "$smoke_project_check"
rg -Fq "check-unity-runtime-bcl.sh" "$smoke_project_check"

echo "Unity runtime BCL checker contract passed."
