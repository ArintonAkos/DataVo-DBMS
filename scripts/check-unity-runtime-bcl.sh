#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
unity_root="/Applications/Unity/Hub/Editor/6000.5.4f1/Unity.app/Contents"
manifest="$root/tests/unity/DataVo.UnitySmoke/UnityRuntimeBclManifest.json"
inspector_project="$root/tools/DataVo.UnityCandidateInspector/DataVo.UnityCandidateInspector.csproj"

usage() {
  echo "Usage: $0 [--unity-root <Unity.app/Contents>] [--manifest <path>]" >&2
  exit 64
}

while (($# > 0)); do
  case "$1" in
    --unity-root)
      (($# >= 2)) || usage
      unity_root="$2"
      shift 2
      ;;
    --manifest)
      (($# >= 2)) || usage
      manifest="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

[[ -d "$unity_root" ]] || {
  echo "Pinned Unity 6000.5.4f1 contents directory is missing: $unity_root" >&2
  exit 1
}

[[ -f "$manifest" ]] || {
  echo "Unity runtime BCL manifest is missing: $manifest" >&2
  exit 1
}

[[ -f "$inspector_project" ]] || {
  echo "Unity candidate inspector project is missing: $inspector_project" >&2
  exit 1
}

temp_root="$(mktemp -d)"
cleanup() { rm -rf "$temp_root"; }
trap cleanup EXIT HUP INT TERM

records="$temp_root/records.json"
node - "$manifest" "$records" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");

const [manifestPath, recordsPath] = process.argv.slice(2);
const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
const fail = (message) => { throw new Error("Invalid Unity runtime BCL manifest: " + message); };

if (manifest.schemaVersion !== 1) fail("schemaVersion must be 1.");
if (manifest.unity?.editorVersion !== "6000.5.4f1") fail("unity.editorVersion must be 6000.5.4f1.");
if (manifest.unity?.revision !== "d550df8bd089") fail("unity.revision must be d550df8bd089.");
if (!Array.isArray(manifest.assemblies) || manifest.assemblies.length === 0) fail("assemblies must be a non-empty array.");

const fileNames = new Set();
const runtimePaths = new Set();
let previousFileName = "";
const records = manifest.assemblies.map((assembly) => {
  if (!assembly || typeof assembly !== "object") fail("assembly entries must be objects.");
  const { fileName, runtimePath, identity, sha256 } = assembly;
  if (typeof fileName !== "string" || !fileName.endsWith(".dll") || path.basename(fileName) !== fileName) {
    fail("assembly fileName must be a DLL basename.");
  }
  if (typeof runtimePath !== "string" || path.isAbsolute(runtimePath) || runtimePath.split(/[\\/]/).includes("..")) {
    fail("runtimePath for " + fileName + " must be a safe relative path.");
  }
  if (typeof identity !== "string" || identity.length === 0) fail("identity for " + fileName + " is missing.");
  if (typeof sha256 !== "string" || !/^[0-9a-f]{64}$/.test(sha256)) fail("sha256 for " + fileName + " must be lowercase hexadecimal.");
  if (fileNames.has(fileName)) fail("duplicate fileName: " + fileName);
  if (runtimePaths.has(runtimePath)) fail("duplicate runtimePath: " + runtimePath);
  if (previousFileName.localeCompare(fileName) >= 0) fail("assemblies must be sorted by fileName.");
  fileNames.add(fileName);
  runtimePaths.add(runtimePath);
  previousFileName = fileName;
  return { fileName, runtimePath: runtimePath.replaceAll("\\", "/"), identity, sha256 };
});

fs.writeFileSync(recordsPath, JSON.stringify(records, null, 2) + "\n");
NODE

assembly_paths=()
while IFS= read -r assembly_path; do
  assembly_paths+=("$assembly_path")
done < <(node - "$records" "$unity_root" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");
const [recordsPath, unityRoot] = process.argv.slice(2);
for (const record of JSON.parse(fs.readFileSync(recordsPath, "utf8"))) {
  const assemblyPath = path.join(unityRoot, record.runtimePath);
  if (!fs.statSync(assemblyPath, { throwIfNoEntry: false })?.isFile()) {
    throw new Error(`Unity runtime assembly is missing for ${record.fileName}: ${assemblyPath}`);
  }
  process.stdout.write(assemblyPath + "\n");
}
NODE
)

for assembly_path in "${assembly_paths[@]}"; do
  actual_sha256="$(shasum -a 256 "$assembly_path" | awk '{ print $1 }')"
  expected_sha256="$(node - "$records" "$assembly_path" "$unity_root" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");
const [recordsPath, assemblyPath, unityRoot] = process.argv.slice(2);
const relativePath = path.relative(unityRoot, assemblyPath).replaceAll(path.sep, "/");
const record = JSON.parse(fs.readFileSync(recordsPath, "utf8")).find((entry) => entry.runtimePath === relativePath);
if (!record) throw new Error("No manifest record for " + assemblyPath);
process.stdout.write(record.sha256 + "\n");
NODE
)"

  if [[ "$actual_sha256" != "$expected_sha256" ]]; then
    file_name="$(basename "$assembly_path")"
    echo "SHA-256 mismatch for $file_name: expected $expected_sha256, observed $actual_sha256, runtime path: $assembly_path" >&2
    exit 1
  fi
done

inventory="$temp_root/assembly-identities.json"
dotnet run --project "$inspector_project" -- \
  --identity-output "$inventory" \
  "${assembly_paths[@]}" \
  >/dev/null

node - "$records" "$inventory" "$unity_root" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");

const [recordsPath, inventoryPath, unityRoot] = process.argv.slice(2);
const records = JSON.parse(fs.readFileSync(recordsPath, "utf8"));
const inventory = JSON.parse(fs.readFileSync(inventoryPath, "utf8"));
if (inventory.schemaVersion !== 1 || !Array.isArray(inventory.assemblies)) {
  throw new Error("Unity candidate inspector produced an invalid assembly identity inventory.");
}

const bySourcePath = new Map(records.map((record) => [path.resolve(unityRoot, record.runtimePath), record]));
if (inventory.assemblies.length !== bySourcePath.size) {
  throw new Error("Unity candidate inspector returned the wrong number of runtime assemblies.");
}

for (const assembly of inventory.assemblies) {
  const sourcePath = path.resolve(assembly.sourcePath);
  const record = bySourcePath.get(sourcePath);
  if (!record) throw new Error("Unity candidate inspector returned an undeclared assembly: " + sourcePath);
  if (assembly.identity !== record.identity) {
    throw new Error(`Assembly identity mismatch for ${record.fileName}: expected ${record.identity}, observed ${assembly.identity}, runtime path: ${sourcePath}`);
  }
}
NODE

echo "Verified Unity runtime BCL inventory: $manifest"
