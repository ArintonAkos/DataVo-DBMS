#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tool_project="$root/tools/DataVo.UnityCandidateInspector/DataVo.UnityCandidateInspector.csproj"
tool_dll="$root/tools/DataVo.UnityCandidateInspector/bin/Debug/net10.0/DataVo.UnityCandidateInspector.dll"

if [[ ! -f "$tool_project" ]]; then
  echo "Missing candidate inspector project: $tool_project" >&2
  exit 1
fi

if [[ ! -f "$tool_dll" ]]; then
  echo "Missing built candidate inspector: $tool_dll" >&2
  exit 1
fi

temp_root="$(mktemp -d)"
cleanup() { rm -rf "$temp_root"; }
trap cleanup EXIT HUP INT TERM

fixture_root="$temp_root/fixture"
runtime_dir="$fixture_root/Runtime"
mkdir -p "$runtime_dir"

portable_dll="$root/DataVo.Core/bin/Debug/netstandard2.1/DataVo.Core.dll"
[[ -f "$portable_dll" ]] || {
  echo "Missing portable DataVo.Core build output: $portable_dll. Build DataVo.Core with DataVoEnablePortableTarget=true first." >&2
  exit 1
}

copied_dll="$runtime_dir/DataVo.Core.dll"
cp "$portable_dll" "$copied_dll"

inventory="$fixture_root/assembly-inventory.json"
dotnet "$tool_dll" \
  --root "$fixture_root" \
  --output "$inventory" \
  "$copied_dll"

node - "$inventory" <<'NODE'
const fs = require("node:fs");
const inventory = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));

function assert(condition, message) {
  if (!condition) {
    console.error(message);
    process.exit(1);
  }
}

assert(inventory.schemaVersion === 1, "Assembly inventory schemaVersion must be 1.");
assert(Array.isArray(inventory.assemblies) && inventory.assemblies.length === 1, "Expected exactly one inspected assembly.");

const assembly = inventory.assemblies[0];
assert(assembly.file === "Runtime/DataVo.Core.dll", "Assembly path must be fixture-relative.");
assert(typeof assembly.sha256 === "string" && /^[0-9a-f]{64}$/.test(assembly.sha256), "Assembly SHA-256 must be lowercase hex.");
assert(typeof assembly.identity === "string" && assembly.identity.startsWith("DataVo.Core,"), "Expected DataVo.Core assembly identity.");
assert(Array.isArray(assembly.publicSystemTypes), "Assembly inventory must include publicSystemTypes.");
NODE

duplicate_output="$fixture_root/duplicate.json"
if dotnet "$tool_dll" \
  --root "$fixture_root" \
  --output "$duplicate_output" \
  "$copied_dll" \
  "$copied_dll" >"$temp_root/duplicate.stdout" 2>"$temp_root/duplicate.stderr"; then
  echo "Candidate inspector accepted duplicate assembly identity." >&2
  exit 1
fi

rg -Fq "Duplicate assembly identity" "$temp_root/duplicate.stderr"

identity_inventory="$temp_root/dependency-identities.json"
dotnet "$tool_dll" \
  --identity-output "$identity_inventory" \
  "$portable_dll" \
  "$copied_dll"

node - "$identity_inventory" "$portable_dll" "$copied_dll" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");

const [inventoryPath, ...sourcePaths] = process.argv.slice(2);
const inventory = JSON.parse(fs.readFileSync(inventoryPath, "utf8"));

function assert(condition, message) {
  if (!condition) {
    console.error(message);
    process.exit(1);
  }
}

assert(inventory.schemaVersion === 1, "Identity inventory schemaVersion must be 1.");
assert(Array.isArray(inventory.assemblies) && inventory.assemblies.length === sourcePaths.length, "Identity inventory must contain every requested assembly.");

const expectedPaths = sourcePaths.map((sourcePath) => path.resolve(sourcePath)).sort((left, right) => left.localeCompare(right));
assert(JSON.stringify(inventory.assemblies.map((assembly) => assembly.sourcePath)) === JSON.stringify(expectedPaths), "Identity inventory assemblies must be sorted by absolute source path.");

for (const assembly of inventory.assemblies) {
  assert(path.isAbsolute(assembly.sourcePath), "Identity inventory sourcePath must be absolute.");
  assert(assembly.fileName === path.basename(assembly.sourcePath), "Identity inventory fileName must match sourcePath.");
  assert(/^[0-9a-f]{64}$/.test(assembly.sha256), "Identity inventory SHA-256 must be lowercase hex.");
  assert(typeof assembly.identity === "string" && assembly.identity.startsWith("DataVo.Core,"), "Identity inventory must contain the full assembly identity.");
  assert(!Object.hasOwn(assembly, "publicSystemTypes"), "Identity-only inventory must not contain publicSystemTypes.");
}
NODE

missing_dll="$temp_root/missing.dll"
if dotnet "$tool_dll" --identity-output "$temp_root/missing.json" "$missing_dll" >"$temp_root/missing.stdout" 2>"$temp_root/missing.stderr"; then
  echo "Candidate inspector accepted a missing identity-mode assembly." >&2
  exit 1
fi

rg -Fq "Assembly does not exist" "$temp_root/missing.stderr"
rg -Fq "$missing_dll" "$temp_root/missing.stderr"

echo "Unity candidate inspector contract passed."
