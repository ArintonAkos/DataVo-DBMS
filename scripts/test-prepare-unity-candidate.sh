#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
prepare="$root/scripts/prepare-unity-candidate.sh"

if [[ ! -f "$prepare" ]]; then
  echo "Missing Unity candidate preparation command: $prepare" >&2
  exit 1
fi

temp_root="$(mktemp -d)"
cleanup() { rm -rf "$temp_root"; }
trap cleanup EXIT HUP INT TERM

fixture="$temp_root/fixture"
bash "$prepare" --output "$fixture"

node - "$fixture" <<'NODE'
const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const fixture = process.argv[2];
const readJson = (name) => JSON.parse(fs.readFileSync(path.join(fixture, name), "utf8"));
const exists = (name) => fs.existsSync(path.join(fixture, name));
const sha256 = (name) =>
  crypto.createHash("sha256").update(fs.readFileSync(path.join(fixture, name))).digest("hex");

function assert(condition, message) {
  if (!condition) {
    console.error(message);
    process.exit(1);
  }
}

const packageJson = readJson("package.json");
const candidate = readJson("candidate-manifest.json");
const dependencies = readJson("dependency-assets.json");
const inventory = readJson("assembly-inventory.json");

assert(packageJson.name === "com.datavo.core.candidate", "Fixture package name is incorrect.");
assert(packageJson.unity === "6000.0", "Fixture package must declare Unity 6000.0.");
assert(exists("Runtime/DataVo.Core.dll"), "Fixture is missing Runtime/DataVo.Core.dll.");
assert(fs.statSync(path.join(fixture, "Runtime/Dependencies")).isDirectory(), "Fixture is missing Runtime/Dependencies.");
assert(candidate.schemaVersion === 1, "Candidate manifest schemaVersion must be 1.");
assert(/^[0-9a-f]{40}$/.test(candidate.sourceCommit), "Candidate manifest must record the full source commit.");
assert(candidate.corePackage.id === "DataVo.Core", "Candidate manifest must identify DataVo.Core.");
assert(candidate.corePackage.assetPath === "lib/netstandard2.1/DataVo.Core.dll", "Candidate manifest must identify the portable Core asset.");
assert(/^[0-9a-f]{64}$/.test(candidate.corePackage.sha256), "Candidate manifest must include the package SHA-256.");
assert(dependencies.schemaVersion === 1 && Array.isArray(dependencies.dependencies), "Dependency assets inventory is malformed.");
assert(dependencies.dependencies.length > 0, "Candidate fixture must include the resolved managed dependency closure.");
assert(inventory.schemaVersion === 1 && Array.isArray(inventory.assemblies), "Assembly inventory is malformed.");
assert(Array.isArray(candidate.assemblies) && candidate.assemblies.length === inventory.assemblies.length, "Candidate manifest must preserve every assembly record.");

const coreRecords = candidate.assemblies.filter((assembly) => assembly.file === "Runtime/DataVo.Core.dll");
assert(coreRecords.length === 1, "Candidate manifest must contain exactly one DataVo.Core assembly.");
assert(coreRecords[0].source.assetPath === "lib/netstandard2.1/DataVo.Core.dll", "Core assembly provenance is incorrect.");

for (const assembly of candidate.assemblies) {
  assert(!path.isAbsolute(assembly.file), "Assembly path must be fixture-relative.");
  assert(exists(assembly.file), "Manifest assembly is missing: " + assembly.file);
  assert(sha256(assembly.file) === assembly.sha256, "Assembly hash mismatch: " + assembly.file);
  assert(typeof assembly.identity === "string" && assembly.identity.length > 0, "Assembly identity is missing.");
}

for (const dependency of dependencies.dependencies) {
  assert(!path.isAbsolute(dependency.fixturePath), "Dependency fixture path must be relative.");
  assert(exists(dependency.fixturePath), "Copied dependency is missing: " + dependency.fixturePath);
  assert(typeof dependency.packageId === "string" && dependency.packageId.length > 0, "Dependency package ID is missing.");
  assert(typeof dependency.packageVersion === "string" && dependency.packageVersion.length > 0, "Dependency package version is missing.");
  assert(typeof dependency.assetPath === "string" && dependency.assetPath.endsWith(".dll"), "Dependency asset path must name a DLL.");
}
NODE

preexisting="$temp_root/preexisting"
mkdir "$preexisting"
printf 'sentinel\n' > "$preexisting/keep.txt"

if bash "$prepare" --output "$preexisting" >"$temp_root/preexisting.stdout" 2>"$temp_root/preexisting.stderr"; then
  echo "Candidate preparation overwrote an existing output path." >&2
  exit 1
fi

[[ "$(cat "$preexisting/keep.txt")" == "sentinel" ]]
rg -Fq "already exists" "$temp_root/preexisting.stderr"

echo "Unity candidate preparation contract passed."
