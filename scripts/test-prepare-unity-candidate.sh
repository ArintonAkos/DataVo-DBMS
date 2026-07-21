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
assert(candidate.schemaVersion === 2, "Candidate manifest schemaVersion must be 2.");
assert(/^[0-9a-f]{40}$/.test(candidate.sourceCommit), "Candidate manifest must record the full source commit.");
assert(candidate.corePackage.id === "DataVo.Core", "Candidate manifest must identify DataVo.Core.");
assert(candidate.corePackage.assetPath === "lib/netstandard2.1/DataVo.Core.dll", "Candidate manifest must identify the portable Core asset.");
assert(/^[0-9a-f]{64}$/.test(candidate.corePackage.sha256), "Candidate manifest must include the package SHA-256.");
assert(dependencies.schemaVersion === 2 && Array.isArray(dependencies.dependencies), "Dependency assets inventory is malformed.");
assert(dependencies.dependencies.length > 0, "Candidate fixture must include the resolved managed dependency closure.");
const jsonDependency = dependencies.dependencies.find((dependency) => dependency.packageId === "System.Text.Json");
assert(jsonDependency, "Candidate fixture must record the System.Text.Json dependency.");
assert(jsonDependency.packageVersion === "8.0.0", "Portable candidate must resolve System.Text.Json 8.0.0 for Unity compatibility.");
assert(jsonDependency.resolution === "unity-provided", "System.Text.Json must be provided by the pinned Unity runtime.");
assert(inventory.schemaVersion === 1 && Array.isArray(inventory.assemblies), "Assembly inventory is malformed.");
assert(Array.isArray(candidate.assemblies) && candidate.assemblies.length === inventory.assemblies.length, "Candidate manifest must preserve every assembly record.");
assert(candidate.unityRuntimeInventory?.path === "tests/unity/DataVo.UnitySmoke/UnityRuntimeBclManifest.json", "Candidate manifest must reference the tracked Unity runtime inventory.");
assert(/^[0-9a-f]{64}$/.test(candidate.unityRuntimeInventory?.sha256 ?? ""), "Candidate manifest must hash the Unity runtime inventory.");

const coreRecords = candidate.assemblies.filter((assembly) => assembly.file === "Runtime/DataVo.Core.dll");
assert(coreRecords.length === 1, "Candidate manifest must contain exactly one DataVo.Core assembly.");
assert(coreRecords[0].source.assetPath === "lib/netstandard2.1/DataVo.Core.dll", "Core assembly provenance is incorrect.");

for (const assembly of candidate.assemblies) {
  assert(!path.isAbsolute(assembly.file), "Assembly path must be fixture-relative.");
  assert(exists(assembly.file), "Manifest assembly is missing: " + assembly.file);
  assert(sha256(assembly.file) === assembly.sha256, "Assembly hash mismatch: " + assembly.file);
  assert(typeof assembly.identity === "string" && assembly.identity.length > 0, "Assembly identity is missing.");
}

const inventoryFiles = new Set(inventory.assemblies.map((assembly) => assembly.file));
const candidateFiles = new Set(candidate.assemblies.map((assembly) => assembly.file));
for (const dependency of dependencies.dependencies) {
  assert(typeof dependency.packageId === "string" && dependency.packageId.length > 0, "Dependency package ID is missing.");
  assert(typeof dependency.packageVersion === "string" && dependency.packageVersion.length > 0, "Dependency package version is missing.");
  assert(typeof dependency.assetPath === "string" && dependency.assetPath.endsWith(".dll"), "Dependency asset path must name a DLL.");
  assert(typeof dependency.identity === "string" && dependency.identity.length > 0, "Dependency identity is missing.");

  if (dependency.resolution === "copied") {
    assert(!path.isAbsolute(dependency.fixturePath), "Copied dependency fixture path must be relative.");
    assert(exists(dependency.fixturePath), "Copied dependency is missing: " + dependency.fixturePath);
    assert(inventoryFiles.has(dependency.fixturePath), "Copied dependency is missing from assembly inventory: " + dependency.fixturePath);
    assert(candidateFiles.has(dependency.fixturePath), "Copied dependency is missing from candidate manifest: " + dependency.fixturePath);
  } else if (dependency.resolution === "unity-provided") {
    assert(!Object.hasOwn(dependency, "fixturePath"), "Unity-provided dependency must not have a fixture path.");
    assert(typeof dependency.unityRuntimePath === "string" && dependency.unityRuntimePath.length > 0, "Unity-provided dependency must identify the Unity runtime path.");
    assert(dependency.unityRuntimeInventory === "tests/unity/DataVo.UnitySmoke/UnityRuntimeBclManifest.json", "Unity-provided dependency must identify the tracked inventory.");
    assert(!inventoryFiles.has("Runtime/Dependencies/" + path.basename(dependency.assetPath)), "Unity-provided dependency must not be in assembly inventory.");
    assert(!candidateFiles.has("Runtime/Dependencies/" + path.basename(dependency.assetPath)), "Unity-provided dependency must not be in candidate manifest.");
  } else {
    assert(false, "Dependency has an unknown resolution: " + dependency.resolution);
  }
}

for (const provider of [
  "Microsoft.Bcl.AsyncInterfaces.dll",
  "System.Runtime.CompilerServices.Unsafe.dll",
  "System.Text.Encodings.Web.dll",
  "System.Text.Json.dll"
]) {
  assert(!exists("Runtime/Dependencies/" + provider), "Unity-provided framework assembly must not be copied: " + provider);
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
