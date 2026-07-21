#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project="$root/demos/unity/DataVo.UnityArenaDemo"
runtime_bcl_manifest="$root/tests/unity/DataVo.UnitySmoke/UnityRuntimeBclManifest.json"
runtime_bcl_checker="$root/scripts/check-unity-runtime-bcl.sh"
require_candidate=false

if [[ $# -gt 1 ]]; then
  echo "Usage: $0 [--require-candidate]" >&2
  exit 2
fi

if [[ $# -eq 1 ]]; then
  if [[ "$1" != "--require-candidate" ]]; then
    echo "Usage: $0 [--require-candidate]" >&2
    exit 2
  fi

  require_candidate=true
fi

for file in \
  "$project/ProjectSettings/ProjectVersion.txt" \
  "$project/ProjectSettings/ProjectSettings.asset" \
  "$project/Packages/manifest.json" \
  "$project/Packages/packages-lock.json" \
  "$runtime_bcl_manifest" \
  "$runtime_bcl_checker"; do
  if [[ ! -f "$file" ]]; then
    echo "Unity arena project file is missing: $file" >&2
    exit 1
  fi
done

node - "$project" "$require_candidate" <<'NODE'
const fs = require('node:fs');
const path = require('node:path');

const [project, requireCandidate] = process.argv.slice(2);
const projectVersion = fs.readFileSync(path.join(project, 'ProjectSettings', 'ProjectVersion.txt'), 'utf8');
const requiredVersionLines = [
  'm_EditorVersion: 6000.5.4f1',
  'm_EditorVersionWithRevision: 6000.5.4f1 (d550df8bd089)'
];

for (const line of requiredVersionLines) {
  if (!projectVersion.includes(line)) {
    throw new Error(`Unity arena project version is missing: ${line}`);
  }
}

const manifest = JSON.parse(fs.readFileSync(path.join(project, 'Packages', 'manifest.json'), 'utf8'));
const expectedDependencies = {
  'com.unity.mathematics': '1.4.0',
  'com.unity.test-framework': '1.7.0',
  'com.datavo.core.candidate': 'file:../.generated',
  'com.datavo.unity.mathematics': 'file:../../../../integrations/unity/com.datavo.unity.mathematics'
};

for (const [name, expected] of Object.entries(expectedDependencies)) {
  if (manifest.dependencies?.[name] !== expected) {
    throw new Error(`Manifest dependency ${name} must be ${expected}.`);
  }
}

const lock = JSON.parse(fs.readFileSync(path.join(project, 'Packages', 'packages-lock.json'), 'utf8'));
const lockedDependencies = lock.dependencies ?? {};
for (const [name, expectedVersion] of Object.entries({
  'com.unity.mathematics': '1.4.0',
  'com.unity.test-framework': '1.7.0'
})) {
  if (lockedDependencies[name]?.version !== expectedVersion) {
    throw new Error(`Lock dependency ${name} must be ${expectedVersion}.`);
  }
}

for (const name of ['com.datavo.core.candidate', 'com.datavo.unity.mathematics']) {
  if (lockedDependencies[name]?.source !== 'local') {
    throw new Error(`Lock dependency ${name} must have local source.`);
  }
}

if (requireCandidate === 'true') {
  const candidateLink = path.join(project, '.generated');
  const linkStatus = fs.lstatSync(candidateLink, { throwIfNoEntry: false });
  if (!linkStatus?.isSymbolicLink()) {
    throw new Error(`Generated candidate link is missing: ${candidateLink}`);
  }

  const candidateRoot = fs.realpathSync(candidateLink);
  for (const relativePath of ['package.json', 'candidate-manifest.json', 'Runtime/DataVo.Core.dll']) {
    if (!fs.statSync(path.join(candidateRoot, relativePath), { throwIfNoEntry: false })?.isFile()) {
      throw new Error(`Generated candidate file is missing: ${relativePath}`);
    }
  }

  const candidatePackage = JSON.parse(fs.readFileSync(path.join(candidateRoot, 'package.json'), 'utf8'));
  if (candidatePackage.name !== 'com.datavo.core.candidate') {
    throw new Error('Generated candidate package name must be com.datavo.core.candidate.');
  }
}
NODE

echo "Unity arena project contract passed."
