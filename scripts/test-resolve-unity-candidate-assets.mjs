#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { resolveCandidateAssets } from "./resolve-unity-candidate-assets.mjs";

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "datavo-unity-resolver-"));

function removeTempRoot() {
  fs.rmSync(tempRoot, { recursive: true, force: true });
}

function createDependency(name, contents = name) {
  const sourcePath = path.join(tempRoot, "sources", name);
  fs.mkdirSync(path.dirname(sourcePath), { recursive: true });
  fs.writeFileSync(sourcePath, contents);
  return sourcePath;
}

function dependency(packageId, packageVersion, assetPath, sourcePath) {
  return { packageId, packageVersion, assetPath, sourcePath };
}

function identityRecord(sourcePath, identity) {
  return {
    sourcePath: path.resolve(sourcePath),
    fileName: path.basename(sourcePath),
    sha256: "a".repeat(64),
    identity
  };
}

const unityInventory = {
  schemaVersion: 1,
  assemblies: [
    {
      fileName: "System.Text.Json.dll",
      runtimePath: "Resources/Scripting/BCLExtensions/runtime/netstandard2.1/System.Text.Json.dll",
      identity: "System.Text.Json, Version=8.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
      sha256: "b".repeat(64)
    }
  ]
};

function resolveScenario(name, dependencies, identityInventory, inventory = unityInventory) {
  const scenarioRoot = path.join(tempRoot, name);
  const runtimeDirectory = path.join(scenarioRoot, "Runtime", "Dependencies");
  const outputPath = path.join(scenarioRoot, "dependency-assets.json");
  resolveCandidateAssets({
    dependencies,
    identityInventory,
    unityRuntimeInventory: inventory,
    unityRuntimeInventoryReference: "tests/unity/DataVo.UnitySmoke/UnityRuntimeBclManifest.json",
    runtimeDirectory,
    outputPath
  });
  return {
    runtimeDirectory,
    output: JSON.parse(fs.readFileSync(outputPath, "utf8"))
  };
}

try {
  const copiedSource = createDependency("Custom.Dependency.dll", "copied-once");
  const copied = resolveScenario(
    "copied",
    [dependency("Custom.Dependency", "1.2.3", "lib/netstandard2.1/Custom.Dependency.dll", copiedSource)],
    { schemaVersion: 1, assemblies: [identityRecord(copiedSource, "Custom.Dependency, Version=1.2.3.0, Culture=neutral, PublicKeyToken=null")] }
  );

  assert.equal(copied.output.schemaVersion, 2);
  assert.equal(copied.output.dependencies.length, 1);
  assert.deepEqual(copied.output.dependencies[0], {
    packageId: "Custom.Dependency",
    packageVersion: "1.2.3",
    assetPath: "lib/netstandard2.1/Custom.Dependency.dll",
    identity: "Custom.Dependency, Version=1.2.3.0, Culture=neutral, PublicKeyToken=null",
    resolution: "copied",
    fixturePath: "Runtime/Dependencies/Custom.Dependency.dll"
  });
  assert.equal(fs.readFileSync(path.join(copied.runtimeDirectory, "Custom.Dependency.dll"), "utf8"), "copied-once");
  assert.equal(fs.readdirSync(copied.runtimeDirectory).length, 1);

  const jsonSource = createDependency("System.Text.Json.dll", "provided-by-unity");
  const unityProvided = resolveScenario(
    "unity-provided",
    [dependency("System.Text.Json", "8.0.6", "lib/netstandard2.0/System.Text.Json.dll", jsonSource)],
    { schemaVersion: 1, assemblies: [identityRecord(jsonSource, unityInventory.assemblies[0].identity)] }
  );

  assert.deepEqual(unityProvided.output.dependencies[0], {
    packageId: "System.Text.Json",
    packageVersion: "8.0.6",
    assetPath: "lib/netstandard2.0/System.Text.Json.dll",
    identity: unityInventory.assemblies[0].identity,
    resolution: "unity-provided",
    unityRuntimePath: unityInventory.assemblies[0].runtimePath,
    unityRuntimeInventory: "tests/unity/DataVo.UnitySmoke/UnityRuntimeBclManifest.json"
  });
  assert.deepEqual(fs.readdirSync(unityProvided.runtimeDirectory), []);

  const conflictSource = createDependency("conflict/System.Text.Json.dll", "conflict");
  assert.throws(
    () => resolveScenario(
      "conflict",
      [dependency("System.Text.Json", "10.0.0", "lib/netstandard2.0/System.Text.Json.dll", conflictSource)],
      { schemaVersion: 1, assemblies: [identityRecord(conflictSource, "System.Text.Json, Version=10.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51")] }
    ),
    (error) =>
      error instanceof Error &&
      error.message.includes("System.Text.Json 10.0.0") &&
      error.message.includes("Version=10.0.0.0") &&
      error.message.includes(unityInventory.assemblies[0].identity) &&
      error.message.includes(unityInventory.assemblies[0].runtimePath)
  );

  const duplicateLeft = createDependency("duplicate/left/Shared.dll");
  const duplicateRight = createDependency("duplicate/right/Shared.dll");
  assert.throws(
    () => resolveScenario(
      "duplicate-basename",
      [
        dependency("Left", "1.0.0", "lib/netstandard2.1/Shared.dll", duplicateLeft),
        dependency("Right", "1.0.0", "lib/netstandard2.1/Shared.dll", duplicateRight)
      ],
      {
        schemaVersion: 1,
        assemblies: [
          identityRecord(duplicateLeft, "Left, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"),
          identityRecord(duplicateRight, "Right, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null")
        ]
      }
    ),
    /Managed dependency basename collision for Shared\.dll/
  );

  console.log("Unity candidate dependency resolver contract passed.");
} finally {
  removeTempRoot();
}
