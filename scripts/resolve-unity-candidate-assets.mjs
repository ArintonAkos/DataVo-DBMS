#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

function usage(message) {
  if (message) {
    console.error(message);
  }

  console.error(
    "Usage: resolve-unity-candidate-assets.mjs --assets <project.assets.json> --packages-root <directory> --runtime-directory <directory> --output <dependency-assets.json> --inspector <DataVo.UnityCandidateInspector.dll> --unity-runtime-inventory <UnityRuntimeBclManifest.json> [--unity-runtime-inventory-reference <tracked-path>]",
  );
  process.exit(64);
}

function fail(message) {
  throw new Error(message);
}

function readValue(args, index, option) {
  if (index + 1 >= args.length || args[index + 1].startsWith("--")) {
    usage("Missing value for " + option + ".");
  }

  return args[index + 1];
}

function parseOptions(args) {
  const options = {};

  for (let index = 0; index < args.length; index += 1) {
    const option = args[index];
    switch (option) {
      case "--assets":
      case "--packages-root":
      case "--runtime-directory":
      case "--output":
      case "--inspector":
      case "--unity-runtime-inventory":
      case "--unity-runtime-inventory-reference":
        options[option] = readValue(args, index, option);
        index += 1;
        break;
      default:
        usage("Unknown option: " + option);
    }
  }

  const requiredOptions = [
    "--assets",
    "--packages-root",
    "--runtime-directory",
    "--output",
    "--inspector",
    "--unity-runtime-inventory",
  ];
  for (const option of requiredOptions) {
    if (!options[option]) {
      usage("Missing required " + option + ".");
    }
  }

  return {
    assetsPath: path.resolve(options["--assets"]),
    packagesRoot: path.resolve(options["--packages-root"]),
    runtimeDirectory: path.resolve(options["--runtime-directory"]),
    outputPath: path.resolve(options["--output"]),
    inspectorPath: path.resolve(options["--inspector"]),
    unityRuntimeInventoryPath: path.resolve(options["--unity-runtime-inventory"]),
    unityRuntimeInventoryReference: options["--unity-runtime-inventory-reference"] ?? path.resolve(options["--unity-runtime-inventory"]),
  };
}

function packageKeyParts(packageKey) {
  const separator = packageKey.lastIndexOf("/");
  if (separator <= 0 || separator === packageKey.length - 1) {
    fail("Invalid NuGet library key in assets file: " + packageKey);
  }

  return {
    packageId: packageKey.slice(0, separator),
    packageVersion: packageKey.slice(separator + 1),
  };
}

function selectedAssets(targetLibrary) {
  const runtime = Object.keys(targetLibrary.runtime ?? {}).filter((asset) => asset.toLowerCase().endsWith(".dll"));
  if (runtime.length > 0) {
    return runtime;
  }

  return Object.keys(targetLibrary.compile ?? {}).filter((asset) => asset.toLowerCase().endsWith(".dll"));
}

function resolveTarget(assets) {
  const targetName = Object.keys(assets.targets ?? {}).find((name) =>
    name.startsWith(".NETStandard,Version=v2.1"),
  );

  if (!targetName) {
    fail("The restored assets file does not contain a netstandard2.1 target.");
  }

  return assets.targets[targetName];
}

function resolveDependenciesFromAssets(assets, packagesRoot) {
  const target = resolveTarget(assets);
  const dependencies = [];

  for (const [libraryKey, targetLibrary] of Object.entries(target)) {
    const { packageId, packageVersion } = packageKeyParts(libraryKey);
    if (packageId === "DataVo.Core") {
      continue;
    }

    const library = assets.libraries?.[libraryKey];
    if (!library?.path) {
      fail("Assets file does not map " + libraryKey + " to a package path.");
    }

    for (const assetPath of selectedAssets(targetLibrary)) {
      dependencies.push({
        packageId,
        packageVersion,
        assetPath,
        sourcePath: path.join(packagesRoot, library.path, assetPath),
      });
    }
  }

  return dependencies;
}

function compareDependencies(left, right) {
  return left.packageId.localeCompare(right.packageId) ||
    left.packageVersion.localeCompare(right.packageVersion) ||
    left.assetPath.localeCompare(right.assetPath);
}

function validateUnityRuntimeInventory(inventory) {
  if (inventory?.schemaVersion !== 1 || !Array.isArray(inventory.assemblies)) {
    fail("Unity runtime inventory must have schemaVersion 1 and an assemblies array.");
  }

  const byFileName = new Map();
  for (const assembly of inventory.assemblies) {
    if (!assembly || typeof assembly !== "object") {
      fail("Unity runtime inventory contains an invalid assembly record.");
    }

    if (typeof assembly.fileName !== "string" || path.basename(assembly.fileName) !== assembly.fileName) {
      fail("Unity runtime inventory assembly fileName must be a basename.");
    }

    if (typeof assembly.runtimePath !== "string" || assembly.runtimePath.length === 0) {
      fail("Unity runtime inventory runtimePath is missing for " + assembly.fileName + ".");
    }

    if (typeof assembly.identity !== "string" || assembly.identity.length === 0) {
      fail("Unity runtime inventory identity is missing for " + assembly.fileName + ".");
    }

    if (byFileName.has(assembly.fileName)) {
      fail("Unity runtime inventory has duplicate fileName: " + assembly.fileName);
    }

    byFileName.set(assembly.fileName, assembly);
  }

  return byFileName;
}

function validateIdentityInventory(identityInventory, dependencies) {
  if (identityInventory?.schemaVersion !== 1 || !Array.isArray(identityInventory.assemblies)) {
    fail("Dependency identity inventory must have schemaVersion 1 and an assemblies array.");
  }

  const expectedSources = new Map();
  for (const dependency of dependencies) {
    const sourcePath = path.resolve(dependency.sourcePath);
    if (expectedSources.has(sourcePath)) {
      fail("Resolved managed dependency appears more than once: " + sourcePath);
    }

    if (!fs.statSync(sourcePath, { throwIfNoEntry: false })?.isFile()) {
      fail("Resolved managed dependency does not exist: " + sourcePath);
    }

    expectedSources.set(sourcePath, dependency);
  }

  const records = new Map();
  for (const assembly of identityInventory.assemblies) {
    if (!assembly || typeof assembly !== "object" || typeof assembly.sourcePath !== "string") {
      fail("Dependency identity inventory contains an invalid assembly record.");
    }

    const sourcePath = path.resolve(assembly.sourcePath);
    if (records.has(sourcePath)) {
      fail("Dependency identity inventory has duplicate sourcePath: " + sourcePath);
    }

    if (typeof assembly.fileName !== "string" || assembly.fileName !== path.basename(sourcePath)) {
      fail("Dependency identity inventory has an invalid fileName for " + sourcePath + ".");
    }

    if (typeof assembly.identity !== "string" || assembly.identity.length === 0) {
      fail("Dependency identity inventory has no identity for " + sourcePath + ".");
    }

    records.set(sourcePath, assembly);
  }

  if (records.size !== expectedSources.size) {
    fail("Dependency identity inventory does not match the resolved managed dependency closure.");
  }

  for (const sourcePath of expectedSources.keys()) {
    if (!records.has(sourcePath)) {
      fail("Dependency identity inventory is missing: " + sourcePath);
    }
  }

  for (const sourcePath of records.keys()) {
    if (!expectedSources.has(sourcePath)) {
      fail("Dependency identity inventory contains an unexpected assembly: " + sourcePath);
    }
  }

  return records;
}

function normalizedAssetPath(assetPath) {
  return assetPath.split(path.sep).join("/");
}

export function resolveCandidateAssets({
  dependencies,
  identityInventory,
  unityRuntimeInventory,
  unityRuntimeInventoryReference,
  runtimeDirectory,
  outputPath,
}) {
  if (!Array.isArray(dependencies)) {
    fail("Resolved managed dependencies must be an array.");
  }

  if (typeof unityRuntimeInventoryReference !== "string" || unityRuntimeInventoryReference.length === 0) {
    fail("Unity runtime inventory reference is required.");
  }

  const runtimeDirectoryPath = path.resolve(runtimeDirectory);
  const outputPathValue = path.resolve(outputPath);
  if (fs.existsSync(runtimeDirectoryPath)) {
    fail("Runtime directory must not already exist: " + runtimeDirectoryPath);
  }

  const sortedDependencies = dependencies
    .map((dependency) => ({ ...dependency, sourcePath: path.resolve(dependency.sourcePath) }))
    .sort(compareDependencies);
  const destinations = new Map();

  for (const dependency of sortedDependencies) {
    if (typeof dependency.packageId !== "string" || dependency.packageId.length === 0 ||
      typeof dependency.packageVersion !== "string" || dependency.packageVersion.length === 0 ||
      typeof dependency.assetPath !== "string" || !dependency.assetPath.toLowerCase().endsWith(".dll")) {
      fail("Resolved managed dependency has incomplete provenance.");
    }

    const fileName = path.basename(dependency.assetPath);
    if (destinations.has(fileName)) {
      const previous = destinations.get(fileName);
      fail(
        "Managed dependency basename collision for " + fileName + ": " +
        previous.packageId + " and " + dependency.packageId + ".",
      );
    }

    destinations.set(fileName, dependency);
  }

  const identityBySource = validateIdentityInventory(identityInventory, sortedDependencies);
  const unityByFileName = validateUnityRuntimeInventory(unityRuntimeInventory);
  const resolved = [];

  for (const dependency of sortedDependencies) {
    const identityRecord = identityBySource.get(dependency.sourcePath);
    const fileName = path.basename(dependency.assetPath);
    const unityProvider = unityByFileName.get(fileName);
    const baseRecord = {
      packageId: dependency.packageId,
      packageVersion: dependency.packageVersion,
      assetPath: normalizedAssetPath(dependency.assetPath),
      identity: identityRecord.identity,
    };

    if (!unityProvider) {
      resolved.push({
        ...baseRecord,
        resolution: "copied",
        fixturePath: "Runtime/Dependencies/" + fileName,
        sourcePath: dependency.sourcePath,
      });
      continue;
    }

    if (identityRecord.identity !== unityProvider.identity) {
      fail(
        "Unity runtime assembly identity conflict for " + fileName + ": NuGet " +
        dependency.packageId + " " + dependency.packageVersion + " (" + dependency.assetPath +
        ") declares '" + identityRecord.identity + "', but Unity runtime " +
        unityProvider.runtimePath + " declares '" + unityProvider.identity + "'.",
      );
    }

    resolved.push({
      ...baseRecord,
      resolution: "unity-provided",
      unityRuntimePath: unityProvider.runtimePath,
      unityRuntimeInventory: unityRuntimeInventoryReference,
    });
  }

  fs.mkdirSync(runtimeDirectoryPath, { recursive: true });
  for (const dependency of resolved) {
    if (dependency.resolution === "copied") {
      fs.copyFileSync(dependency.sourcePath, path.join(runtimeDirectoryPath, path.basename(dependency.fixturePath)));
    }
  }

  const outputDependencies = resolved.map(({ sourcePath, ...dependency }) => dependency);
  fs.mkdirSync(path.dirname(outputPathValue), { recursive: true });
  fs.writeFileSync(
    outputPathValue,
    JSON.stringify({ schemaVersion: 2, dependencies: outputDependencies }, null, 2) + "\n",
  );

  return { schemaVersion: 2, dependencies: outputDependencies };
}

function inspectDependencies(inspectorPath, dependencies) {
  if (!fs.statSync(inspectorPath, { throwIfNoEntry: false })?.isFile()) {
    fail("Candidate identity inspector does not exist: " + inspectorPath);
  }

  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "datavo-unity-identity-"));
  const outputPath = path.join(tempRoot, "dependency-identities.json");
  try {
    execFileSync(
      "dotnet",
      [inspectorPath, "--identity-output", outputPath, ...dependencies.map((dependency) => dependency.sourcePath)],
      { stdio: ["ignore", "inherit", "inherit"] },
    );
    return JSON.parse(fs.readFileSync(outputPath, "utf8"));
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
}

function main() {
  const options = parseOptions(process.argv.slice(2));

  if (!fs.statSync(options.assetsPath, { throwIfNoEntry: false })?.isFile()) {
    fail("Assets file does not exist: " + options.assetsPath);
  }

  if (!fs.statSync(options.packagesRoot, { throwIfNoEntry: false })?.isDirectory()) {
    fail("Packages root does not exist: " + options.packagesRoot);
  }

  if (!fs.statSync(options.unityRuntimeInventoryPath, { throwIfNoEntry: false })?.isFile()) {
    fail("Unity runtime inventory does not exist: " + options.unityRuntimeInventoryPath);
  }

  const assets = JSON.parse(fs.readFileSync(options.assetsPath, "utf8"));
  const dependencies = resolveDependenciesFromAssets(assets, options.packagesRoot);
  const identityInventory = inspectDependencies(options.inspectorPath, dependencies);
  const unityRuntimeInventory = JSON.parse(fs.readFileSync(options.unityRuntimeInventoryPath, "utf8"));

  resolveCandidateAssets({
    dependencies,
    identityInventory,
    unityRuntimeInventory,
    unityRuntimeInventoryReference: options.unityRuntimeInventoryReference,
    runtimeDirectory: options.runtimeDirectory,
    outputPath: options.outputPath,
  });
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}
