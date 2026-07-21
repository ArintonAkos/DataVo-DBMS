#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

function fail(message) {
  console.error(message);
  process.exit(1);
}

function usage(message) {
  if (message) {
    console.error(message);
  }

  console.error(
    "Usage: resolve-unity-candidate-assets.mjs --assets <project.assets.json> --packages-root <directory> --runtime-directory <directory> --output <dependency-assets.json>",
  );
  process.exit(64);
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
        options[option] = readValue(args, index, option);
        index += 1;
        break;
      default:
        usage("Unknown option: " + option);
    }
  }

  for (const option of ["--assets", "--packages-root", "--runtime-directory", "--output"]) {
    if (!options[option]) {
      usage("Missing required " + option + ".");
    }
  }

  return {
    assetsPath: path.resolve(options["--assets"]),
    packagesRoot: path.resolve(options["--packages-root"]),
    runtimeDirectory: path.resolve(options["--runtime-directory"]),
    outputPath: path.resolve(options["--output"]),
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

function main() {
  const options = parseOptions(process.argv.slice(2));

  if (!fs.existsSync(options.assetsPath)) {
    fail("Assets file does not exist: " + options.assetsPath);
  }

  if (!fs.statSync(options.packagesRoot).isDirectory()) {
    fail("Packages root does not exist: " + options.packagesRoot);
  }

  if (fs.existsSync(options.runtimeDirectory)) {
    fail("Runtime directory must not already exist: " + options.runtimeDirectory);
  }

  const assets = JSON.parse(fs.readFileSync(options.assetsPath, "utf8"));
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
        packagePath: library.path,
      });
    }
  }

  dependencies.sort((left, right) =>
    left.packageId.localeCompare(right.packageId) ||
    left.packageVersion.localeCompare(right.packageVersion) ||
    left.assetPath.localeCompare(right.assetPath),
  );

  const destinations = new Map();
  for (const dependency of dependencies) {
    const basename = path.basename(dependency.assetPath);
    if (destinations.has(basename)) {
      fail(
        "Managed dependency basename collision for " +
          basename +
          ": " +
          destinations.get(basename).packageId +
          " and " +
          dependency.packageId +
          ".",
      );
    }

    destinations.set(basename, dependency);
  }

  fs.mkdirSync(options.runtimeDirectory, { recursive: false });

  const outputDependencies = dependencies.map((dependency) => {
    const sourcePath = path.join(options.packagesRoot, dependency.packagePath, dependency.assetPath);
    if (!fs.existsSync(sourcePath)) {
      fail("Resolved managed dependency does not exist: " + sourcePath);
    }

    const fixturePath = "Runtime/Dependencies/" + path.basename(dependency.assetPath);
    fs.copyFileSync(sourcePath, path.join(options.runtimeDirectory, path.basename(dependency.assetPath)));

    return {
      packageId: dependency.packageId,
      packageVersion: dependency.packageVersion,
      assetPath: dependency.assetPath.replaceAll(path.sep, "/"),
      fixturePath,
    };
  });

  fs.mkdirSync(path.dirname(options.outputPath), { recursive: true });
  fs.writeFileSync(
    options.outputPath,
    JSON.stringify({ schemaVersion: 1, dependencies: outputDependencies }, null, 2) + "\n",
  );
}

main();
