#!/usr/bin/env node
// Writes package.json and the .meta files Unity needs for a package installed read-only: without a .meta
// beside every asset, Unity ignores the asset. GUIDs derive from the path, so rebuilds keep the same ones.
import { createHash } from "node:crypto";
import { readdirSync, writeFileSync } from "node:fs";
import { join, posix } from "node:path";

const packageName = "com.datavo.core";

function argument(name) {
  const index = process.argv.indexOf(name);
  if (index < 0 || index + 1 >= process.argv.length) {
    throw new Error(`Missing required argument: ${name}`);
  }

  return process.argv[index + 1];
}

const guidFor = (relativePath) =>
  createHash("md5").update(`${packageName}/${relativePath}`).digest("hex");

const folderMeta = (guid) => `fileFormatVersion: 2
guid: ${guid}
folderAsset: yes
DefaultImporter:
  externalObjects: {}
  userData:
  assetBundleName:
  assetBundleVariant:
`;

const textMeta = (guid) => `fileFormatVersion: 2
guid: ${guid}
TextScriptImporter:
  externalObjects: {}
  userData:
  assetBundleName:
  assetBundleVariant:
`;

const pluginMeta = (guid) => `fileFormatVersion: 2
guid: ${guid}
PluginImporter:
  externalObjects: {}
  serializedVersion: 3
  iconMap: {}
  executionOrder: {}
  defineConstraints: []
  isPreloaded: 0
  isOverridable: 0
  isExplicitlyReferenced: 0
  validateReferences: 1
  platformData:
    Any:
      enabled: 1
      settings: {}
  userData:
  assetBundleName:
  assetBundleVariant:
`;

function writeMetas(packageRoot, relativeDirectory, seen) {
  const directory = join(packageRoot, relativeDirectory);
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    // Unity skips folders ending in "~", and a .meta is never an asset of its own.
    if (entry.name.endsWith("~") || entry.name.endsWith(".meta") || entry.name.startsWith(".")) {
      continue;
    }

    const relativePath = posix.join(relativeDirectory, entry.name);
    const guid = guidFor(relativePath);
    if (seen.has(guid)) {
      throw new Error(`GUID collision on ${relativePath}`);
    }

    seen.add(guid);
    const content = entry.isDirectory()
      ? folderMeta(guid)
      : entry.name.endsWith(".dll")
        ? pluginMeta(guid)
        : textMeta(guid);
    writeFileSync(join(packageRoot, `${relativePath}.meta`), content);
    if (entry.isDirectory()) {
      writeMetas(packageRoot, relativePath, seen);
    }
  }
}

const packageRoot = argument("--package");
const version = argument("--version");

const manifest = {
  name: packageName,
  displayName: "DataVo Core",
  version,
  unity: "6000.0",
  description:
    "Experimental preview of the DataVo embedded SQL engine for Unity.",
  license: "MIT",
  keywords: ["database", "sql", "embedded", "vector-search", "reactive"],
  samples: [
    {
      displayName: "Quick Start",
      description: "Creates an in-memory table and runs a prepared query.",
      path: "Samples~/QuickStart",
    },
  ],
};

writeFileSync(join(packageRoot, "package.json"), `${JSON.stringify(manifest, null, 2)}\n`);
writeMetas(packageRoot, "", new Set());
