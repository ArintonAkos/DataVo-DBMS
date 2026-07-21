#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
output_input=""

usage() {
  echo "Usage: $0 --output <new-fixture-directory>" >&2
  exit 64
}

while (($# > 0)); do
  case "$1" in
    --output)
      (($# >= 2)) || usage
      [[ -z "$output_input" ]] || usage
      output_input="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

[[ -n "$output_input" ]] || usage
[[ ! -e "$output_input" ]] || {
  echo "Unity candidate output already exists: $output_input" >&2
  exit 1
}

output_parent="$(dirname "$output_input")"
output_name="$(basename "$output_input")"
mkdir -p "$output_parent"
output_parent="$(cd "$output_parent" && pwd -P)"
output_path="$output_parent/$output_name"
[[ ! -e "$output_path" ]] || {
  echo "Unity candidate output already exists: $output_path" >&2
  exit 1
}

temp_root="$(mktemp -d)"
stage="$(mktemp -d "$output_parent/.${output_name}.staging.XXXXXX")"
cleanup() {
  rm -rf "$temp_root" "$stage"
}
trap cleanup EXIT HUP INT TERM

source_commit="$(git -C "$root" rev-parse HEAD)"
package_version="0.0.0-unitycandidate.${source_commit:0:12}"

packages_dir="$temp_root/packages"
pack_cache="$temp_root/pack-packages"
pack_extensions="$temp_root/pack-extensions/"
consumer_cache="$temp_root/consumer-packages"
consumer_http_cache="$temp_root/consumer-http-cache"
consumer_extensions="$temp_root/consumer-extensions/"
tool_cache="$temp_root/tool-packages"
tool_extensions="$temp_root/tool-extensions/"
dotnet_home="$temp_root/dotnet-home"
export DOTNET_CLI_HOME="$dotnet_home"
unset NUGET_FALLBACK_PACKAGES || true

mkdir -p "$packages_dir" "$pack_cache" "$pack_extensions" "$consumer_cache" \
  "$consumer_http_cache" "$consumer_extensions" "$tool_cache" "$tool_extensions" \
  "$dotnet_home" "$stage/Runtime"

NUGET_PACKAGES="$pack_cache" dotnet restore "$root/DataVo.Core/DataVo.Core.csproj" \
  -p:DataVoEnablePortableTarget=true \
  -p:MSBuildProjectExtensionsPath="$pack_extensions" \
  --force --no-cache

NUGET_PACKAGES="$pack_cache" dotnet pack "$root/DataVo.Core/DataVo.Core.csproj" \
  -c Release \
  --no-restore \
  -p:DataVoEnablePortableTarget=true \
  -p:EnablePackageValidation=true \
  -p:MSBuildProjectExtensionsPath="$pack_extensions" \
  -p:PackageVersion="$package_version" \
  -o "$packages_dir"

core_package="$packages_dir/DataVo.Core.$package_version.nupkg"
[[ -f "$core_package" ]] || {
  echo "Missing candidate package: $core_package" >&2
  exit 1
}

entries="$(unzip -Z1 "$core_package")"
core_dlls="$(printf '%s\n' "$entries" | awk '$0 ~ /(^|\/)DataVo\.Core\.dll$/ { print }' | LC_ALL=C sort)"
expected_dlls=$'lib/net10.0/DataVo.Core.dll\nlib/netstandard2.1/DataVo.Core.dll'
if [[ "$core_dlls" != "$expected_dlls" ]]; then
  echo "Candidate package has the wrong DataVo.Core DLL entries:" >&2
  printf '%s\n' "${core_dlls:-<none>}" >&2
  exit 1
fi

core_dll="$stage/Runtime/DataVo.Core.dll"
unzip -p "$core_package" "lib/netstandard2.1/DataVo.Core.dll" > "$core_dll"
[[ -s "$core_dll" ]] || {
  echo "Extracted portable DataVo.Core.dll is empty." >&2
  exit 1
}

NUGET_PACKAGES="$consumer_cache" NUGET_HTTP_CACHE_PATH="$consumer_http_cache" \
dotnet restore "$root/tools/DataVo.PortableConsumer/DataVo.PortableConsumer.csproj" \
  --source "$packages_dir" \
  --source "https://api.nuget.org/v3/index.json" \
  --force --no-cache \
  -p:MSBuildProjectExtensionsPath="$consumer_extensions" \
  -p:DataVoCandidateVersion="$package_version"

NUGET_PACKAGES="$consumer_cache" NUGET_HTTP_CACHE_PATH="$consumer_http_cache" \
dotnet build "$root/tools/DataVo.PortableConsumer/DataVo.PortableConsumer.csproj" \
  -c Release \
  --no-restore \
  -p:MSBuildProjectExtensionsPath="$consumer_extensions" \
  -p:DataVoCandidateVersion="$package_version"

consumer_assets="${consumer_extensions}project.assets.json"
node - "$consumer_assets" "$package_version" <<'NODE'
const fs = require("node:fs");

const [assetsPath, packageVersion] = process.argv.slice(2);
const assets = JSON.parse(fs.readFileSync(assetsPath, "utf8"));
const targetName = Object.keys(assets.targets ?? {}).find((name) =>
  name.startsWith(".NETStandard,Version=v2.1"),
);
if (!targetName) {
  console.error("Portable consumer restore did not create a netstandard2.1 target.");
  process.exit(1);
}

const core = assets.targets[targetName]["DataVo.Core/" + packageVersion];
if (!core || !Object.prototype.hasOwnProperty.call(core.compile ?? {}, "lib/netstandard2.1/DataVo.Core.dll")) {
  console.error("Portable consumer did not select lib/netstandard2.1/DataVo.Core.dll.");
  process.exit(1);
}
NODE

cached_package="$consumer_cache/datavo.core/$package_version/datavo.core.$package_version.nupkg"
cmp -s "$core_package" "$cached_package" || {
  echo "Consumer cache does not contain the exact packed candidate nupkg." >&2
  exit 1
}

node "$root/scripts/resolve-unity-candidate-assets.mjs" \
  --assets "$consumer_assets" \
  --packages-root "$consumer_cache" \
  --runtime-directory "$stage/Runtime/Dependencies" \
  --output "$stage/dependency-assets.json"

NUGET_PACKAGES="$tool_cache" dotnet restore "$root/tools/DataVo.UnityCandidateInspector/DataVo.UnityCandidateInspector.csproj" \
  -p:MSBuildProjectExtensionsPath="$tool_extensions" \
  --force --no-cache

NUGET_PACKAGES="$tool_cache" dotnet build "$root/tools/DataVo.UnityCandidateInspector/DataVo.UnityCandidateInspector.csproj" \
  --no-restore \
  --disable-build-servers \
  -p:MSBuildProjectExtensionsPath="$tool_extensions"

inspector="$root/tools/DataVo.UnityCandidateInspector/bin/Debug/net10.0/DataVo.UnityCandidateInspector.dll"
[[ -f "$inspector" ]] || {
  echo "Missing candidate inspector output: $inspector" >&2
  exit 1
}

assemblies=("$core_dll")
while IFS= read -r dependency; do
  assemblies+=("$dependency")
done < <(find "$stage/Runtime/Dependencies" -type f -name "*.dll" | LC_ALL=C sort)

dotnet "$inspector" \
  --root "$stage" \
  --output "$stage/assembly-inventory.json" \
  "${assemblies[@]}"

package_sha256="$(shasum -a 256 "$core_package" | awk '{ print $1 }')"
node - "$stage" "$source_commit" "$package_version" "$package_sha256" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");

const [stage, sourceCommit, packageVersion, packageSha256] = process.argv.slice(2);
const dependencies = JSON.parse(fs.readFileSync(path.join(stage, "dependency-assets.json"), "utf8"));
const inventory = JSON.parse(fs.readFileSync(path.join(stage, "assembly-inventory.json"), "utf8"));
const dependencyByFixturePath = new Map(
  dependencies.dependencies.map((dependency) => [dependency.fixturePath, dependency]),
);

const assemblies = inventory.assemblies.map((assembly) => {
  if (assembly.file === "Runtime/DataVo.Core.dll") {
    return {
      ...assembly,
      source: {
        packageId: "DataVo.Core",
        packageVersion,
        assetPath: "lib/netstandard2.1/DataVo.Core.dll",
      },
    };
  }

  const dependency = dependencyByFixturePath.get(assembly.file);
  if (!dependency) {
    throw new Error("No dependency provenance for " + assembly.file);
  }

  return {
    ...assembly,
    source: {
      packageId: dependency.packageId,
      packageVersion: dependency.packageVersion,
      assetPath: dependency.assetPath,
    },
  };
});

const packageJson = {
  name: "com.datavo.core.candidate",
  displayName: "DataVo Core Candidate (Generated)",
  version: "0.0.0-preview.1",
  unity: "6000.0",
  description: "Generated test-only fixture from the exact experimental DataVo.Core portable package.",
};

const candidateManifest = {
  schemaVersion: 1,
  sourceCommit,
  fixturePackage: {
    name: packageJson.name,
    version: packageJson.version,
  },
  corePackage: {
    id: "DataVo.Core",
    version: packageVersion,
    sha256: packageSha256,
    assetPath: "lib/netstandard2.1/DataVo.Core.dll",
  },
  dependencyAssets: "dependency-assets.json",
  assemblyInventory: "assembly-inventory.json",
  assemblies,
};

fs.writeFileSync(path.join(stage, "package.json"), JSON.stringify(packageJson, null, 2) + "\n");
fs.writeFileSync(path.join(stage, "candidate-manifest.json"), JSON.stringify(candidateManifest, null, 2) + "\n");
NODE

printf '%s\n' \
  "# DataVo Core Candidate Fixture" \
  "" \
  "This directory is generated test material from an exact experimental DataVo.Core portable package." \
  "Do not commit it, publish it, or interpret it as Unity support evidence." \
  "The adjacent candidate-manifest.json records the source commit, package hash, assembly identities, and dependency provenance." \
  > "$stage/README.md"

for required in \
  "$stage/package.json" \
  "$stage/README.md" \
  "$stage/candidate-manifest.json" \
  "$stage/dependency-assets.json" \
  "$stage/assembly-inventory.json" \
  "$stage/Runtime/DataVo.Core.dll" \
  "$stage/Runtime/Dependencies"; do
  [[ -e "$required" ]] || {
    echo "Candidate fixture is missing required output: $required" >&2
    exit 1
  }
done

mv "$stage" "$output_path"
stage=""
echo "Prepared Unity candidate fixture: $output_path"
