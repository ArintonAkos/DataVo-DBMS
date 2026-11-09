#!/usr/bin/env bash
# Builds the installable com.datavo.core UPM package from the verified portable candidate.
# Output: <output>/com.datavo.core (expanded) and <output>/com.datavo.core-<version>.tgz.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
output=""
version="0.2.0-rc.1"

usage() {
  echo "Usage: $0 --output <new-directory> [--version <semver>]" >&2
  exit 64
}

while (($# > 0)); do
  case "$1" in
    --output) (($# >= 2)) || usage; output="$2"; shift 2 ;;
    --version) (($# >= 2)) || usage; version="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -n "$output" ]] || usage
[[ ! -e "$output" ]] || { echo "Output already exists: $output" >&2; exit 1; }
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || { echo "Invalid UPM version: $version" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT HUP INT TERM

bash "$root/scripts/prepare-unity-candidate.sh" --output "$work/candidate"

package="$work/package"
mkdir -p "$package/Runtime/Plugins/Dependencies" "$package/Analyzers" "$package/Documentation~" "$package/Samples~/QuickStart"

cp "$work/candidate/Runtime/DataVo.Core.dll" "$package/Runtime/Plugins/"
dotnet build "$root/DataVo.Generators/DataVo.Generators.csproj" -c Release -v q --nologo >/dev/null
cp "$root/DataVo.Generators/bin/Release/netstandard2.0/DataVo.Generators.dll" "$package/Analyzers/"
find "$work/candidate/Runtime/Dependencies" -type f -name '*.dll' -exec cp {} "$package/Runtime/Plugins/Dependencies/" \;
cp "$work/candidate/candidate-manifest.json" "$work/candidate/dependency-assets.json" \
  "$work/candidate/assembly-inventory.json" "$package/Documentation~/"
cp "$root/LICENSE" "$package/LICENSE.md"
cp "$root/integrations/unity/com.datavo.core/README.md" \
  "$root/integrations/unity/com.datavo.core/CHANGELOG.md" \
  "$root/integrations/unity/com.datavo.core/Third Party Notices.md" "$package/"
cp "$root/integrations/unity/com.datavo.core/Samples~/QuickStart/"* "$package/Samples~/QuickStart/"

node "$root/scripts/write-unity-upm-metadata.mjs" --package "$package" --version "$version"

mkdir -p "$(dirname "$output")"
output_final="$(cd "$(dirname "$output")" && pwd -P)/$(basename "$output")"
mkdir "$work/out"
cp -R "$package" "$work/out/com.datavo.core"
COPYFILE_DISABLE=1 tar -C "$work" -czf "$work/out/com.datavo.core-$version.tgz" package
mv "$work/out" "$output_final"

echo "Unity package: $output_final/com.datavo.core-$version.tgz"
