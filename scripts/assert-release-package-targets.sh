#!/usr/bin/env bash
set -euo pipefail

# Asserts the asset layout of all five publishable DataVo packages in one directory.
# Used by publish-nuget.yml (before pushing) and verify-local-packages.sh.
# Each package must exist exactly once and ship exactly the expected assembly paths, so a dropped or extra
# target framework fails the release instead of reaching NuGet.org.

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <directory containing the DataVo *.nupkg files>" >&2
  exit 2
fi

readonly packages_dir="$1"
readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

[[ -d "$packages_dir" ]] || {
  echo "Package directory does not exist: $packages_dir" >&2
  exit 2
}

# Prints the single <id>.<version>.nupkg in the package directory, or fails.
find_package() {
  local id="$1" found
  found="$(find "$packages_dir" -maxdepth 1 -type f -name "$id.[0-9]*.nupkg" -print)"
  if [[ -z "$found" || "$found" == *$'\n'* ]]; then
    echo "Expected exactly one $id nupkg in $packages_dir, found:" >&2
    printf '%s\n' "${found:-<none>}" >&2
    return 1
  fi
  printf '%s\n' "$found"
}

# Fails unless the package's entries for <file name> are exactly the expected paths.
assert_entries() {
  local package="$1" file_name="$2"
  shift 2
  local actual expected escaped
  escaped="$(printf '%s' "$file_name" | sed 's/\./\\./g')"
  actual="$(unzip -Z1 "$package" | grep -E "(^|/)${escaped}\$" | LC_ALL=C sort || true)"
  expected="$(printf '%s\n' "$@" | LC_ALL=C sort)"
  if [[ "$actual" != "$expected" ]]; then
    echo "$(basename "$package") has the wrong $file_name entries." >&2
    echo "Expected:" >&2
    printf '  %s\n' "$@" >&2
    echo "Found:" >&2
    printf '  %s\n' "${actual:-<none>}" >&2
    return 1
  fi
}

core="$(find_package DataVo.Core)"
bash "$script_dir/assert-core-package-targets.sh" "$core"

data="$(find_package DataVo.Data)"
assert_entries "$data" DataVo.Data.dll \
  lib/net8.0/DataVo.Data.dll \
  lib/net9.0/DataVo.Data.dll \
  lib/net10.0/DataVo.Data.dll \
  lib/netstandard2.1/DataVo.Data.dll
echo "Data package target assertion passed: net8.0, net9.0, net10.0, netstandard2.1."

vector_data="$(find_package DataVo.VectorData)"
assert_entries "$vector_data" DataVo.VectorData.dll \
  lib/net8.0/DataVo.VectorData.dll \
  lib/net9.0/DataVo.VectorData.dll \
  lib/net10.0/DataVo.VectorData.dll
echo "VectorData package target assertion passed: net8.0, net9.0, net10.0."

ef_core="$(find_package DataVo.EntityFrameworkCore)"
assert_entries "$ef_core" DataVo.EntityFrameworkCore.dll \
  lib/net10.0/DataVo.EntityFrameworkCore.dll
echo "EntityFrameworkCore package target assertion passed: net10.0."

generators="$(find_package DataVo.Generators)"
assert_entries "$generators" DataVo.Generators.dll \
  analyzers/dotnet/cs/DataVo.Generators.dll
assert_entries "$generators" DataVo.Generators.targets \
  build/DataVo.Generators.targets \
  buildTransitive/DataVo.Generators.targets
echo "Generators package layout assertion passed: analyzers/dotnet/cs + build/buildTransitive targets."

echo "All five release package layouts verified."
