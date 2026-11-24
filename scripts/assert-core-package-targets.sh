#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <DataVo.Core.nupkg>" >&2
  exit 2
fi

readonly package_path="$1"

if [[ ! -f "$package_path" ]]; then
  echo "Core package does not exist: $package_path" >&2
  exit 2
fi

entries="$(unzip -Z1 "$package_path")"
readonly entries
core_dlls="$(
  printf '%s\n' "$entries" |
    sed -n '/^lib\/[^/]*\/DataVo\.Core\.dll$/p' |
    sort
)"
readonly core_dlls

expected="$(printf '%s\n' \
  lib/net10.0/DataVo.Core.dll \
  lib/net8.0/DataVo.Core.dll \
  lib/net9.0/DataVo.Core.dll \
  lib/netstandard2.1/DataVo.Core.dll | sort)"
readonly expected

if [[ "$core_dlls" != "$expected" ]]; then
  echo "Public DataVo.Core package must contain exactly the net8.0, net9.0, net10.0 and netstandard2.1 assets." >&2
  echo "Found Core DLL entries:" >&2
  printf '%s\n' "${core_dlls:-<none>}" >&2
  exit 1
fi

echo "Core package target assertion passed: net8.0, net9.0, net10.0, netstandard2.1."
