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
portable_entries="$(
  printf '%s\n' "$entries" |
    sed -n '/netstandard2\.1/p'
)"
readonly portable_entries
core_dlls="$(
  printf '%s\n' "$entries" |
    sed -n '/^lib\/[^/]*\/DataVo\.Core\.dll$/p'
)"
readonly core_dlls

if [[ -n "$portable_entries" ]]; then
  echo "Public DataVo.Core package contains quarantined netstandard2.1 entries:" >&2
  printf '%s\n' "$portable_entries" >&2
  exit 1
fi

if [[ "$core_dlls" != "lib/net10.0/DataVo.Core.dll" ]]; then
  echo "Public DataVo.Core package must contain exactly lib/net10.0/DataVo.Core.dll." >&2
  echo "Found Core DLL entries:" >&2
  printf '%s\n' "${core_dlls:-<none>}" >&2
  exit 1
fi

echo "Core package target assertion passed: net10.0 only."
