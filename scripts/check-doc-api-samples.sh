#!/usr/bin/env bash
set -euo pipefail

readonly pattern='ExecuteWithParams|DatabasePath'
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ROOT
readonly paths=("$ROOT/README.md" "$ROOT/docs/features")

set +e
matches="$(rg -n "$pattern" "${paths[@]}")"
status=$?
set -e

if [[ $status -eq 0 ]]; then
  printf '%s\n' "$matches"
  echo "Found obsolete public API sample symbols in docs." >&2
  exit 1
fi

if [[ $status -gt 1 ]]; then
  printf '%s\n' "$matches" >&2
  exit "$status"
fi
