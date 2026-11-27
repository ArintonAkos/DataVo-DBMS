#!/usr/bin/env bash
set -euo pipefail

# Fails when the README or the user manual still shows removed public API surface:
#   - pre-0.4 sample symbols (ExecuteWithParams, DatabasePath);
#   - the paged Disk storage mode removed in 0.5.0 (StorageMode.Disk, DiskStorageEngine, UseDiskStorage);
#   - DataVoConfig.DiskStoragePath, renamed DataDirectory in 0.5.1.
# Run by deploy-docs.yml before the docs are built. Requires ripgrep (rg).

readonly pattern='ExecuteWithParams|DatabasePath|StorageMode\.Disk\b|DiskStorageEngine|UseDiskStorage|DiskStoragePath'
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ROOT
readonly paths=("$ROOT/README.md" "$ROOT/docs/manual")

set +e
matches="$(rg -n "$pattern" "${paths[@]}")"
status=$?
set -e

if [[ $status -eq 0 ]]; then
  printf '%s\n' "$matches"
  echo "Found removed public API symbols in README.md or docs/manual." >&2
  exit 1
fi

if [[ $status -gt 1 ]]; then
  printf '%s\n' "$matches" >&2
  exit "$status"
fi

echo "OK: README.md and docs/manual show no removed public API symbols."
