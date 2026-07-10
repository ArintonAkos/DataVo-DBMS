#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

require_text() {
  local file="$1"
  local expected="$2"

  if ! rg -Fq -- "$expected" "$file"; then
    echo "Missing required Unity status text in $file:" >&2
    echo "  $expected" >&2
    return 1
  fi
}

reject_text() {
  local file="$1"
  local forbidden="$2"

  if rg -Fq -- "$forbidden" "$file"; then
    echo "Found obsolete Unity support claim in $file:" >&2
    echo "  $forbidden" >&2
    return 1
  fi
}

readonly support_rows=(
  "| Unity Editor | Unverified until Stage 3 |"
  "| Windows x64 IL2CPP | Unverified until Stage 3 |"
  "| Direct Burst calls | Unsupported by design |"
  "| Job-to-managed batch bridge | Planned proof |"
  "| In-memory mode | Candidate scope |"
  "| Disk/LSM persistence | Unsupported until separately validated |"
)

for support_file in \
  "$ROOT/docs/manual/preface/alpha-scope.md" \
  "$ROOT/docs/features/unity-and-godot.md"; do
  for row in "${support_rows[@]}"; do
    require_text "$support_file" "$row"
  done
done

require_text \
  "$ROOT/README.md" \
  "Unity is an evaluation target, not a supported runtime."
require_text \
  "$ROOT/docs/features/setup-and-packaging.md" \
  "The public DataVo.Core package currently contains only a net10.0 asset."
require_text \
  "$ROOT/docs/features/roadmap-and-integrations.md" \
  "Unity is an evaluation target, not an available runtime integration."
require_text \
  "$ROOT/docs/features/index.md" \
  "### I want to evaluate future Unity or Godot support"

reject_text \
  "$ROOT/README.md" \
  "Use DataVo as a local gameplay/profile/state database."
reject_text \
  "$ROOT/README.md" \
  "Keep persistence and query behavior deterministic across development environments."
reject_text \
  "$ROOT/README.md" \
  "Reuse the same SQL surface in tools and runtime workflows."
reject_text \
  "$ROOT/docs/features/setup-and-packaging.md" \
  "Unity and Godot teams: use disk mode for persistent save/profile data"
reject_text \
  "$ROOT/docs/features/roadmap-and-integrations.md" \
  "DataVo is suitable for game-development local data workflows"
reject_text \
  "$ROOT/docs/features/index.md" \
  "### I want to ship to Unity or Godot"
reject_text \
  "$ROOT/docs/features/unity-and-godot.md" \
  "StorageMode = StorageMode.Disk"

echo "Unity support-status documentation check passed."
