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

readonly roadmap_rows=(
  "Unity is an **evaluation target**, not a supported runtime."
  "| Unity 6000.5.4f1 Editor (macOS) | Narrow smoke pass |"
  "| macOS IL2CPP Player | Narrow smoke pass |"
  "| Windows x64 IL2CPP | Unverified |"
  "| Direct Burst calls | Unsupported by design |"
)

for row in "${roadmap_rows[@]}"; do
  require_text "$ROOT/docs/manual/getting-started/roadmap.md" "$row"
done
require_text \
  "$ROOT/docs/manual/getting-started/installation.md" \
  "| \`DataVo.Core\` | \`net8.0\`, \`net9.0\`, \`net10.0\`, \`netstandard2.1\` |"
require_text \
  "$ROOT/README.md" \
  "**Unity** is an evaluation target, not a supported runtime."
require_text \
  "$ROOT/README.md" \
  "Unity 6000.5.4f1 macOS Editor and a macOS IL2CPP Player. Windows IL2CPP is unverified and direct Burst calls are unsupported by design."

reject_text \
  "$ROOT/README.md" \
  "Use DataVo as a local gameplay/profile/state database."
reject_text \
  "$ROOT/README.md" \
  "Keep persistence and query behavior deterministic across development environments."
reject_text \
  "$ROOT/README.md" \
  "Reuse the same SQL surface in tools and runtime workflows."
# StorageMode.Disk was removed in 0.5.0; the UPM package README must not advertise it.
reject_text \
  "$ROOT/integrations/unity/com.datavo.core/README.md" \
  "StorageMode.Disk"
reject_text \
  "$ROOT/README.md" \
  "The planned first Unity proof covers in-memory execution in the Editor and a Windows x64 IL2CPP player."

echo "Unity support-status documentation check passed."
