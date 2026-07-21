#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
candidate_link="$root/demos/unity/DataVo.UnityArenaDemo/.generated"

if [[ -e "$candidate_link" && ! -L "$candidate_link" ]]; then
  echo "Unity arena candidate path exists but is not an owned symlink: $candidate_link" >&2
  exit 1
fi

candidate_root="$(mktemp -d /private/tmp/datavo-unity-arena-candidate.XXXXXX)"
candidate_output="$candidate_root/com.datavo.core.candidate"

bash "$root/scripts/check-unity-runtime-bcl.sh"
bash "$root/scripts/prepare-unity-candidate.sh" --output "$candidate_output"
ln -sfn "$candidate_output" "$candidate_link"

echo "Prepared Unity arena candidate: $candidate_output"
echo "Candidate manifest: $candidate_output/candidate-manifest.json"
