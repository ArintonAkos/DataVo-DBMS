#!/usr/bin/env bash
set -euo pipefail

# Writes the 256-bit and 512-bit columnar kernel loops from the 128-bit ones. The three files differ only in
# the vector width, so the 128-bit file is the source: edit it, then run this script.

DIR="$(cd "$(dirname "$0")/.." && pwd)/DataVo.Core/Indexing/Columnar"

for width in 256 512; do
  sed -e "s/128/$width/g" \
      -e "s|^// The kernel loops at $width bits\..*|// The kernel loops at $width bits. Generated from ColumnarKernels.V128.cs by|" \
      -e "s|^// (scripts/generate-columnar-kernels.sh); edit this one and regenerate\.|// scripts/generate-columnar-kernels.sh; do not edit.|" \
      "$DIR/ColumnarKernels.V128.cs" > "$DIR/ColumnarKernels.V$width.cs"
done
