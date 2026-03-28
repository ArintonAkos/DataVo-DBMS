#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT_DIR"

echo "Running fast HNSW test lane (perf benchmarks disabled)..."
dotnet test "$ROOT_DIR/DataVo.Tests/DataVo.Tests.csproj" -c Release --filter "FullyQualifiedName~HNSWIndexTests"

echo "DONE: Fast HNSW test lane passed."
