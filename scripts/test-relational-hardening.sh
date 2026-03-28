#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT_DIR"

echo "Running relational hardening lane (disk index concurrency)..."
dotnet test "$ROOT_DIR/DataVo.Tests/DataVo.Tests.csproj" -c Release --filter "FullyQualifiedName~DiskIndexConcurrencyTests"

echo "DONE: Relational hardening lane passed."
