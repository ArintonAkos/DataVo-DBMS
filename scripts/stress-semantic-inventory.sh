#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
APP_DIR="$ROOT_DIR/examples/semantic-inventory/backend"

ITERATIONS="${1:-500}"

cd "$APP_DIR"

echo "[semantic-inventory] building"
dotnet build -v minimal

echo "[semantic-inventory] running app (start manually if needed)"
echo "Open /showcase and run stress with iterations=$ITERATIONS"
echo "Tip: use 'dotnet run' in this folder and then browse http://localhost:5000/showcase or https://localhost:5001/showcase"
