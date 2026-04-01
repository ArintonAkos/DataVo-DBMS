#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
APP_DIR="$ROOT_DIR/examples/semantic-inventory/backend"
MODEL="${OLLAMA_MODEL:-nomic-embed-text}"
OLLAMA_URL="${OLLAMA_BASE_URL:-http://127.0.0.1:11434}"
USE_OLLAMA="${USE_OLLAMA:-true}"
SEED_ITEM_COUNT="${SEED_ITEM_COUNT:-2500}"
ASPNETCORE_ENVIRONMENT="${ASPNETCORE_ENVIRONMENT:-Development}"
ASPNETCORE_URLS="${ASPNETCORE_URLS:-http://127.0.0.1:5088}"

if [[ "$USE_OLLAMA" == "true" ]]; then
  if ! command -v ollama >/dev/null 2>&1; then
    echo "[semantic-inventory] ollama CLI not found. Install Ollama or run with USE_OLLAMA=false"
    exit 1
  fi

  echo "[semantic-inventory] ensuring model '$MODEL' is available"
  ollama pull "$MODEL"
fi

cd "$APP_DIR"

echo "[semantic-inventory] restoring and building"
dotnet restore
dotnet build -v minimal

echo "[semantic-inventory] starting app with embeddings"
echo "  Embeddings__UseOllama=$USE_OLLAMA"
echo "  Embeddings__OllamaBaseUrl=$OLLAMA_URL"
echo "  Embeddings__OllamaModel=$MODEL"
echo "  Seed__ItemCount=$SEED_ITEM_COUNT"
echo "  ASPNETCORE_ENVIRONMENT=$ASPNETCORE_ENVIRONMENT"
echo "  ASPNETCORE_URLS=$ASPNETCORE_URLS"
echo "[semantic-inventory] open: $ASPNETCORE_URLS"

ASPNETCORE_ENVIRONMENT="$ASPNETCORE_ENVIRONMENT" \
ASPNETCORE_URLS="$ASPNETCORE_URLS" \
Embeddings__UseOllama="$USE_OLLAMA" \
Embeddings__OllamaBaseUrl="$OLLAMA_URL" \
Embeddings__OllamaModel="$MODEL" \
Seed__ItemCount="$SEED_ITEM_COUNT" \
dotnet run
