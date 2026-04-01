# Semantic Inventory EF Blazor App

This example is now a full EF Core + Blazor Server application with:

- EF Core model (`Items`, `Inventory`, `Sales`, `ItemEmbeddings`)
- LINQ queries for catalog, top-sellers, and semantic search shaping
- transactional order placement
- EF migrations and startup migration application
- startup seeding (`AppSeeder`)
- local Ollama embedding integration with deterministic fallback

## Layout

- `backend/`: full Blazor + EF Core app
- `backend/Migrations/`: generated EF migration files
- `seed/seed.sql`: optional SQL reference seed script from the initial prototype

## Pages

- `/` Dashboard: KPIs and top sellers
- `/catalog`: filterable catalog list
- `/semantic-search`: vector similarity ranking UI
- `/orders`: transactional order placement
- `/showcase`: embed item text and run load test

## Run

```bash
cd examples/semantic-inventory/backend
dotnet restore
dotnet build
dotnet run
```

## Run With Ollama (Recommended)

If Ollama is installed locally, use the helper script from the repo root:

```bash
./scripts/start-semantic-inventory.sh
```

This script:

- pulls the embedding model (`nomic-embed-text` by default)
- starts the app with Ollama embedding settings
- allows semantic text search in `/semantic-search`

Useful overrides:

```bash
OLLAMA_MODEL=mxbai-embed-large ./scripts/start-semantic-inventory.sh
USE_OLLAMA=false ./scripts/start-semantic-inventory.sh
```

Optional large seed size:

```bash
SEED_ITEM_COUNT=5000 ./scripts/start-semantic-inventory.sh
```

## React + shadcn-style Frontend

In a second terminal:

```bash
cd examples/semantic-inventory/frontend
npm install
npm run dev
```

Open `http://localhost:5173`.

The React UI talks to backend APIs (`/api/catalog`, `/api/top-sellers`, `/api/search`, `/api/order`).

Backend default URL is `http://127.0.0.1:5088` (set by `scripts/start-semantic-inventory.sh`).
If needed, override frontend API base URL:

```bash
VITE_API_BASE_URL=http://127.0.0.1:5088 npm run dev
```

## Migrations

Local EF tool is configured via `dotnet-tools.json` in `backend/`.

```bash
cd examples/semantic-inventory/backend
dotnet dotnet-ef migrations add <MigrationName>
dotnet dotnet-ef database update
```

On app startup, `Program.cs` applies `Database.Migrate()` and then runs `AppSeeder.SeedAsync()`.
After seeding, the app computes embeddings for all items (`EmbedAllItemsAsync`) so similarity search is immediately usable.
