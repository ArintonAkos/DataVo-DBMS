# Semantic Inventory React UI

This frontend is a React + Vite app with shadcn-style components, designed to consume the Semantic Inventory backend APIs.

## Run

1. Start backend first (from repo root):

```bash
./scripts/start-semantic-inventory.sh
```

2. In this frontend folder:

```bash
npm install
npm run dev
```

3. Open:

- `http://localhost:5173`

The backend serves on `http://localhost:5000`.

## API endpoints consumed

- `GET /api/catalog?category=`
- `GET /api/top-sellers?take=`
- `POST /api/search` with `{ query, topK }`
- `POST /api/order` with `{ itemId, qty }`

## Data scale

The backend seeds thousands of products by default. Adjust with:

```bash
Seed__ItemCount=5000 ./scripts/start-semantic-inventory.sh
```
