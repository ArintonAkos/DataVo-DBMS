# DataVo Docs Remote MCP Worker

A zero-dependency Cloudflare Worker that exposes a live **Streamable HTTP MCP** endpoint (`https://<your-worker>.workers.dev/mcp`) backed by the static AI exports on GitHub Pages (`https://arintonakos.github.io/DataVo-DBMS/ai/index.json` and `/ai/pages/*.md`).

## Deploy (2 steps)

1. From this directory, deploy to your Cloudflare account (free tier is plenty):

   ```bash
   cd docs/mcp-worker
   npx wrangler deploy
   ```

2. Paste the printed URL + `/mcp` into Gemini's **Connect to an MCP server** dialog:

   ```text
   https://datavo-docs-mcp.<your-subdomain>.workers.dev/mcp
   ```

   Leave **Client ID** and **Client secret** blank.

Because the worker fetches `/ai/index.json` and `/ai/pages/*.md` from GitHub Pages with a 5-minute edge cache (`CACHE_TTL_SECONDS = "300"`), documentation updates pushed to `master` appear automatically without redeploying the worker.
