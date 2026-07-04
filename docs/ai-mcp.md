# AI Access And MCP

DataVo documentation is exported in AI-readable Markdown alongside the VitePress site. The export is useful for local assistants, retrieval systems, and MCP-compatible clients.

## Static AI Files

VitePress serves these files from the docs site:

| File | Purpose |
| --- | --- |
| `/llms.txt` | Short AI entrypoint with links to core pages. |
| `/llms-full.txt` | Full concatenated Markdown export. |
| `/ai/index.json` | Structured page index with routes and AI Markdown paths. |
| `/ai/pages/<route>.md` | Canonical Markdown for each documentation page. |

Each VitePress documentation page also has a `Copy page for AI` button. It copies the page's canonical Markdown export, not the rendered HTML.

## Local MCP Server

The repository includes a local stdio MCP server that reads the same AI export.

```bash
cd docs
npm run docs:ai
npm run mcp
```

The server exposes:

| Tool | Purpose |
| --- | --- |
| `list_docs` | Lists exported documentation pages. |
| `get_doc` | Fetches one page by route, source path, title, or `datavo-docs://page/...` URI. |
| `search_docs` | Runs simple case-insensitive text search over exported Markdown. |

It also exposes MCP resources:

| Resource | Purpose |
| --- | --- |
| `datavo-docs://index` | JSON index of exported docs. |
| `datavo-docs://page/<slug>` | Markdown for a specific page. |

Example MCP client configuration:

```json
{
  "mcpServers": {
    "datavo-docs": {
      "command": "node",
      "args": ["/absolute/path/to/DataVo-DBMS/docs/mcp/datavo-docs-server.cjs"],
      "cwd": "/absolute/path/to/DataVo-DBMS/docs"
    }
  }
}
```

Run `npm run docs:ai` after changing Markdown pages. The normal `docs:dev` and `docs:build` scripts run it automatically.
