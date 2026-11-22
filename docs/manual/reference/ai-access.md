---
title: AI Access & MCP
description: Consume DataVo v0.4 documentation in LLMs and coding assistants via llms.txt, per-page Markdown exports, and the local stdio MCP server.
---

# AI Access & MCP

DataVo publishes a curated, AI-readable Markdown export of the `v0.4.0-rc.1` manual alongside the VitePress site. Only public manual pages are exported — internal architecture notes, audits, and raw source directories are excluded.

## Static AI Endpoints

| Endpoint | Purpose |
| --- | --- |
| `/llms.txt` | Concise entry point listing all 38 public manual pages |
| `/llms-full.txt` | Complete public manual concatenated into a single Markdown file |
| `/ai/index.json` | Structured JSON index mapping routes to Markdown export paths |
| `/ai/pages/<route>.md` | Canonical Markdown export for an individual page (e.g., `/ai/pages/manual/getting-started/quickstart.md`) |
| `/mcp/datavo-docs.json` | Local stdio MCP server discovery config |
| `/mcp/remote-docs.json` | HTTP-retrievable page index for remote AI tools |

Every manual page also displays a **Copy page for AI** button at the top that copies the canonical Markdown export directly to your clipboard.

## Local Stdio MCP Server

GitHub Pages hosts the static Markdown and discovery JSON files, while local coding assistants can run the stdio MCP server directly from the `docs` directory:

```bash
cd docs
npm run docs:ai
npm run mcp
```

The MCP server restricts access strictly to the curated public manual:

```text
get_doc("/manual/getting-started/quickstart")  -> public Markdown
get_doc("architecture/index-manager.md")       -> Access Denied
```

::: details MCP Client Configuration Example
```json
{
  "mcpServers": {
    "datavo-docs": {
      "command": "node",
      "args": ["./mcp/datavo-docs-server.cjs"],
      "cwd": "./docs"
    }
  }
}
```
:::

## See Also

- [Introduction](/manual/getting-started/introduction) — Overview of DataVo v0.4.
- [Quickstart](/manual/getting-started/quickstart) — Get started in C# in 60 seconds.
