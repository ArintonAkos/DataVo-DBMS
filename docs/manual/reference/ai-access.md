# AI Access And MCP

DataVo publishes an AI-readable version of the manual alongside the VitePress site. The export includes only curated public manual pages. It does not expose generated API pages, architecture notes, audit notes, or internal planning documents.

When the docs site is deployed, AI tools can start with `llms.txt`.

```text
https://<docs-host>/llms.txt
```

For tools that need the whole public manual in one file, use `llms-full.txt`.

```text
https://<docs-host>/llms-full.txt
```

For tools that prefer structured discovery, use the JSON index. Each entry points to the canonical Markdown export for a public manual page.

```text
https://<docs-host>/ai/index.json
```

A single page is exported as Markdown under `/ai/pages/...`.

```text
https://<docs-host>/ai/pages/manual/tutorial/quickstart.md
```

Every manual page also has a `Copy page for AI` button. It copies the canonical Markdown export, not the rendered HTML. That makes it suitable for pasting into an assistant, an issue, or a retrieval test.

For local MCP clients, clone the repository and run the stdio server from the `docs` directory.

```bash
cd docs
npm run docs:ai
npm run mcp
```

The MCP server exposes only public manual pages. If a client asks for an internal architecture file, generated API page, or audit document, the server returns an access-denied/not-found response.

```text
get_doc("/manual/tutorial/quickstart")
  -> public Markdown

get_doc("architecture/index-manager.md")
  -> Access Denied
```

GitHub Pages can host the static AI docs and MCP discovery files, but it cannot run the stdio MCP server process. Hosted discovery JSON is still useful because it tells tools where the public docs live and how the local MCP server is intended to be started.

```text
https://<docs-host>/mcp/datavo-docs.json
https://<docs-host>/mcp/remote-docs.json
```

## AI Access Support

| Feature | Status | Notes |
| --- | --- | --- |
| `/llms.txt` | Supported | Short AI entrypoint with links to curated manual pages. |
| `/llms-full.txt` | Supported | Concatenated Markdown export of the curated manual only. |
| `/ai/index.json` | Supported | Structured index of public manual pages. |
| `/ai/pages/<route>.md` | Supported | Canonical Markdown for one public manual page. |
| `Copy page for AI` | Supported | Copies canonical Markdown for the current page. |
| Local stdio MCP server | Supported | `npm run mcp` serves the curated manual to MCP clients. |
| Hosted MCP discovery JSON | Supported | Static config files are generated under `/mcp/`. |
| Internal docs export | Not Supported | Raw architecture, audit, generated API, and source folders are intentionally excluded. |
| Running MCP directly on GitHub Pages | Not Supported | Static hosts cannot run a stdio MCP process. |
