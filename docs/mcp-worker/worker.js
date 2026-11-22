/**
 * Cloudflare Worker exposing a remote Streamable HTTP MCP server (/mcp)
 * backed by the static AI exports hosted on GitHub Pages.
 *
 * Exposes the exact same read-only surface as docs/mcp/datavo-docs-server.cjs:
 *   Tools:     list_docs, get_doc, search_docs
 *   Resources: datavo-docs://index, datavo-docs://page/<route>
 */

const SERVER_INFO = { name: "datavo-docs", version: "0.1.0" };
const SUPPORTED_PROTOCOL_VERSION = "2024-11-05";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Accept, Mcp-Session-Id",
};

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...CORS_HEADERS,
    },
  });
}

function rpcResult(id, result) {
  return { jsonrpc: "2.0", id, result };
}

function rpcError(id, code, message, data) {
  return {
    jsonrpc: "2.0",
    id,
    error: data === undefined ? { code, message } : { code, message, data },
  };
}

function textContent(text, isError = false) {
  return {
    content: [{ type: "text", text }],
    ...(isError ? { isError: true } : {}),
  };
}

async function fetchCachedText(url, ttlSeconds) {
  const res = await fetch(url, {
    cf: { cacheTtl: ttlSeconds, cacheEverything: true },
  });
  if (!res.ok) {
    throw new Error(`Upstream fetch failed (${res.status}) for ${url}`);
  }
  return res.text();
}

async function loadIndex(env) {
  const base = (env.DOCS_BASE_URL || "https://arintonakos.github.io/DataVo-DBMS").replace(/\/+$/u, "");
  const ttl = Number(env.CACHE_TTL_SECONDS || 300);
  const raw = await fetchCachedText(`${base}/ai/index.json`, ttl);
  return { base, ttl, index: JSON.parse(raw) };
}

function uriForPage(page) {
  const slug = page.route.replace(/^\/+/u, "") || "index";
  return `datavo-docs://page/${slug}`;
}

function findPage(pages, identifier) {
  const normalized = String(identifier || "").trim().replace(/^datavo-docs:\/\/page\//u, "");
  const withLeadingSlash = normalized.startsWith("/") ? normalized : `/${normalized}`;
  const lower = normalized.toLowerCase();

  return (
    pages.find(
      (p) =>
        p.route === withLeadingSlash ||
        p.route.replace(/^\/+/u, "") === normalized ||
        p.source.toLowerCase() === lower ||
        p.title.toLowerCase() === lower
    ) || null
  );
}

function toolsList() {
  return [
    {
      name: "list_docs",
      description: "List DataVo documentation pages and engineering blog posts available as AI-readable Markdown.",
      inputSchema: { type: "object", properties: {}, additionalProperties: false },
    },
    {
      name: "get_doc",
      description: "Fetch one DataVo documentation page or blog post by route, source path, title, or datavo-docs URI.",
      inputSchema: {
        type: "object",
        properties: {
          path: {
            type: "string",
            description: "Route such as /manual/getting-started/quickstart or /blog/part-1-where-it-wins-and-where-it-loses.",
          },
        },
        required: ["path"],
        additionalProperties: false,
      },
    },
    {
      name: "search_docs",
      description: "Search DataVo documentation and blog Markdown with a case-insensitive text query.",
      inputSchema: {
        type: "object",
        properties: {
          query: { type: "string", description: "Search text." },
          limit: { type: "integer", minimum: 1, maximum: 20, default: 8 },
        },
        required: ["query"],
        additionalProperties: false,
      },
    },
  ];
}

async function callTool(params, env) {
  const name = params?.name;
  const args = params?.arguments ?? {};
  const { base, ttl, index } = await loadIndex(env);
  const pages = index.pages || [];

  if (name === "list_docs") {
    return textContent(JSON.stringify(pages, null, 2));
  }

  if (name === "get_doc") {
    const page = findPage(pages, args.path);
    if (!page) {
      return textContent(`404 Not Found: no public DataVo page matched '${args.path}'.`, true);
    }
    const markdown = await fetchCachedText(`${base}${page.aiPath}`, ttl);
    return textContent(markdown);
  }

  if (name === "search_docs") {
    const query = String(args.query ?? "").trim();
    if (!query) {
      return textContent("Search query cannot be empty.", true);
    }
    const limit = Math.min(Math.max(Number(args.limit ?? 8), 1), 20);
    const needle = query.toLowerCase();

    const contents = await Promise.all(
      pages.map(async (page) => ({
        page,
        text: await fetchCachedText(`${base}${page.aiPath}`, ttl),
      }))
    );

    const matches = [];
    for (const { page, text } of contents) {
      const idx = text.toLowerCase().indexOf(needle);
      if (idx === -1 && !page.title.toLowerCase().includes(needle)) continue;
      const start = Math.max(0, (idx >= 0 ? idx : 0) - 80);
      const end = Math.min(text.length, (idx >= 0 ? idx : 0) + needle.length + 140);
      matches.push({
        title: page.title,
        route: page.route,
        source: page.source,
        snippet: text.slice(start, end).replaceAll(/\s+/gu, " ").trim(),
      });
      if (matches.length >= limit) break;
    }

    return textContent(JSON.stringify(matches, null, 2));
  }

  return textContent(`Unknown tool '${name}'.`, true);
}

async function handleRpc(message, env) {
  if (!message || typeof message.method !== "string") {
    return rpcError(message?.id ?? null, -32600, "Invalid Request");
  }

  // Notifications have no id; acknowledge without a JSON-RPC body.
  if (!Object.prototype.hasOwnProperty.call(message, "id")) {
    return null;
  }

  switch (message.method) {
    case "initialize":
      return rpcResult(message.id, {
        protocolVersion: message.params?.protocolVersion ?? SUPPORTED_PROTOCOL_VERSION,
        capabilities: { resources: {}, tools: {} },
        serverInfo: SERVER_INFO,
      });

    case "ping":
      return rpcResult(message.id, {});

    case "tools/list":
      return rpcResult(message.id, { tools: toolsList() });

    case "tools/call":
      return rpcResult(message.id, await callTool(message.params, env));

    case "resources/list": {
      const { index } = await loadIndex(env);
      const pages = index.pages || [];
      return rpcResult(message.id, {
        resources: [
          {
            uri: "datavo-docs://index",
            name: "DataVo docs index",
            description: "Index of DataVo manual and blog pages exported for AI retrieval.",
            mimeType: "application/json",
          },
          ...pages.map((p) => ({
            uri: uriForPage(p),
            name: p.title,
            description: `${p.route} (${p.source})`,
            mimeType: "text/markdown",
          })),
        ],
      });
    }

    case "resources/read": {
      const uri = String(message.params?.uri ?? "");
      const { base, ttl, index } = await loadIndex(env);
      const pages = index.pages || [];

      if (uri === "datavo-docs://index") {
        return rpcResult(message.id, {
          contents: [{ uri, mimeType: "application/json", text: JSON.stringify(index, null, 2) }],
        });
      }

      const page = findPage(pages, uri);
      if (!page) {
        return rpcError(message.id, -32602, `Unknown resource URI: ${uri}`);
      }
      const markdown = await fetchCachedText(`${base}${page.aiPath}`, ttl);
      return rpcResult(message.id, {
        contents: [{ uri: uriForPage(page), mimeType: "text/markdown", text: markdown }],
      });
    }

    case "prompts/list":
      return rpcResult(message.id, { prompts: [] });

    default:
      return rpcError(message.id, -32601, `Method not found: ${message.method}`);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (request.method === "GET" && (url.pathname === "/" || url.pathname === "/health")) {
      return jsonResponse({
        ok: true,
        server: SERVER_INFO,
        mcpEndpoint: `${url.origin}/mcp`,
        docsBaseUrl: env.DOCS_BASE_URL,
      });
    }

    if (url.pathname !== "/mcp") {
      return jsonResponse({ error: "Not Found. Use POST /mcp." }, 404);
    }

    if (request.method !== "POST") {
      return jsonResponse({ error: "Method Not Allowed. Use POST /mcp." }, 405);
    }

    let payload;
    try {
      payload = await request.json();
    } catch {
      return jsonResponse(rpcError(null, -32700, "Parse error"), 400);
    }

    if (Array.isArray(payload)) {
      const responses = (await Promise.all(payload.map((msg) => handleRpc(msg, env)))).filter(Boolean);
      return responses.length === 0
        ? new Response(null, { status: 202, headers: CORS_HEADERS })
        : jsonResponse(responses);
    }

    const response = await handleRpc(payload, env);
    if (!response) {
      return new Response(null, { status: 202, headers: CORS_HEADERS });
    }
    return jsonResponse(response);
  },
};
