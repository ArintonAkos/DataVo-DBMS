#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { publicManualPages, publicManualRoutes, routeForPublicSource } = require("../scripts/public-manual.cjs");

const docsRoot = path.resolve(__dirname, "..");
const publicRoot = path.join(docsRoot, "public");
const aiRoot = path.join(publicRoot, "ai");
const indexPath = path.join(aiRoot, "index.json");
const serverInfo = { name: "datavo-docs", version: "0.1.0" };
const allowedRoutes = publicManualRoutes();
const allowedSources = new Set(publicManualPages.map((page) => page.source.toLowerCase()));
const deniedPathFragments = [
  "architecture/",
  "audit/",
  "datavo.core/",
  "features/",
  "superpowers/",
  ".vitepress/",
  "node_modules/",
  "public/",
  "src/",
];

let inputBuffer = "";

process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  inputBuffer += chunk;
  const lines = inputBuffer.split("\n");
  inputBuffer = lines.pop() || "";
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.length > 0) {
      handleRawMessage(trimmed);
    }
  }
});

process.stdin.on("end", () => process.exit(0));

function handleRawMessage(raw) {
  let message;
  try {
    message = JSON.parse(raw);
  } catch (error) {
    sendError(null, -32700, "Parse error", String(error));
    return;
  }

  Promise.resolve(handleMessage(message)).catch((error) => {
    if (message && Object.prototype.hasOwnProperty.call(message, "id")) {
      sendError(message.id, -32603, "Internal error", error instanceof Error ? error.message : String(error));
    }
  });
}

async function handleMessage(message) {
  if (!message || typeof message.method !== "string") {
    sendError(message?.id ?? null, -32600, "Invalid Request");
    return;
  }

  if (!Object.prototype.hasOwnProperty.call(message, "id")) {
    return;
  }

  switch (message.method) {
    case "initialize":
      sendResult(message.id, {
        protocolVersion: message.params?.protocolVersion ?? "2024-11-05",
        capabilities: {
          resources: {},
          tools: {},
        },
        serverInfo,
      });
      return;
    case "tools/list":
      sendResult(message.id, { tools: toolsList() });
      return;
    case "tools/call":
      sendResult(message.id, callTool(message.params ?? {}));
      return;
    case "resources/list":
      sendResult(message.id, { resources: resourcesList() });
      return;
    case "resources/read":
      sendResult(message.id, readResource(message.params ?? {}));
      return;
    case "prompts/list":
      sendResult(message.id, { prompts: [] });
      return;
    default:
      sendError(message.id, -32601, `Method not found: ${message.method}`);
  }
}

function toolsList() {
  return [
    {
      name: "list_docs",
      description: "List DataVo documentation pages available as AI-readable Markdown.",
      inputSchema: {
        type: "object",
        properties: {},
        additionalProperties: false,
      },
    },
    {
      name: "get_doc",
      description: "Fetch one DataVo documentation page by route, source path, title, or datavo-docs URI.",
      inputSchema: {
        type: "object",
        properties: {
          path: {
            type: "string",
            description: "Route such as /quickstart, source path such as quickstart.md, or URI such as datavo-docs://page/quickstart.",
          },
        },
        required: ["path"],
        additionalProperties: false,
      },
    },
    {
      name: "search_docs",
      description: "Search DataVo documentation Markdown with a simple case-insensitive text query.",
      inputSchema: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "Search text.",
          },
          limit: {
            type: "integer",
            minimum: 1,
            maximum: 20,
            default: 8,
          },
        },
        required: ["query"],
        additionalProperties: false,
      },
    },
  ];
}

function callTool(params) {
  const name = params.name;
  const args = params.arguments ?? {};

  if (name === "list_docs") {
    return textContent(JSON.stringify(loadIndex().pages, null, 2));
  }

  if (name === "get_doc") {
    if (isDeniedIdentifier(String(args.path ?? ""))) {
      return textContent("Access Denied: this MCP server only exposes the curated public DataVo manual.", true);
    }

    const page = findPage(String(args.path ?? ""));
    if (!page) {
      return textContent(`404 Not Found: no public DataVo manual page matched '${args.path}'.`, true);
    }

    return textContent(readPage(page));
  }

  if (name === "search_docs") {
    const query = String(args.query ?? "").trim();
    if (query.length === 0) {
      return textContent("Search query cannot be empty.", true);
    }

    const limit = Math.min(Math.max(Number(args.limit ?? 8), 1), 20);
    return textContent(JSON.stringify(searchDocs(query, limit), null, 2));
  }

  return textContent(`Unknown tool '${name}'.`, true);
}

function resourcesList() {
  const pages = loadIndex().pages;
  return [
    {
      uri: "datavo-docs://index",
      name: "DataVo docs index",
      description: "Index of DataVo documentation pages exported for AI retrieval.",
      mimeType: "application/json",
    },
    ...pages.map((page) => ({
      uri: uriForPage(page),
      name: page.title,
      description: `${page.route} (${page.source})`,
      mimeType: "text/markdown",
    })),
  ];
}

function readResource(params) {
  const uri = String(params.uri ?? "");
  if (uri === "datavo-docs://index") {
    return {
      contents: [
        {
          uri,
          mimeType: "application/json",
          text: JSON.stringify(loadIndex(), null, 2),
        },
      ],
    };
  }

  const page = findPage(uri);
  if (!page) {
    throw new Error(`No DataVo documentation resource matched '${uri}'.`);
  }

  return {
    contents: [
      {
        uri: uriForPage(page),
        mimeType: "text/markdown",
        text: readPage(page),
      },
    ],
  };
}

function loadIndex() {
  if (!fs.existsSync(indexPath)) {
    throw new Error("AI documentation export is missing. Run 'npm run docs:ai' in the docs directory.");
  }

  const index = JSON.parse(fs.readFileSync(indexPath, "utf8"));
  return {
    ...index,
    pages: index.pages.filter((page) =>
      allowedRoutes.has(page.route) &&
      allowedSources.has(String(page.source).toLowerCase()) &&
      page.route === routeForPublicSource(page.source)),
  };
}

function findPage(identifier) {
  if (isDeniedIdentifier(identifier)) {
    return null;
  }

  const needle = normalizeIdentifier(identifier);
  if (!needle) {
    return null;
  }

  return loadIndex().pages.find((page) => {
    const candidates = [
      page.route,
      page.source,
      page.title,
      page.aiPath,
      uriForPage(page),
    ].map(normalizeIdentifier);
    return candidates.includes(needle);
  }) ?? null;
}

function isDeniedIdentifier(identifier) {
  const normalized = String(identifier).trim().replaceAll("\\", "/").toLowerCase();
  return deniedPathFragments.some((fragment) => normalized.includes(fragment));
}

function normalizeIdentifier(value) {
  return String(value)
    .trim()
    .replace(/^datavo-docs:\/\/page\//u, "/")
    .replace(/^datavo-docs:\/\/index$/u, "/ai/index.json")
    .replace(/^\/ai\/pages\//u, "/")
    .replace(/\.md$/u, "")
    .replace(/\/index$/u, "/")
    .replace(/\/$/u, "")
    .toLowerCase() || "/";
}

function readPage(page) {
  const relative = page.aiPath.replace(/^\//u, "");
  const fullPath = path.join(publicRoot, relative);
  if (!fullPath.startsWith(publicRoot)) {
    throw new Error(`Invalid AI page path '${page.aiPath}'.`);
  }

  return fs.readFileSync(fullPath, "utf8");
}

function searchDocs(query, limit) {
  const q = query.toLowerCase();
  return loadIndex().pages
    .map((page) => {
      const markdown = readPage(page);
      const lower = markdown.toLowerCase();
      const index = lower.indexOf(q);
      if (index < 0) {
        return null;
      }

      const start = Math.max(0, index - 120);
      const end = Math.min(markdown.length, index + q.length + 180);
      return {
        title: page.title,
        route: page.route,
        uri: uriForPage(page),
        aiPath: page.aiPath,
        snippet: markdown.slice(start, end).replace(/\s+/gu, " ").trim(),
      };
    })
    .filter(Boolean)
    .slice(0, limit);
}

function uriForPage(page) {
  const slug = page.route === "/" ? "index" : page.route.replace(/^\//u, "");
  return `datavo-docs://page/${slug}`;
}

function textContent(text, isError = false) {
  return {
    isError,
    content: [
      {
        type: "text",
        text,
      },
    ],
  };
}

function sendResult(id, result) {
  send({ jsonrpc: "2.0", id, result });
}

function sendError(id, code, message, data) {
  send({
    jsonrpc: "2.0",
    id,
    error: {
      code,
      message,
      ...(data ? { data } : {}),
    },
  });
}

function send(message) {
  process.stdout.write(JSON.stringify(message) + "\n");
}
