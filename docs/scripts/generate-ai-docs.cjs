#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { publicManualPages, routeForPublicSource } = require("./public-manual.cjs");

const docsRoot = path.resolve(__dirname, "..");
const publicRoot = path.join(docsRoot, "public");
const aiRoot = path.join(publicRoot, "ai");
const pagesRoot = path.join(aiRoot, "pages");

function pageOutputPath(route) {
  return `${route.replace(/^\//u, "")}.md`;
}

function stripFrontmatter(markdown) {
  return markdown.replace(/^---\n[\s\S]*?\n---\n*/u, "");
}

function stripVueBenchmarkCharts(markdown) {
  return markdown.replace(/\n?<BenchmarkChart\n[\s\S]*?\/>\n?/gu, "\n");
}

function simplifyHtml(markdown) {
  return markdown
    .replace(/<span[^>]*background:([^;"']+)[^>]*><\/span>\s*/gu, "")
    .replace(/<\/?span[^>]*>/gu, "");
}

function extractTitle(markdown, route) {
  const heading = markdown.match(/^#\s+(.+)$/mu);
  if (heading) {
    return heading[1].trim();
  }

  if (route === "/") {
    return "DataVo";
  }

  return route
    .split("/")
    .filter(Boolean)
    .pop()
    .replace(/[-_]/gu, " ")
    .replace(/\b\w/gu, (char) => char.toUpperCase());
}

function normalizeMarkdown(markdown, route, sourcePath) {
  const body = simplifyHtml(stripVueBenchmarkCharts(stripFrontmatter(markdown))).trim();
  const title = extractTitle(body, route);
  const source = path.relative(docsRoot, sourcePath).replaceAll(path.sep, "/");

  return {
    title,
    markdown: [
      `# ${title}`,
      "",
      `> Source route: ${route}`,
      `> Source file: ${source}`,
      "",
      body.replace(/^#\s+.+\n*/u, "").trim(),
      "",
    ].join("\n"),
  };
}

function ensureCleanDirectory(directory) {
  fs.rmSync(directory, { recursive: true, force: true });
  fs.mkdirSync(directory, { recursive: true });
}

function writeFile(relativePath, contents) {
  const target = path.join(publicRoot, relativePath);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, contents);
}

function main() {
  ensureCleanDirectory(aiRoot);
  fs.mkdirSync(pagesRoot, { recursive: true });

  const pages = publicManualPages.map((manualPage) => {
    const filePath = path.join(docsRoot, manualPage.source);
    if (!fs.existsSync(filePath)) {
      throw new Error(`Public manual page is missing: ${manualPage.source}`);
    }

    const route = routeForPublicSource(manualPage.source);
    const aiPath = `/ai/pages/${pageOutputPath(route)}`;
    const source = manualPage.source;
    const normalized = normalizeMarkdown(fs.readFileSync(filePath, "utf8"), route, filePath);
    writeFile(aiPath.replace(/^\//u, ""), normalized.markdown);

    return {
      title: normalized.title,
      route,
      source,
      aiPath,
    };
  });

  writeFile(
    "ai/index.json",
    `${JSON.stringify(
      {
        name: "DataVo Documentation",
        generatedAt: new Date().toISOString(),
        pages,
      },
      null,
      2,
    )}\n`,
  );

  writeFile(
    "llms.txt",
    [
      "# DataVo Documentation",
      "",
      "DataVo is a C#-native embedded database engine. This file exposes only the curated public v0.1 Alpha manual.",
      "",
      "## Manual Pages",
      ...pages.map((page) => `- [${page.title}](${page.aiPath})`),
      "",
      "## Full Index",
      "- [AI docs index](/ai/index.json)",
      "- [Full Markdown export](/llms-full.txt)",
      "- [MCP discovery config](/mcp/datavo-docs.json)",
      "",
    ].join("\n"),
  );

  writeFile(
    "llms-full.txt",
    pages
      .map((page) => {
        const markdown = fs.readFileSync(path.join(publicRoot, page.aiPath), "utf8").trim();
        return `${markdown}\n\n---\n`;
      })
      .join("\n"),
  );

  writeFile(
    "mcp/datavo-docs.json",
    `${JSON.stringify(
      {
        name: "datavo-docs",
        description: "Local MCP server configuration for the curated DataVo v0.1 Alpha manual.",
        docsBaseUrl: "/",
        aiIndex: "/ai/index.json",
        llms: "/llms.txt",
        llmsFull: "/llms-full.txt",
        publicScope: "Curated manual pages only. Internal architecture, audit, generated API, and source folders are excluded.",
        mcpServers: {
          "datavo-docs": {
            command: "node",
            args: ["./mcp/datavo-docs-server.cjs"],
            cwd: "./docs",
          },
        },
      },
      null,
      2,
    )}\n`,
  );

  writeFile(
    "mcp/remote-docs.json",
    `${JSON.stringify(
      {
        name: "DataVo public documentation",
        description: "HTTP-retrievable AI documentation surface for the curated DataVo v0.1 Alpha manual.",
        publicScope: "Curated manual pages only.",
        entrypoints: {
          llms: "/llms.txt",
          llmsFull: "/llms-full.txt",
          aiIndex: "/ai/index.json",
        },
        pages: pages.map((page) => ({
          title: page.title,
          route: page.route,
          markdown: page.aiPath,
        })),
      },
      null,
      2,
    )}\n`,
  );

  console.log(`Generated ${pages.length} AI documentation pages in ${path.relative(process.cwd(), aiRoot)}`);
}

main();
