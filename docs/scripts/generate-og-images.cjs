#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { chromium } = require("playwright");

const docsRoot = path.resolve(__dirname, "..");
const publicRoot = path.join(docsRoot, "public");
const ogRoot = path.join(publicRoot, "og");
const logoPath = path.join(publicRoot, "logo-dark.svg");
const interFontPath = path.join(
  docsRoot,
  "node_modules/vitepress/dist/client/theme-default/fonts/inter-roman-latin.woff2",
);

const SITE_WORDMARK = "DataVo-DBMS";
const SITE_DISPLAY_URL = "arintonakos.github.io/DataVo-DBMS";
const DEFAULT_TITLE = "C#-native embedded database";
const DEFAULT_DESCRIPTION =
  "In-process SQL, LSM storage, vector search, and live Z-set reactive views for .NET 8, 9 and 10.";

const EXCLUDED_TOP_DIRS = new Set([
  ".vitepress",
  "mcp",
  "mcp-worker",
  "node_modules",
  "public",
  "research",
  "scripts",
  "src",
  "test-results",
  "tests",
]);

function ogSlugForRelativePath(relativePath) {
  const normalized = relativePath.replaceAll("\\", "/").replace(/^\/+/u, "");
  const stripped = normalized
    .replace(/(^|\/)index\.md$/u, "")
    .replace(/\.md$/u, "")
    .replace(/\/+$/u, "");
  return stripped ? stripped.replaceAll("/", "-") : "index";
}

function walkMarkdownFiles(dir, results = []) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.isDirectory()) {
      if (dir === docsRoot && EXCLUDED_TOP_DIRS.has(entry.name)) {
        continue;
      }
      walkMarkdownFiles(path.join(dir, entry.name), results);
    } else if (entry.isFile() && entry.name.endsWith(".md")) {
      const fullPath = path.join(dir, entry.name);
      const relativePath = path.relative(docsRoot, fullPath).replaceAll(path.sep, "/");
      results.push({ fullPath, relativePath });
    }
  }
  return results.sort((a, b) => a.relativePath.localeCompare(b.relativePath));
}

function unquoteYamlScalar(rawValue) {
  const trimmed = rawValue.trim();
  if (trimmed.length >= 2 && trimmed.startsWith('"') && trimmed.endsWith('"')) {
    try {
      return JSON.parse(trimmed);
    } catch {
      return trimmed.slice(1, -1).replace(/\\"/gu, '"').replace(/\\\\/gu, "\\");
    }
  }
  if (trimmed.length >= 2 && trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1).replace(/''/gu, "'");
  }
  return trimmed;
}

function parseFrontmatter(markdown) {
  const match = markdown.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n?/u);
  if (!match) {
    return { data: {}, body: markdown };
  }

  const rawFrontmatter = match[1];
  const body = markdown.slice(match[0].length);
  const data = {};
  const lines = rawFrontmatter.split(/\r?\n/u);

  let currentSection = null;
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    if (!line.trim() || line.trim().startsWith("#")) {
      continue;
    }

    const topLevelMatch = line.match(/^([A-Za-z0-9_-]+):\s*(.*)$/u);
    if (topLevelMatch) {
      const key = topLevelMatch[1];
      const value = topLevelMatch[2];
      if (!value.trim()) {
        currentSection = key;
        data[key] = {};
      } else if (value.trim() === ">" || value.trim() === "|" || value.trim() === ">-" || value.trim() === "|-") {
        const blockLines = [];
        while (i + 1 < lines.length && /^\s+/u.test(lines[i + 1])) {
          i += 1;
          blockLines.push(lines[i].trim());
        }
        data[key] = blockLines.join(" ").trim();
        currentSection = null;
      } else {
        data[key] = unquoteYamlScalar(value);
        currentSection = null;
      }
      continue;
    }

    if (currentSection && typeof data[currentSection] === "object") {
      const nestedMatch = line.match(/^\s{2}([A-Za-z0-9_-]+):\s*(.+)$/u);
      if (nestedMatch) {
        data[currentSection][nestedMatch[1]] = unquoteYamlScalar(nestedMatch[2]);
      }
    }
  }

  return { data, body };
}

function formatTypographicText(text) {
  return text
    .replace(/\b(\d+(?:\.\d+)?)\s+(ns|µs|us|ms|s|B|KB|MB|GB)\b/gu, "$1\u00A0$2")
    .replace(/\.NET\s+(\d+)/gu, ".NET\u00A0$1");
}

function cleanMarkdownInline(text) {
  return formatTypographicText(
    text
      .replace(/!\[([^\]]*)\]\([^)]*\)/gu, "$1")
      .replace(/\[([^\]]+)\]\([^)]*\)/gu, "$1")
      .replace(/<[^>]+>/gu, "")
      .replace(/`([^`]+)`/gu, "$1")
      .replace(/\*\*([^*]+)\*\*/gu, "$1")
      .replace(/__([^_]+)__/gu, "$1")
      .replace(/\*([^*]+)\*/gu, "$1")
      .replace(/_([^_]+)_/gu, "$1")
      .replace(/\s+/gu, " ")
      .trim(),
  );
}

function extractFirstHeading(body) {
  const match = body.match(/^#\s+(.+)$/mu);
  return match ? cleanMarkdownInline(match[1]) : "";
}

function extractFirstParagraph(body) {
  const withoutCodeBlocks = body.replace(/```[\s\S]*?```/gu, "\n");
  const blocks = withoutCodeBlocks.split(/\r?\n\s*\r?\n/u);
  for (const rawBlock of blocks) {
    const trimmed = rawBlock.trim();
    if (
      !trimmed ||
      trimmed.startsWith("#") ||
      trimmed.startsWith(":::") ||
      trimmed.startsWith("<") ||
      trimmed.startsWith("|") ||
      trimmed.startsWith("- ") ||
      trimmed.startsWith("* ") ||
      /^\d+\.\s/u.test(trimmed) ||
      /^\*[^*]+\*$/u.test(trimmed)
    ) {
      continue;
    }
    const cleaned = cleanMarkdownInline(trimmed);
    if (cleaned.length >= 20) {
      return cleaned;
    }
  }
  return "";
}

function extractPageMeta(fullPath, relativePath) {
  const markdown = fs.readFileSync(fullPath, "utf8");
  const { data, body } = parseFrontmatter(markdown);

  let title = "";
  if (typeof data.title === "string" && data.title.trim()) {
    title = cleanMarkdownInline(data.title);
  } else if (data.layout === "home" && data.hero && typeof data.hero.text === "string") {
    title = cleanMarkdownInline(data.hero.text);
  } else {
    title = extractFirstHeading(body);
  }

  if (!title) {
    const slug = ogSlugForRelativePath(relativePath);
    title = slug
      .split("-")
      .map((part) => (part ? part[0].toUpperCase() + part.slice(1) : ""))
      .join(" ");
  }

  let description = "";
  if (typeof data.description === "string" && data.description.trim()) {
    description = cleanMarkdownInline(data.description);
  } else if (data.layout === "home" && data.hero && typeof data.hero.tagline === "string") {
    description = cleanMarkdownInline(data.hero.tagline);
  } else {
    description = extractFirstParagraph(body) || DEFAULT_DESCRIPTION;
  }

  return {
    slug: ogSlugForRelativePath(relativePath),
    relativePath,
    title,
    description,
  };
}

function buildTemplateHtml(logoSvg, interFontBase64) {
  const fontFaceCss = interFontBase64
    ? `@font-face {
        font-family: 'Inter';
        src: url('data:font/woff2;base64,${interFontBase64}') format('woff2');
        font-weight: 100 900;
        font-style: normal;
        font-display: block;
      }`
    : "";

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=1200, initial-scale=1.0" />
<style>
  ${fontFaceCss}

  * {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
  }

  html, body {
    width: 1200px;
    height: 630px;
    overflow: hidden;
    background: #0D1117;
    color: #F0F6FC;
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-rendering: geometricPrecision;
  }

  .card {
    width: 1200px;
    height: 630px;
    padding: 72px 84px 64px;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    background: #0D1117;
    border-top: 4px solid #30363D;
  }

  .top-row {
    display: flex;
    align-items: center;
    gap: 14px;
  }

  .logo {
    width: 36px;
    height: 36px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
  }

  .logo svg {
    width: 36px;
    height: 36px;
    display: block;
  }

  .wordmark {
    font-size: 24px;
    font-weight: 600;
    letter-spacing: -0.025em;
    color: #F0F6FC;
    line-height: 1;
  }

  .center {
    display: flex;
    flex-direction: column;
    gap: 22px;
    max-width: 1032px;
    margin: auto 0;
    padding: 20px 0;
  }

  .title {
    font-size: 62px;
    font-weight: 700;
    letter-spacing: -0.035em;
    line-height: 1.12;
    color: #FFFFFF;
    text-wrap: balance;
    display: -webkit-box;
    -webkit-line-clamp: 3;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }

  .title.size-md {
    font-size: 52px;
    line-height: 1.14;
  }

  .title.size-lg {
    font-size: 44px;
    line-height: 1.18;
  }

  .title.size-xl {
    font-size: 38px;
    line-height: 1.22;
  }

  .description {
    font-size: 25px;
    font-weight: 400;
    letter-spacing: -0.012em;
    line-height: 1.48;
    color: #8B949E;
    max-width: 960px;
    display: -webkit-box;
    -webkit-line-clamp: 3;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }

  .description.compact {
    font-size: 22px;
    line-height: 1.46;
  }

  .bottom-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding-top: 24px;
    border-top: 1px solid #21262D;
  }

  .url {
    font-family: 'SFMono-Regular', 'JetBrains Mono', Menlo, Monaco, Consolas, 'Liberation Mono', 'DejaVu Sans Mono', ui-monospace, monospace;
    font-size: 18px;
    font-weight: 500;
    letter-spacing: -0.01em;
    color: #7D8590;
    line-height: 1;
  }
</style>
</head>
<body>
  <main class="card">
    <header class="top-row">
      <div class="logo">${logoSvg}</div>
      <span class="wordmark">${SITE_WORDMARK}</span>
    </header>

    <section class="center">
      <h1 id="og-title" class="title"></h1>
      <p id="og-description" class="description"></p>
    </section>

    <footer class="bottom-row">
      <span class="url">${SITE_DISPLAY_URL}</span>
    </footer>
  </main>
</body>
</html>`;
}

async function launchBrowser() {
  try {
    return await chromium.launch({ headless: true });
  } catch (bundledError) {
    const candidates = [
      { channel: "chrome", headless: true },
      { executablePath: "/usr/bin/google-chrome", headless: true },
      { executablePath: "/usr/bin/chromium", headless: true },
      { executablePath: "/usr/bin/chromium-browser", headless: true },
      {
        executablePath: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        headless: true,
      },
    ];

    for (const options of candidates) {
      if (options.executablePath && !fs.existsSync(options.executablePath)) {
        continue;
      }
      try {
        return await chromium.launch(options);
      } catch {
        // Try next available Chrome/Chromium binary.
      }
    }

    throw new Error(
      `Failed to launch Chromium for OG image generation. Install Playwright Chromium with "npm run test:browser:install". Original error: ${bundledError.message}`,
    );
  }
}

async function main() {
  const startedAt = Date.now();
  const files = walkMarkdownFiles(docsRoot);
  const pages = files.map(({ fullPath, relativePath }) => extractPageMeta(fullPath, relativePath));

  const seenSlugs = new Set();
  for (const page of pages) {
    if (seenSlugs.has(page.slug)) {
      throw new Error(`Duplicate OG route slug "${page.slug}" for ${page.relativePath}`);
    }
    seenSlugs.add(page.slug);
  }

  pages.push({
    slug: "default",
    relativePath: "(fallback)",
    title: DEFAULT_TITLE,
    description: DEFAULT_DESCRIPTION,
  });

  const logoSvg = fs.readFileSync(logoPath, "utf8").trim();
  const interFontBase64 = fs.existsSync(interFontPath)
    ? fs.readFileSync(interFontPath).toString("base64")
    : "";
  const templateHtml = buildTemplateHtml(logoSvg, interFontBase64);

  fs.rmSync(ogRoot, { recursive: true, force: true });
  fs.mkdirSync(ogRoot, { recursive: true });

  const browser = await launchBrowser();
  try {
    const context = await browser.newContext({
      viewport: { width: 1200, height: 630 },
      deviceScaleFactor: 1,
    });
    const page = await context.newPage();
    await page.setContent(templateHtml, { waitUntil: "load" });
    await page.evaluate(() => document.fonts.ready);

    for (const item of pages) {
      await page.evaluate(
        ({ title, description }) => {
          const titleEl = document.getElementById("og-title");
          const descEl = document.getElementById("og-description");

          titleEl.textContent = title;
          titleEl.className = "title";
          if (title.length > 90) {
            titleEl.classList.add("size-xl");
          } else if (title.length > 58) {
            titleEl.classList.add("size-lg");
          } else if (title.length > 28) {
            titleEl.classList.add("size-md");
          }

          descEl.textContent = description;
          descEl.className = title.length > 58 ? "description compact" : "description";
        },
        { title: item.title, description: item.description },
      );

      const outputPath = path.join(ogRoot, `${item.slug}.png`);
      await page.screenshot({
        path: outputPath,
        type: "png",
        clip: { x: 0, y: 0, width: 1200, height: 630 },
      });
    }

    await context.close();
  } finally {
    await browser.close();
  }

  const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(2);
  console.log(
    `Generated ${pages.length} OpenGraph images in ${path.relative(process.cwd(), ogRoot)} (${elapsedSec}s)`,
  );
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

module.exports = {
  ogSlugForRelativePath,
};
