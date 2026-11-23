import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig, type HeadConfig } from "vitepress";
import { withMermaid } from "vitepress-plugin-mermaid";

const isProdBuild =
  process.env.NODE_ENV === "production" || process.argv.includes("build");

const SITE_URL = "https://arintonakos.github.io/DataVo-DBMS";
const SITE_TITLE = "DataVo-DBMS";
const SITE_DESCRIPTION =
  "C#-native embedded database documentation for DataVo v0.4 (0.4.0-rc.1).";

const configDir = path.dirname(fileURLToPath(import.meta.url));
const ogPublicDir = path.resolve(configDir, "../public/og");

function ogSlugForRelativePath(relativePath: string): string {
  const normalized = relativePath.replaceAll("\\", "/").replace(/^\/+/u, "");
  const stripped = normalized
    .replace(/(^|\/)index\.md$/u, "")
    .replace(/\.md$/u, "")
    .replace(/\/+$/u, "");
  return stripped ? stripped.replaceAll("/", "-") : "index";
}

function canonicalUrlForRelativePath(relativePath: string): string {
  const normalized = relativePath.replaceAll("\\", "/").replace(/^\/+/u, "");
  if (normalized === "index.md") {
    return `${SITE_URL}/`;
  }
  if (normalized.endsWith("/index.md")) {
    return `${SITE_URL}/${normalized.slice(0, -"index.md".length)}`;
  }
  return `${SITE_URL}/${normalized.replace(/\.md$/u, "")}`;
}

function appendMetaIfMissing(
  headList: HeadConfig[],
  attr: "property" | "name",
  key: string,
  content: string,
): void {
  const exists = headList.some(
    ([tag, attrs]) => tag === "meta" && attrs?.[attr] === key,
  );
  if (!exists) {
    headList.push(["meta", { [attr]: key, content }]);
  }
}

const head: HeadConfig[] = [
  ["link", { rel: "icon", type: "image/svg+xml", href: "/DataVo-DBMS/favicon.svg" }],
  ["link", { rel: "icon", type: "image/x-icon", href: "/DataVo-DBMS/favicon.ico" }],
];

if (isProdBuild) {
  head.push([
    "script",
    {
      "data-goatcounter": "https://datavodb.goatcounter.com/count",
      async: "",
      src: "//gc.zgo.at/count.js",
    },
  ]);
}

export default withMermaid(
  defineConfig({
    title: "DataVo",
    base: "/DataVo-DBMS/",
    description: SITE_DESCRIPTION,
    cleanUrls: true,
    head,
    transformPageData(pageData) {
      const frontmatter = pageData.frontmatter ?? {};
      const pageHead: HeadConfig[] = Array.isArray(frontmatter.head)
        ? [...frontmatter.head]
        : [];

      const slug = ogSlugForRelativePath(pageData.relativePath);
      const ogFileName = fs.existsSync(path.join(ogPublicDir, `${slug}.png`))
        ? `${slug}.png`
        : "default.png";
      const ogImageUrl = `${SITE_URL}/og/${ogFileName}`;
      const ogUrl = canonicalUrlForRelativePath(pageData.relativePath);

      const ogTitle =
        typeof frontmatter.title === "string" && frontmatter.title.trim()
          ? frontmatter.title.trim()
          : frontmatter.layout === "home" && frontmatter.hero?.text
            ? `${SITE_TITLE} — ${frontmatter.hero.text}`
            : pageData.title || SITE_TITLE;

      const ogDescription =
        (typeof frontmatter.description === "string" && frontmatter.description.trim()) ||
        pageData.description ||
        (typeof frontmatter.hero?.tagline === "string" && frontmatter.hero.tagline.trim()) ||
        SITE_DESCRIPTION;

      const ogType =
        pageData.relativePath.startsWith("blog/") &&
        pageData.relativePath !== "blog/index.md"
          ? "article"
          : "website";

      appendMetaIfMissing(pageHead, "property", "og:type", ogType);
      appendMetaIfMissing(pageHead, "property", "og:site_name", SITE_TITLE);
      appendMetaIfMissing(pageHead, "property", "og:url", ogUrl);
      appendMetaIfMissing(pageHead, "property", "og:title", ogTitle);
      appendMetaIfMissing(pageHead, "property", "og:description", ogDescription);
      appendMetaIfMissing(pageHead, "property", "og:image", ogImageUrl);
      appendMetaIfMissing(pageHead, "property", "og:image:width", "1200");
      appendMetaIfMissing(pageHead, "property", "og:image:height", "630");
      appendMetaIfMissing(pageHead, "name", "twitter:card", "summary_large_image");
      appendMetaIfMissing(pageHead, "name", "twitter:title", ogTitle);
      appendMetaIfMissing(pageHead, "name", "twitter:description", ogDescription);
      appendMetaIfMissing(pageHead, "name", "twitter:image", ogImageUrl);

      frontmatter.head = pageHead;
      pageData.frontmatter = frontmatter;
    },
    srcExclude: [
      "public/**",
      "features/**",
      "superpowers/**",
      "research/**",
    ],
    themeConfig: {
      logo: {
        light: "/logo-light.svg",
        dark: "/logo-dark.svg",
      },
      siteTitle: "DataVo",
      nav: [
        { text: "Get Started", link: "/manual/getting-started/quickstart" },
        { text: "Fundamentals", link: "/manual/fundamentals/storage-modes" },
        { text: "Client APIs", link: "/manual/client-interfaces/datavo-context-api" },
        { text: "SQL Reference", link: "/manual/sql/compatibility" },
        { text: "Blog", link: "/blog/" },
      ],
      sidebar: {
        "/manual/": [
          {
            text: "Getting Started",
            collapsed: false,
            items: [
              { text: "Introduction", link: "/manual/getting-started/introduction" },
              { text: "Installation & Packages", link: "/manual/getting-started/installation" },
              { text: "Quickstart", link: "/manual/getting-started/quickstart" },
              { text: "Choosing an Interface", link: "/manual/getting-started/choosing-an-interface" },
              { text: "Status & Roadmap", link: "/manual/getting-started/roadmap" },
            ],
          },
          {
            text: "Fundamentals",
            collapsed: false,
            items: [
              { text: "Storage Modes", link: "/manual/fundamentals/storage-modes" },
              { text: "LSM & Durability", link: "/manual/fundamentals/lsm-and-durability" },
              { text: "Transactions & MVCC", link: "/manual/fundamentals/transactions-and-mvcc" },
              { text: "Vector Search", link: "/manual/fundamentals/vector-search" },
              { text: "Streaming Queries", link: "/manual/fundamentals/streaming-queries" },
              { text: "Live Reactive Views (Z-Set IVM)", link: "/manual/fundamentals/reactive-views" },
              { text: "DataVo-Sync", link: "/manual/fundamentals/datavo-sync" },
            ],
          },
          {
            text: "Client Interfaces",
            collapsed: true,
            items: [
              { text: "DataVoContext API", link: "/manual/client-interfaces/datavo-context-api" },
              { text: "Roslyn Source Generators", link: "/manual/client-interfaces/source-generators" },
              { text: "Compiled LINQ", link: "/manual/client-interfaces/compiled-linq" },
              { text: "Native AOT", link: "/manual/client-interfaces/native-aot" },
              { text: "ADO.NET Provider", link: "/manual/client-interfaces/ado-net" },
              { text: "Dapper", link: "/manual/client-interfaces/dapper" },
              { text: "Entity Framework Core", link: "/manual/client-interfaces/entity-framework" },
            ],
          },
          {
            text: "SQL Reference",
            collapsed: true,
            items: [
              { text: "SQL Compatibility", link: "/manual/sql/compatibility" },
              { text: "Data Types", link: "/manual/sql/data-types" },
              {
                text: "Statements",
                collapsed: false,
                items: [
                  { text: "SELECT", link: "/manual/sql/statements/select" },
                  { text: "INSERT", link: "/manual/sql/statements/insert" },
                  { text: "UPDATE", link: "/manual/sql/statements/update" },
                  { text: "DELETE", link: "/manual/sql/statements/delete" },
                  { text: "CREATE TABLE", link: "/manual/sql/statements/create-table" },
                  { text: "ALTER TABLE & DROP", link: "/manual/sql/statements/alter-and-drop" },
                  { text: "CREATE INDEX", link: "/manual/sql/statements/indexes" },
                  { text: "EXPLAIN", link: "/manual/sql/statements/explain" },
                  { text: "Transactions", link: "/manual/sql/statements/transactions" },
                  { text: "Databases & Security", link: "/manual/sql/statements/databases-and-security" },
                ],
              },
              { text: "Functions & Operators", link: "/manual/sql/functions" },
            ],
          },
          {
            text: "Internals & Performance",
            collapsed: true,
            items: [
              { text: "Query Planner & Fast Paths", link: "/manual/internals/query-planner" },
              { text: "Zero-Allocation Rows", link: "/manual/internals/zero-allocation-rows" },
              { text: "Compiled Scan Kernels", link: "/manual/internals/compiled-scan-kernels" },
              { text: "DataVo vs SQLite", link: "/manual/performance/datavo-vs-sqlite" },
              { text: "Benchmarks", link: "/manual/performance/benchmarks" },
              { text: "Reproducing Benchmarks", link: "/manual/performance/reproducing-benchmarks" },
            ],
          },
          {
            text: "Reference",
            collapsed: true,
            items: [
              { text: "Configuration", link: "/manual/reference/configuration" },
              { text: "Error Handling", link: "/manual/reference/error-handling" },
              { text: "Limits & Unsupported Features", link: "/manual/reference/limits" },
              { text: "AI Access & MCP", link: "/manual/reference/ai-access" },
            ],
          },
        ],
        "/blog/": [
          {
            text: "Engineering Blog",
            items: [
              { text: "All Posts", link: "/blog/" },
              {
                text: "Part 1: Where It Wins and Where It Loses",
                link: "/blog/part-1-where-it-wins-and-where-it-loses",
              },
              {
                text: "Part 2: Packed Buffers, Native AOT & Roslyn Codegen",
                link: "/blog/part-2-packed-buffers-aot-and-roslyn-codegen",
              },
            ],
          },
        ],
      },
      socialLinks: [
        { icon: "github", link: "https://github.com/ArintonAkos/DataVo-DBMS" },
      ],
      search: {
        provider: "local",
      },
    },
  }),
);
