import { defineConfig, type HeadConfig } from "vitepress";
import { withMermaid } from "vitepress-plugin-mermaid";

const isProdBuild =
  process.env.NODE_ENV === "production" || process.argv.includes("build");

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
    description:
      "C#-native embedded database documentation for DataVo v0.4 (0.4.0-rc.1).",
    cleanUrls: true,
    head,
    srcExclude: [
      "superpowers/**",
      "specs/**",
      "plans/**",
      "public/**",
      "DataVo.Core/**",
      "architecture/**",
      "audit/**",
      "features/**",
      "Parser.cs.tmp",
      "ai-mcp.md",
      "benchmarks.md",
      "configuration.md",
      "fast-paths.md",
      "quickstart.md",
      "source-generators.md",
      "sql-reference.md",
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
                  { text: "DML (INSERT, UPDATE, DELETE)", link: "/manual/sql/statements/dml" },
                  { text: "DDL (CREATE, ALTER, DROP)", link: "/manual/sql/statements/ddl" },
                  { text: "Indexes", link: "/manual/sql/statements/indexes" },
                  { text: "Transactions & Savepoints", link: "/manual/sql/statements/transactions" },
                  { text: "Admin & Security", link: "/manual/sql/statements/admin-and-security" },
                ],
              },
              { text: "Built-in Functions", link: "/manual/sql/functions" },
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
