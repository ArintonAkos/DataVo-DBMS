import { defineConfig } from "vitepress";
import { withMermaid } from "vitepress-plugin-mermaid";

export default withMermaid(
  defineConfig({
    title: "DataVo",
    base: "/DataVo-DBMS/",
    description:
      "C#-native embedded database documentation for DataVo v0.2.0-rc.3.",
    cleanUrls: true,
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
      siteTitle: "DataVo",
      nav: [
        { text: "Home", link: "/" },
        { text: "Get Started", link: "/manual/getting-started/installation" },
        { text: "Manual", link: "/manual/preface/what-is-datavo" },
        { text: "Quickstart", link: "/manual/tutorial/quickstart" },
        { text: "SQL", link: "/manual/sql-language/supported-sql" },
        { text: "Benchmarks", link: "/manual/performance/benchmarks" },
        { text: "AI Access", link: "/manual/reference/ai-access" },
      ],
      sidebar: [
        {
          text: "Preface",
          collapsed: false,
          items: [
            { text: "What Is DataVo?", link: "/manual/preface/what-is-datavo" },
            { text: "Project Status & Scope", link: "/manual/preface/project-status" },
            { text: "v0.1 Alpha Scope", link: "/manual/preface/alpha-scope" },
            { text: "Roadmap", link: "/manual/preface/roadmap" },
          ],
        },
        {
          text: "Getting Started",
          collapsed: false,
          items: [
            { text: "Installation & Packages", link: "/manual/getting-started/installation" },
            { text: "Choosing an Interface", link: "/manual/getting-started/choosing-an-interface" },
          ],
        },
        {
          text: "Tutorial",
          collapsed: true,
          items: [
            { text: "Quickstart", link: "/manual/tutorial/quickstart" },
            { text: "Using DataVoContext", link: "/manual/tutorial/datavo-context" },
            { text: "ADO.NET & Dapper", link: "/manual/tutorial/ado-net-and-dapper" },
            { text: "Entity Framework Example", link: "/manual/tutorial/entity-framework-example" },
            { text: "Vector Search Example", link: "/manual/tutorial/vector-search-example" },
          ],
        },
        {
          text: "SQL Language",
          collapsed: true,
          items: [
            { text: "Supported SQL", link: "/manual/sql-language/supported-sql" },
            { text: "CREATE TABLE", link: "/manual/sql-language/create-table" },
            { text: "Data Manipulation", link: "/manual/sql-language/data-manipulation" },
            { text: "Queries", link: "/manual/sql-language/queries" },
            { text: "Vector Search and Indexes", link: "/manual/sql-language/vector-search-syntax" },
            { text: "SQL Compatibility Matrix", link: "/manual/sql-language/sql-compatibility" },
          ],
        },
        {
          text: "Storage Engine",
          collapsed: true,
          items: [
            { text: "Storage Modes", link: "/manual/storage-engine/storage-modes" },
            { text: "LSM Mode", link: "/manual/storage-engine/lsm-mode" },
            { text: "WAL And Durability", link: "/manual/storage-engine/wal-and-durability" },
            { text: "Transactions and MVCC", link: "/manual/storage-engine/transactions-acid-mvcc" },
            { text: "Query Planner And Fast Paths", link: "/manual/storage-engine/query-planner-fast-paths" },
          ],
        },
        {
          text: "Client Interfaces",
          collapsed: true,
          items: [
            { text: "Connecting to DataVo", link: "/manual/client-interfaces/datavo-context-api" },
            { text: "ADO.NET Provider", link: "/manual/client-interfaces/ado-net" },
            { text: "Dapper Support", link: "/manual/client-interfaces/dapper" },
            { text: "Entity Framework Support", link: "/manual/client-interfaces/entity-framework" },
            { text: "Roslyn Source Generators", link: "/manual/client-interfaces/source-generators" },
            { text: "Native AOT", link: "/manual/client-interfaces/native-aot" },
          ],
        },
        {
          text: "Internals",
          collapsed: true,
          items: [
            { text: "Zero-Allocation Rows", link: "/manual/internals/zero-allocation-rows" },
            { text: "Compiled Scan Kernels", link: "/manual/internals/compiled-scan-kernels" },
          ],
        },
        {
          text: "Performance",
          collapsed: true,
          items: [
            { text: "Benchmark Results", link: "/manual/performance/benchmarks" },
            { text: "Benchmark Methodology", link: "/manual/performance/methodology" },
            { text: "Reproducing Benchmarks", link: "/manual/performance/reproducing-benchmarks" },
          ],
        },
        {
          text: "Reference",
          collapsed: true,
          items: [
            { text: "Configuration Reference", link: "/manual/reference/configuration" },
            { text: "Limits", link: "/manual/reference/limits" },
            { text: "Unsupported Features", link: "/manual/reference/unsupported-features" },
            { text: "Error Handling", link: "/manual/reference/error-handling" },
            { text: "AI Access And MCP", link: "/manual/reference/ai-access" },
          ],
        },
      ],
      socialLinks: [
        { icon: "github", link: "https://github.com/ArintonAkos/DataVo-DBMS" },
      ],
      search: {
        provider: "local",
      },
    },
  }),
);
