const publicManualPages = [
  // Getting Started
  { title: "Introduction", source: "manual/getting-started/introduction.md" },
  { title: "Installation & Packages", source: "manual/getting-started/installation.md" },
  { title: "Quickstart", source: "manual/getting-started/quickstart.md" },
  { title: "Choosing an Interface", source: "manual/getting-started/choosing-an-interface.md" },
  { title: "Status & Roadmap", source: "manual/getting-started/roadmap.md" },

  // Fundamentals
  { title: "Storage Modes", source: "manual/fundamentals/storage-modes.md" },
  { title: "LSM & Durability", source: "manual/fundamentals/lsm-and-durability.md" },
  { title: "Transactions & MVCC", source: "manual/fundamentals/transactions-and-mvcc.md" },
  { title: "Vector Search", source: "manual/fundamentals/vector-search.md" },
  { title: "Streaming Queries", source: "manual/fundamentals/streaming-queries.md" },
  { title: "Live Reactive Views (Z-Set IVM)", source: "manual/fundamentals/reactive-views.md" },
  { title: "DataVo-Sync", source: "manual/fundamentals/datavo-sync.md" },

  // Client Interfaces
  { title: "DataVoContext API", source: "manual/client-interfaces/datavo-context-api.md" },
  { title: "Roslyn Source Generators", source: "manual/client-interfaces/source-generators.md" },
  { title: "Compiled LINQ", source: "manual/client-interfaces/compiled-linq.md" },
  { title: "Native AOT", source: "manual/client-interfaces/native-aot.md" },
  { title: "ADO.NET Provider", source: "manual/client-interfaces/ado-net.md" },
  { title: "Dapper", source: "manual/client-interfaces/dapper.md" },
  { title: "Entity Framework Core", source: "manual/client-interfaces/entity-framework.md" },

  // SQL Reference
  { title: "SQL Compatibility", source: "manual/sql/compatibility.md" },
  { title: "Data Types", source: "manual/sql/data-types.md" },
  { title: "SELECT", source: "manual/sql/statements/select.md" },
  { title: "INSERT", source: "manual/sql/statements/insert.md" },
  { title: "UPDATE", source: "manual/sql/statements/update.md" },
  { title: "DELETE", source: "manual/sql/statements/delete.md" },
  { title: "CREATE TABLE", source: "manual/sql/statements/create-table.md" },
  { title: "ALTER TABLE & DROP", source: "manual/sql/statements/alter-and-drop.md" },
  { title: "CREATE INDEX", source: "manual/sql/statements/indexes.md" },
  { title: "EXPLAIN", source: "manual/sql/statements/explain.md" },
  { title: "Transactions", source: "manual/sql/statements/transactions.md" },
  { title: "Databases & Security", source: "manual/sql/statements/databases-and-security.md" },
  { title: "Functions & Operators", source: "manual/sql/functions.md" },

  // Internals & Performance
  { title: "Query Planner & Fast Paths", source: "manual/internals/query-planner.md" },
  { title: "Zero-Allocation Rows", source: "manual/internals/zero-allocation-rows.md" },
  { title: "Compiled Scan Kernels", source: "manual/internals/compiled-scan-kernels.md" },
  { title: "DataVo vs SQLite", source: "manual/performance/datavo-vs-sqlite.md" },
  { title: "Benchmarks", source: "manual/performance/benchmarks.md" },
  { title: "Reproducing Benchmarks", source: "manual/performance/reproducing-benchmarks.md" },

  // Reference
  { title: "Configuration", source: "manual/reference/configuration.md" },
  { title: "Error Handling", source: "manual/reference/error-handling.md" },
  { title: "Limits & Unsupported Features", source: "manual/reference/limits.md" },
  { title: "AI Access & MCP", source: "manual/reference/ai-access.md" },

  // Engineering Blog
  { title: "Engineering Blog", source: "blog/index.md" },
  { title: "Part 1: Where DataVo Wins and Where It Loses", source: "blog/part-1-where-it-wins-and-where-it-loses.md" },
  { title: "Part 2: Packed Byte Buffers, Native AOT, and Roslyn Codegen", source: "blog/part-2-packed-buffers-aot-and-roslyn-codegen.md" },
];

function routeForPublicSource(source) {
  return `/${source.replace(/(^|\/)index\.md$/u, "").replace(/\.md$/u, "")}`;
}

function publicManualRoutes() {
  return new Set(publicManualPages.map((page) => routeForPublicSource(page.source)));
}

module.exports = {
  publicManualPages,
  publicManualRoutes,
  routeForPublicSource,
};
