const publicManualPages = [
  { title: "What Is DataVo?", source: "manual/preface/what-is-datavo.md" },
  { title: "v0.1 Alpha Scope", source: "manual/preface/alpha-scope.md" },
  { title: "Roadmap", source: "manual/preface/roadmap.md" },

  { title: "Quickstart", source: "manual/tutorial/quickstart.md" },
  { title: "Using DataVoContext", source: "manual/tutorial/datavo-context.md" },
  { title: "Entity Framework Example", source: "manual/tutorial/entity-framework-example.md" },
  { title: "Vector Search Example", source: "manual/tutorial/vector-search-example.md" },

  { title: "Supported SQL", source: "manual/sql-language/supported-sql.md" },
  { title: "CREATE TABLE", source: "manual/sql-language/create-table.md" },
  { title: "Data Manipulation", source: "manual/sql-language/data-manipulation.md" },
  { title: "Queries", source: "manual/sql-language/queries.md" },
  { title: "Vector Search and Indexes", source: "manual/sql-language/vector-search-syntax.md" },
  { title: "SQL Compatibility Matrix", source: "manual/sql-language/sql-compatibility.md" },

  { title: "Storage Modes", source: "manual/storage-engine/storage-modes.md" },
  { title: "LSM Mode", source: "manual/storage-engine/lsm-mode.md" },
  { title: "WAL And Durability", source: "manual/storage-engine/wal-and-durability.md" },
  { title: "Transactions and MVCC", source: "manual/storage-engine/transactions-acid-mvcc.md" },
  { title: "Query Planner And Fast Paths", source: "manual/storage-engine/query-planner-fast-paths.md" },

  { title: "Connecting to DataVo", source: "manual/client-interfaces/datavo-context-api.md" },
  { title: "Entity Framework Support", source: "manual/client-interfaces/entity-framework.md" },
  { title: "Roslyn Source Generators", source: "manual/client-interfaces/source-generators.md" },
  { title: "Native AOT", source: "manual/client-interfaces/native-aot.md" },

  { title: "Configuration Reference", source: "manual/reference/configuration.md" },
  { title: "Limits", source: "manual/reference/limits.md" },
  { title: "Unsupported Features", source: "manual/reference/unsupported-features.md" },
  { title: "Error Handling", source: "manual/reference/error-handling.md" },
  { title: "AI Access And MCP", source: "manual/reference/ai-access.md" },

  { title: "Benchmark Results", source: "manual/performance/benchmarks.md" },
  { title: "Benchmark Methodology", source: "manual/performance/methodology.md" },
  { title: "Reproducing Benchmarks", source: "manual/performance/reproducing-benchmarks.md" },
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
