---
layout: home

hero:
  name: "DataVo"
  text: "C#-native embedded database"
  tagline: "In-process SQL, LSM storage, vector search, and live Z-set reactive views for .NET 10. Documentation for v0.4 (0.4.0-rc.1)."
  actions:
    - theme: brand
      text: Quickstart
      link: /manual/getting-started/quickstart
    - theme: alt
      text: Introduction
      link: /manual/getting-started/introduction
    - theme: alt
      text: SQL Reference
      link: /manual/sql/compatibility

features:
  - title: Typed Execution & Fast Paths
    details: Filters, grouping, top-k, and integer-key joins read cells directly from stored bytes without per-row dictionary allocations.
    link: /manual/internals/query-planner
  - title: LSM Storage & Columnar SSTables
    details: Crash-safe WAL with strict fsync, clustered primary keys, memory-mapped SSTables, and 4,096-row columnar segments with zone maps.
    link: /manual/fundamentals/lsm-and-durability
  - title: Vector Search & Reactive Views
    details: Native VECTOR(n) columns with FLAT and HNSW indexes plus pull-drain Z-set incremental view maintenance over SQL and LINQ.
    link: /manual/fundamentals/vector-search
  - title: Roslyn AOT & Compiled LINQ
    details: DataVo.Generators compiles DataVoTable<T> lambdas and [DataVoQuery] methods at build time, with no EF Core dependency and full Native AOT support.
    link: /manual/client-interfaces/source-generators
---

DataVo is a managed C# embedded database engine that runs directly inside your `.NET 10` process with no external server, no native DLLs, and full Native AOT compatibility. Choose between packed **InMemory** tables and crash-recoverable **LSM** storage, query via SQL, compiled LINQ, ADO.NET, or Dapper, and optionally pair with the commercial **DataVo-Sync** add-on for offline-first PostgreSQL synchronization.

## Start Here

- [Introduction](/manual/getting-started/introduction): Architecture overview and choose-your-path guide.
- [Quickstart](/manual/getting-started/quickstart): Create a database, insert rows, and query in 60 seconds.
- [Choosing an Interface](/manual/getting-started/choosing-an-interface): Compare `DataVoContext`, Compiled LINQ, ADO.NET, Dapper, and EF Core.
- [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite): Oracle-verified benchmarks across 28 SQL shapes and filtered vector search.
- [Status & Roadmap](/manual/getting-started/roadmap): Release candidate scope, durability guarantees, and the v0.5 plan.

## DataVo-Sync (Commercial Add-On)

For teams that need offline-first PostgreSQL or Supabase synchronization or in-process read replicas (with SQL Server support available on request), [DataVo-Sync](/manual/fundamentals/datavo-sync) is available separately. You can [request Early Access](https://tally.so/r/RWD8G9) or email `datavodb@proton.me`.
