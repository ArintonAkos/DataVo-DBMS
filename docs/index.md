---
layout: home

hero:
  name: "DataVo"
  text: "A C#-native embedded database engine"
  tagline: "v0.1 Alpha documentation for allocation-aware SQL, LSM storage, vector search, and local-first .NET workloads."
  actions:
    - theme: brand
      text: Get started
      link: /manual/tutorial/quickstart
    - theme: alt
      text: Read benchmarks
      link: /manual/performance/benchmarks
    - theme: alt
      text: SQL reference
      link: /manual/sql-language/supported-sql

features:
  - title: Native AOT compatible core
    details: DataVo.Core and DataVo.Data enable trim/AOT analyzers and treat IL2026/IL3050-family diagnostics as errors, keeping the engine aligned with Native AOT constraints.
    link: /manual/client-interfaces/native-aot
  - title: Zero-allocation hot paths
    details: Typed rows, arena-backed LSM MemTables, compiled access paths, and binary WAL frames avoid dictionary materialization on selected internal write and read paths.
    link: /manual/storage-engine/query-planner-fast-paths
  - title: Roslyn compiled queries
    details: The DataVo.Generators package emits compiled query plans, typed row mappers, and optional schema-manifest index hints for supported SELECT, INSERT, and UPDATE shapes.
    link: /manual/client-interfaces/source-generators
  - title: Vector search
    details: Store VECTOR(n) columns, build HNSW or FLAT vector indexes, and rank by cosine (<=>) or L2 (<->) distance directly in DataVo SQL.
    link: /manual/sql-language/vector-search-syntax
---

DataVo v0.1 Alpha is an attempt to build an embedded database engine from inside the .NET runtime instead of behind a native provider boundary. It is written in C#, runs in-process, and is designed around `Span<T>`, source-generated serialization, Native AOT constraints, typed row paths, SIMD kernels, and GC-aware execution. In the checked-in benchmark artifacts, DataVo reaches roughly 1.2M+ ops/sec in relaxed LSM thread-scaling workloads, with near-zero or sharply reduced allocation on the hottest internal paths.

This is not a full RDBMS and it is not a mature SQLite replacement. The interesting part of DataVo is narrower: C#-native embedded storage for local-first apps, game tooling, simulations, browser/WASM workflows, reactive query maintenance, and vector search experiments. The current engine supports SQL-style DDL/DML/querying, disk and in-memory modes, WAL/MVCC work, vector columns/indexing, reactive subscriptions, an ADO.NET-facing package, and early EF integration, but the public packaging and production-hardening story is still alpha.

DataVo is being opened alongside the whitepaper because the design tradeoffs are worth discussing in public: how much can a database gain by knowing it lives inside .NET, avoiding avoidable GC pressure, and binding hot paths earlier than a generic provider can? The project is best suited today for early users who like embedded systems, storage engines, .NET performance work, and honest alpha software.

## What to read first

- [Quickstart](./manual/tutorial/quickstart.md): create a database, table, row, and query from C#.
- [v0.1 Alpha Scope](./manual/preface/alpha-scope.md): understand what is launch-ready and what is still planned.
- [SQL Compatibility Matrix](./manual/sql-language/sql-compatibility.md): see the public SQL support boundary.
- [MVCC, Transactions, And ACID](./manual/storage-engine/transactions-acid-mvcc.md): review the exact isolation and durability setting.
- [Entity Framework Support](./manual/client-interfaces/entity-framework.md): see supported EF workflows and unsupported provider features.
- [Benchmark Results](./manual/performance/benchmarks.md): review selected whitepaper results and the workload caveats.
