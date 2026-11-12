---
layout: home

hero:
  name: "DataVo"
  text: "A C#-native embedded database engine"
  tagline: "v0.2.0-rc.3 Release Candidate documentation for allocation-aware SQL, LSM storage, vector search, and local-first .NET 10 workloads."
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
    details: Typed rows, arena-backed LSM MemTables, compiled access paths, and zero-alloc in-memory point lookups (56 B, 405 ns) avoid dictionary materialization on hot internal paths.
    link: /manual/storage-engine/query-planner-fast-paths
  - title: Roslyn compiled queries
    details: The DataVo.Generators package emits compiled query plans, typed row mappers, and optional schema-manifest index hints for supported SELECT, INSERT, and UPDATE shapes.
    link: /manual/client-interfaces/source-generators
  - title: Vector search
    details: Store VECTOR(n) columns, build HNSW or FLAT vector indexes, and rank by cosine (<=>) or L2 (<->) distance directly in DataVo SQL.
    link: /manual/sql-language/vector-search-syntax
---

DataVo **v0.2.0-rc.3 (Release Candidate)** is an embedded database engine built directly inside the .NET 10 runtime instead of behind a native provider boundary. Written in C# and backed by 2,358 passing tests, it runs in-process and is engineered around `Span<T>`, source-generated serialization, locked Native AOT constraints, typed row paths, SIMD kernels, and GC-aware execution.

DataVo is not a full client/server RDBMS or a drop-in PostgreSQL replacement. Its scope is focused: C#-native embedded storage for local-first .NET 10 applications, game tooling, simulations, reactive query maintenance, and SIMD vector search. In v0.2.0-rc.3, the core embedded API (`DataVoContext`), strict-fsync LSM storage, ADO.NET provider (`DataVo.Data`), Roslyn compiled queries (`DataVo.Generators`), zero-alloc in-memory point lookups, and vector indexing (`HNSW` and `FLAT`) are at Release Candidate status, while active **v0.3** development targets Columnar Indexes for HTAP, compact in-memory row layouts, and a future PostgreSQL-wire tooling bridge (`DataVo.Server`).

## What to read first

- [Quickstart](./manual/tutorial/quickstart.md): create a database, table, row, and query from C#.
- [Release Scope & Boundaries](./manual/preface/alpha-scope.md): review the v0.2.0-rc.3 Release Candidate contract and explicit architectural boundaries.
- [SQL Compatibility Matrix](./manual/sql-language/sql-compatibility.md): see the public SQL support boundary.
- [MVCC, Transactions, And ACID](./manual/storage-engine/transactions-acid-mvcc.md): review the exact isolation and durability setting.
- [Entity Framework Support](./manual/client-interfaces/entity-framework.md): see supported EF workflows and unsupported provider features.
- [Benchmark Results](./manual/performance/benchmarks.md): review v0.2.0-rc.3 benchmark measurements and workload caveats.
