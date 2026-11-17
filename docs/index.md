---
layout: home

hero:
  name: "DataVo"
  text: "A C#-native embedded database engine"
  tagline: "In-process SQL, LSM storage, columnar indexes and vector search for .NET 10. This manual describes the v0.4 line, published on NuGet as 0.4.0-rc.1."
  actions:
    - theme: brand
      text: Get started
      link: /manual/tutorial/quickstart
    - theme: alt
      text: DataVo vs SQLite
      link: /manual/performance/datavo-vs-sqlite
    - theme: alt
      text: SQL reference
      link: /manual/sql-language/supported-sql

features:
  - title: Typed execution paths
    details: Filters, grouping, top-k and INNER/LEFT joins on integer keys read cells straight from stored rows. A typed path returns exactly what the general evaluator would, or declines.
    link: /manual/storage-engine/query-planner-fast-paths
  - title: LSM storage with columnar SSTables
    details: A commit log with optional per-commit fsync, arena MemTables, clustered primary keys, memory-mapped SSTables, and columnar segments with per-chunk zone maps that are read out of core.
    link: /manual/storage-engine/lsm-mode
  - title: Vector search in SQL
    details: VECTOR(n) columns with FLAT and HNSW indexes. Filtered and joined searches test the filter on stored rows and switch to an exact scan when that is cheaper than the graph.
    link: /manual/sql-language/vector-search-syntax
  - title: Compiled LINQ without EF Core
    details: DataVo.Generators turns DataVoTable<T> lambdas and [DataVoQuery] methods into build-time code. No Entity Framework dependency, Native AOT clean.
    link: /manual/client-interfaces/source-generators
---

DataVo is an embedded database engine written in C# that runs inside a .NET 10 process. There is no server and no native library: storage, SQL execution and vector indexes are managed code, built with the trim and Native AOT analyzers as errors.

Two storage engines are under active development. **InMemory** packs rows into contiguous fixed-width cells. **LSM** appends each commit to a log (flushed to stable storage before the commit is acknowledged, unless you choose relaxed durability), applies it to an arena-backed MemTable, flushes 32 MB MemTables to immutable SSTables in the background, and compacts them. Tables keyed by one `INT` or `GUID` are clustered by that key; secondary indexes are themselves keyed SSTables committed with their rows; and a columnar index is written into the SSTables as 4,096-row segments with min/max zone maps, so analytical scans skip chunks and nothing is rebuilt on open.

On one machine with warm caches, the v0.4 engine is faster than SQLite on key lookups and ranges, low-cardinality `GROUP BY`, integer-key joins, columnar aggregates and filtered vector search, and slower on index-ordered `GROUP BY`, unindexed top-k and `OR`/`LIKE` full scans. DuckDB is far faster than both on whole-table analytics. Every number is checked against an oracle before it is timed.

DataVo is not a client/server database: one process owns a database directory, there is no wire protocol or replication, and transactions are snapshot-isolated rather than serializable.

## What to read first

- [What Is DataVo?](./manual/preface/what-is-datavo.md): the architecture in one page, and which version this manual describes.
- [Quickstart](./manual/tutorial/quickstart.md): create a database, table, row, and query from C#.
- [DataVo vs SQLite](./manual/performance/datavo-vs-sqlite.md): where each engine wins on 28 SQL shapes and five filtered vector queries, and why.
- [Query Planner And Fast Paths](./manual/storage-engine/query-planner-fast-paths.md): which query shapes run on typed paths.
- [Release Scope & Boundaries](./manual/preface/alpha-scope.md): durability, isolation and what is out of scope.
- [Roadmap](./manual/preface/roadmap.md): what v0.3 and v0.4 added, and the v0.5 plan.
