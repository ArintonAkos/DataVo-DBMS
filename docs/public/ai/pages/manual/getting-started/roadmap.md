# Status & Roadmap

> Source route: /manual/getting-started/roadmap
> Source file: manual/getting-started/roadmap.md

The current release on NuGet is **0.5.1 (26 November 2026)**, targeting `.NET 8`, `.NET 9`, `.NET 10` and `netstandard2.1`. This page defines the release contract, subsystem status, architectural boundaries, and the milestone history.

## Release Contract

- **Supported shape:** One .NET process owns a database directory (enforced via `datavo.lock`) or an in-memory database, executing SQL or compiled LINQ through `DataVoContext`, ADO.NET, or the EF Core bridge.
- **Durability:** Under `StorageMode.Lsm` with `LsmStrictFsync = true` (default), acknowledged commits are flushed to stable storage. With `LsmStrictFsync = false`, writes hand off to the OS page cache. See [LSM & Durability](/manual/fundamentals/lsm-and-durability).
- **Isolation:** Explicit transactions provide MVCC snapshot isolation with read-your-own-writes and first-committer-wins conflict detection, but not serializable predicate locking (write skew is possible). See [Transactions & MVCC](/manual/fundamentals/transactions-and-mvcc).
- **On-disk format:** v0.5 writes row-keyed SSTables of format version 8 (version 6 when a table has no columnar segment) and keyed index SSTables of format version 5, with one engine-wide commit log (`commit.wal`). Earlier row-file versions (2 and 4), the uncompressed keyed version 3, the per-table WALs and the paged `Disk` engine are not read; pre-release data directories are not upgraded. The format does not yet carry a long-term cross-version compatibility guarantee; keep an export or seed script for critical data.
- **SQL `DATE` contract:** SQL `DATE` materializes as `DataVo.Core.DataVoDate` in row APIs (`CellValue.AsDate()`, compiled `GetDate`), with explicit `FromDateOnly` / `ToDateOnly` / `AsDateOnly` / `GetDateOnly` adapters for `System.DateOnly`.

## Subsystem Status

| Subsystem | Status | Notes |
| --- | --- | --- |
| `DataVoContext` & SQL dialect | Released (0.5.1) | DataVo's own dialect; see [SQL Compatibility](/manual/sql/compatibility) |
| `InMemory` & `Lsm` storage | Released (0.5.1) | The two storage modes (the paged `Disk` engine was removed in v0.5) |
| Transactions & MVCC | Released (0.5.1) | Snapshot isolation, savepoints, atomic `COMMIT` |
| Vector search (`HNSW`, `FLAT`) | Released (0.5.1) | Cosine (`<=>`) and L2 (`<->`), cost-based exact fallback |
| Columnar indexes & SSTable segments | Released (0.5.1) | In-memory buffers + persisted 4,096-row zone-mapped segments |
| Live Reactive Views (Z-set IVM) | Released (0.5.1) | Pull-drain subscriptions via `Subscribe` / `SubscribeZeroAlloc` |
| `DataVo.Generators` & Native AOT | Released (0.5.1) | Trim/AOT analyzers (`IL2026`, `IL3050`) enforced as errors |
| `DataVo.Data` (ADO.NET & Dapper) | Released (0.5.1) | Standard `System.Data.Common` types; no `DbProviderFactory` |
| `DataVo.EntityFrameworkCore` | Preview bridge | Query/DML bridge only; no migrations, excluded from AOT |
| `DataVo.Browser` (WebAssembly) | Experimental | Powers the docs playground; not published on npm |
| `DataVo-Sync` (PostgreSQL sync) | Commercial add-on | Separate proprietary product; see [DataVo-Sync](/manual/fundamentals/datavo-sync) |

## Explicit Boundaries

| Out of Scope / Boundary | Details |
| --- | --- |
| Multi-process directory access | Rejected by `datavo.lock`; multiple contexts in one process share one engine |
| Serializable isolation / phantom locks | No range or predicate locks |
| Network server / wire protocol | Embedded only (`DataVo.Server` PG wire bridge is planned, unscheduled) |
| Stored procedures, triggers, UDFs | Keep procedural logic in C# |
| Replication / high availability | Embedded engine; use [DataVo-Sync](/manual/fundamentals/datavo-sync) for PG sync |

## Milestone History & v0.5 Plan

### v0.2: Embedded Engine Baseline
- LSM storage with WAL, arena MemTables, background SSTable flushes/compaction, and clustered `INT`/`GUID` primary keys.
- Native AOT compliance (`IL2026`/`IL3050` as errors), zero-alloc `RowRef` scans, `VECTOR(n)` with `HNSW`/`FLAT`, and NuGet packaging.

### v0.3: Columnar Indexes & Compact Rows *(shipped in `0.4.0-rc.1`)*
- `[Columnar]` / `USING COLUMNAR` indexes with SIMD-accelerated single-table aggregates.
- Packed `FlatRowStore` in-memory layout (49.2 B/row in benchmarks, 52.1 ns PK lookup).

### v0.4: Columnar SSTables, Correctness & Typed Execution *(published as `0.4.0-rc.1`)*
- **Out-of-core columnar SSTables (format v8):** 4,096-row segments with NULL/NaN masks, min/max zone maps, and incremental shadow/delete vectors.
- **Oracle-verified correctness:** Fixed 5 cross-engine oracle bugs (`B1`–`B5`), `WHERE ... HAVING` without `GROUP BY`, `BIT` predicate binding, and culture-invariant `DOUBLE` lookups.
- **Typed execution:** `OR`/`LIKE` fast paths, integer-key hash joins, index-driven joins, dense bit-set anti-joins, unboxed `GROUP BY`, UTF-8 byte matching, and in-place top-k.
- **Vector search:** HNSW diversity heuristic on by default (100% recall@10 on clustered data), cost-based exact scan fallback, SQL `WITH (...)` HNSW parameters, and pre-optimized SIMD kernels.

### v0.5: AI Integration, Client Ergonomics & Native Interop *(released as `0.5.0`; current release `0.5.1`)*
- **Track A (Native Interop):** Native AOT C ABI (`dv_open`, `dv_exec`, `dv_prepare`) for C/Python/Rust/Go, zero-copy Apache Arrow C Data Interface export from columnar buffers/SSTables, and a reference Python binding.
- **Track B (Client Applications):**
  - *Done:* Build-time compiled `OrderBy`/`ThenBy`, `Skip`/`Take`, `Select` projections, and single-table `ExecuteUpdate`/`ExecuteDelete` in `DataVo.Generators`.
  - *Done:* Multi-target NuGet packages (`net8.0`, `net9.0`, `net10.0`, `netstandard2.1`), native `DATETIME`, `EXPLAIN` with the executed operator, a sharded LSM block cache, and typed reactive top-K/join views.
  - *Done:* [`DataVo.VectorData`](/manual/client-interfaces/vector-data): a `Microsoft.Extensions.VectorData` vector store with HNSW/FLAT indexes, LINQ filters translated to SQL, embedding generation and dependency injection; native `ReadOnlyMemory<float>` vectors in `[DataVoTable]` models (`[VectorDimensions(n)]`), parameters and `SearchNearest`.
  - *Done:* The paged `Disk` storage mode and pre-release format upgrades (v1 SSTables, per-table WAL migration) were removed: `InMemory` and `Lsm` are the two storage modes.
  - *Planned:* 2D geographic WGS84 points with haversine distance and spatial indexing.

## Unity Evaluation Status {#unity-evaluation-status}

Unity is an **evaluation target**, not a supported runtime. The public `DataVo.Core` package ships `net8.0`, `net9.0`, `net10.0` and `netstandard2.1` assets; the `netstandard2.1` asset is what Unity evaluation builds use.

::: details Unity Evaluation Matrix (Unity 6000.5.4f1)
| Target / Boundary | Status | Notes |
| --- | --- | --- |
| Unity 6000.5.4f1 Editor (macOS) | Narrow smoke pass | EditMode smoke suite passes on local portable candidate |
| macOS IL2CPP Player | Narrow smoke pass | Medium-stripping Player builds and passes type-binding + LSM reopen smoke |
| Windows x64 IL2CPP | Unverified | Requires separate Player proof |
| Direct Burst calls | Unsupported by design | Managed engine types cannot run inside Burst jobs |
:::

## See Also

- [Limits & Unsupported Features](/manual/reference/limits): Exact engine limits and unsupported SQL/API features.
- [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite): v0.4 benchmark findings and correctness notes.
- [LSM & Durability](/manual/fundamentals/lsm-and-durability): WAL fsync modes and crash recovery guarantees.
