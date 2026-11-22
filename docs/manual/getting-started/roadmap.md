---
title: Status & Roadmap
description: Release candidate guarantees, subsystem stability, explicit boundaries, and the v0.2–v0.5 roadmap for DataVo 0.4.0-rc.1.
---

# Status & Roadmap

The current release on NuGet is **0.4.0-rc.1 (release candidate, 17 November 2026)**, shipping both the **v0.3** and **v0.4** milestones on `.NET 10`. This page defines the release-candidate contract, subsystem status, architectural boundaries, and what is planned for **v0.5**.

## Release Candidate Contract

- **Supported shape:** One `.NET 10` process owns a database directory (enforced via `datavo.lock`) or an in-memory database, executing SQL or compiled LINQ through `DataVoContext`, ADO.NET, or the EF Core bridge.
- **Durability:** Under `StorageMode.Lsm` with `LsmStrictFsync = true` (default), acknowledged commits are flushed to stable storage. With `LsmStrictFsync = false`, writes hand off to the OS page cache. See [LSM & Durability](/manual/fundamentals/lsm-and-durability).
- **Isolation:** Explicit transactions provide MVCC snapshot isolation with read-your-own-writes and first-committer-wins conflict detection, but not serializable predicate locking (write skew is possible). See [Transactions & MVCC](/manual/fundamentals/transactions-and-mvcc).
- **On-disk format:** v0.4 writes SSTable format version 8 and still reads versions 2, 4, and 6, but does not yet carry a long-term cross-version compatibility guarantee. Keep an export or seed script for critical data.
- **SQL `DATE` contract:** SQL `DATE` materializes as `DataVo.Core.DataVoDate` in row APIs (`CellValue.AsDate()`, compiled `GetDate`), with explicit `FromDateOnly` / `ToDateOnly` / `AsDateOnly` / `GetDateOnly` adapters for `System.DateOnly`.

## Subsystem Status

| Subsystem | Status | Notes |
| --- | --- | --- |
| `DataVoContext` & SQL dialect | Release candidate | DataVo's own dialect; see [SQL Compatibility](/manual/sql/compatibility) |
| `InMemory` & `Lsm` storage | Release candidate | Active engines; `Disk` (paged) is legacy |
| Transactions & MVCC | Release candidate | Snapshot isolation, savepoints, atomic `COMMIT` |
| Vector search (`HNSW`, `FLAT`) | Release candidate | Cosine (`<=>`) and L2 (`<->`), cost-based exact fallback |
| Columnar indexes & SSTable segments | Release candidate | In-memory buffers + persisted 4,096-row zone-mapped segments |
| Live Reactive Views (Z-set IVM) | Release candidate | Pull-drain subscriptions via `Subscribe` / `SubscribeZeroAlloc` |
| `DataVo.Generators` & Native AOT | Release candidate | Trim/AOT analyzers (`IL2026`, `IL3050`) enforced as errors |
| `DataVo.Data` (ADO.NET & Dapper) | Release candidate | Standard `System.Data.Common` types; no `DbProviderFactory` |
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

### v0.2 — Embedded Engine Baseline
- LSM storage with WAL, arena MemTables, background SSTable flushes/compaction, and clustered `INT`/`GUID` primary keys.
- Native AOT compliance (`IL2026`/`IL3050` as errors), zero-alloc `RowRef` scans, `VECTOR(n)` with `HNSW`/`FLAT`, and NuGet packaging.

### v0.3 — Columnar Indexes & Compact Rows *(shipped in `0.4.0-rc.1`)*
- `[Columnar]` / `USING COLUMNAR` indexes with SIMD-accelerated single-table aggregates.
- Packed `FlatRowStore` in-memory layout (49.2 B/row in benchmarks, 52.1 ns PK lookup).

### v0.4 — Columnar SSTables, Correctness & Typed Execution *(published as `0.4.0-rc.1`)*
- **Out-of-core columnar SSTables (format v8):** 4,096-row segments with NULL/NaN masks, min/max zone maps, and incremental shadow/delete vectors.
- **Oracle-verified correctness:** Fixed 5 cross-engine oracle bugs (`B1`–`B5`), `WHERE ... HAVING` without `GROUP BY`, `BIT` predicate binding, and culture-invariant `DOUBLE` lookups.
- **Typed execution:** `OR`/`LIKE` fast paths, integer-key hash joins, index-driven joins, dense bit-set anti-joins, unboxed `GROUP BY`, UTF-8 byte matching, and in-place top-k.
- **Vector search:** HNSW diversity heuristic on by default (100% recall@10 on clustered data), cost-based exact scan fallback, SQL `WITH (...)` HNSW parameters, and pre-optimized SIMD kernels.

### v0.5 — Native Interop & Client Ergonomics *(planned)*
- **Track A (Native Interop):** Native AOT C ABI (`dv_open`, `dv_exec`, `dv_prepare`) for C/Python/Rust/Go, zero-copy Apache Arrow C Data Interface export from columnar buffers/SSTables, and a reference Python binding.
- **Track B (Client Applications):**
  - *Done:* Build-time compiled `OrderBy`/`ThenBy`, `Skip`/`Take`, `Select` projections, and single-table `ExecuteUpdate`/`ExecuteDelete` in `DataVo.Generators`.
  - *Planned:* 2D geographic WGS84 points with haversine distance and spatial indexing, plus multi-target NuGet packages (`net8.0`, `net9.0`, `net10.0`, `netstandard2.1`).

## Unity Evaluation Status {#unity-evaluation-status}

Unity is an **evaluation target**, not a supported runtime. The public `DataVo.Core` package ships `net10.0` only; `DataVoEnablePortableTarget=true` produces an experimental local `netstandard2.1` candidate.

::: details Unity Evaluation Matrix (Unity 6000.5.4f1)
| Target / Boundary | Status | Notes |
| --- | --- | --- |
| Unity 6000.5.4f1 Editor (macOS) | Narrow smoke pass | EditMode smoke suite passes on local portable candidate |
| macOS IL2CPP Player | Narrow smoke pass | Medium-stripping Player builds and passes type-binding + Disk/LSM reopen smoke |
| Windows x64 IL2CPP | Unverified | Requires separate Player proof |
| Direct Burst calls | Unsupported by design | Managed engine types cannot run inside Burst jobs |
:::

## See Also

- [Limits & Unsupported Features](/manual/reference/limits) — Exact engine limits and unsupported SQL/API features.
- [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite) — v0.4 benchmark findings and correctness notes.
- [LSM & Durability](/manual/fundamentals/lsm-and-durability) — WAL fsync modes and crash recovery guarantees.
