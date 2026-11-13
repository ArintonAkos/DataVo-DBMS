# Roadmap

DataVo's roadmap separates completed **v0.2 Release Candidate (`v0.2.0-rc.3`)** milestones from active **v0.3 HTAP & Tooling** engineering. The core design principle remains unchanged: predictable in-process storage, explicit durability contracts, locked Native AOT compliance, and allocation-aware execution paths.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./app_data",
    LsmStrictFsync = true
});
```

## Completed v0.2 Milestones (`v0.2.0-rc.3`)

The v0.2 release line has completed its foundational embedded engine milestones and is backed by 2,358 passing tests:

- **LSM Storage Stability & Clustered Keys:** WAL-backed arena MemTables, background SSTable flushes, strict `fsync` durability (`LsmStrictFsync = true`), and clustered `INT`/`GUID` primary keys that halve on-disk footprint.
- **Locked Native AOT:** `DataVo.Core` and `DataVo.Data` enforce trim and AOT analyzers (`IL2026`/`IL3050`) as build errors.
- **Zero-Allocation In-Memory Fast Path:** Primary-key point lookups bypass SQL parsing and dictionary materialization (`56 B` ADO.NET reader wrapper, ~`405 ns`), alongside zero-allocation `StreamRef()` / `RowRef` table scans.
- **SIMD Vector Search:** Built-in `VECTOR(n)` columns (`VEC2`, `VEC3`, `VEC4` shorthands), `HNSW` and `FLAT` indexes, and SIMD-accelerated cosine (`<=>`) and Euclidean/L2 (`<->`) distance operators.
- **NuGet Release Candidate Packaging:** `DataVo.Core`, `DataVo.Data`, `DataVo.EntityFrameworkCore`, and `DataVo.Generators` are published to nuget.org via CI Trusted Publishing (`--prerelease`).

```bash
dotnet add package DataVo.Core --prerelease
dotnet add package DataVo.Data --prerelease
dotnet add package DataVo.EntityFrameworkCore --prerelease
dotnet add package DataVo.Generators --prerelease
```

## Active v0.3 Roadmap: HTAP, Compact Rows & Wire Tooling

Active engineering for **v0.3** focuses on closing the analytical scan gap with columnar engines while preserving DataVo's sub-microsecond OLTP point lookups:

### 1. Columnar Indexes for HTAP (Hybrid Transactional/Analytical Processing)

While DataVo v0.2 leads SQLite on compiled in-memory aggregates and OLTP point lookups, full analytical table scans over 1M+ rows remain faster on pure column stores like DuckDB. In v0.3, DataVo is introducing **Columnar Indexes** so tables can maintain synchronized columnar projections alongside row storage, enabling SIMD-vectorized analytical scans, filtered aggregations, and `GROUP BY` pipelines directly on contiguous column buffers.

### 2. Compact In-Memory Row Layout

v0.3 overhauls the in-memory row representation into a dense, contiguous layout that eliminates per-row object header overhead, reduces memory footprint on multi-million-row tables, and improves CPU cache-line utilization during scans and index probes.

### 3. PostgreSQL-Wire Tooling Bridge (`DataVo.Server`)

Developers need to inspect embedded DataVo databases from standard GUI clients such as DBeaver, DataGrip, and pgAdmin. `DataVo.Server` is planned as an optional companion host implementing a subset of the PostgreSQL wire protocol and catalog views (`pg_catalog`) over an existing DataVo storage directory.

```text
Application process (OLTP + HTAP)
  -> DataVoContext / DataVoConnection
  -> Row store + v0.3 Columnar Indexes
  -> Embedded LSM / In-Memory storage

Future tooling process (v0.3+)
  -> DataVo.Server
  -> PostgreSQL wire protocol subset
  -> Embedded LSM storage
```

## Completed v0.4 Milestone (On Branch): On-Disk Columnar SSTables

### On-Disk Columnar SSTable Storage (Out-of-core Parquet-style chunks + Zone Maps)

In v0.3 a columnar index on an LSM table was derived state held entirely in memory: it had to be rebuilt from the table's rows on first use after reopen, which required RAM proportional to the full indexed column dataset.

In v0.4 (`feature/on-disk-columnar-sstables`), format-version-8 SSTables persist 4,096-row aligned columnar segments directly inside `.sst` files during flush and compaction (without altering row blocks or `~223–236 ns` point-lookup performance), alongside tombstone key runs and per-chunk min/max/NULL/NaN zone maps:

- **Zero Rebuild & Out-of-Core Execution:** Only unflushed MemTable rows (the "tail") stay in RAM; flushed rows stream directly from memory-mapped or positional SSTable segments in 4,096-row chunks. Opening and answering the first `SUM` over 1,000,000 flushed rows takes `13.7 ms` (`186 KB` allocated) in OS cache, and warm analytical scans run at `135.7–460.8 µs` (`0 B` allocated/query).
- **Zone-Map Chunk Pruning:** Before loading a chunk group, scans evaluate the predicate over the segment directory's per-chunk `min`/`max` and `NULL`/`NaN` flags, skipping non-matching chunks without page faults or reads.

## Ecosystem & Platform Evaluation Lanes

- **Unity Integration (Evaluation Target):** A narrow managed, in-memory candidate has passed a pinned Unity 6000.5.4f1 Editor and macOS IL2CPP Player smoke. Direct Burst calls are unsupported by design; Windows IL2CPP validation and broader game-save durability proofs remain separate evaluation gates.
- **WebAssembly (`Experimental / Playground Lane`):** `DataVo.Browser` (`docs/src/DataVoClient.ts`) powers the interactive documentation playground, but is **not** a production target and is **not** published on npm.

## Roadmap Summary

| Milestone / Feature | Status | Target / Notes |
| --- | --- | --- |
| Core embedded API (`DataVoContext`) & ADO.NET (`DataVo.Data`) | Completed (`v0.2.0-rc.3`) | Primary in-process interfaces on .NET 10. |
| LSM storage stability & clustered PKs | Completed (`v0.2.0-rc.3`) | Strict WAL `fsync`, SSTables, and clustered `INT`/`GUID` keys. |
| Native AOT lock (`DataVo.Core` + `DataVo.Data`) | Completed (`v0.2.0-rc.3`) | Trim and AOT analyzers enforced as build errors. |
| Zero-alloc in-memory point lookup & `RowRef` streaming | Completed (`v0.2.0-rc.3`) | `56 B`, ~`405 ns` PK fast path; 0 B heap allocation on `StreamRef()`. |
| SIMD vector search (`HNSW` & `FLAT`) | Completed (`v0.2.0-rc.3`) | Cosine (`<=>`) and L2 (`<->`) distance operators. |
| NuGet prerelease packages | Completed (`v0.2.0-rc.3`) | Published to nuget.org via CI Trusted Publishing. |
| Columnar Indexes for HTAP | Completed (`v0.3`) | Synchronized columnar buffers on In-Memory and LSM tables; beats DuckDB by up to 8.9× on covered aggregates. |
| Compact in-memory row layout | Completed (`v0.3`) | `FlatRowStore` + direct `PackedKeyReader<T>`: 13× lower row-store memory (`49.2 B/row`) and `52.1 ns` PK lookup. |
| On-disk columnar SSTable storage (out-of-core chunks + zone-map pruning) | Completed (`v0.4` dev) | Format-v8 SSTable columnar segments, tail-only RAM, zone-map chunk skipping, zero rebuild on open (`13.7 ms` open + `SUM` on 1M rows). |
| PostgreSQL-wire tooling bridge (`DataVo.Server`) | Planned (`v0.3+`) | Connect DBeaver, DataGrip, and pgAdmin to DataVo storage. |
| Long-term on-disk format stability guarantee | Planned | Requires cross-version compatibility suite and migration tooling. |
| Unity managed integration proof | Evaluation Target | Narrow Unity 6000.5.4f1 Editor and macOS IL2CPP smoke passed; Windows x64 unverified. |
| Browser / WebAssembly (`DataVo.Browser`) | `Experimental / Playground Lane` | Interactive docs playground only; not production-ready and not published on npm. |
| EF Core migrations | Not Supported | `DataVo.EntityFrameworkCore` is a query/DML bridge, not a migration provider. |
| Multi-node replication & high availability | Not Supported | Out of scope for the embedded engine. |
