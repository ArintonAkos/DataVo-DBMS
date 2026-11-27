# Changelog

## 0.5.1

- **Concurrency fixes:** a single-row `INSERT` takes the table intent lock plus key locks instead of the table write lock; `UPDATE` reserves new primary and unique key values before checking them; `CREATE INDEX`, `DROP INDEX`, `DROP TABLE` and `VACUUM` take the exclusive table lock; lock-free primary-key point reads follow a moved row.
- The fixed-width compiled `UPDATE` fast path also runs in `StorageMode.InMemory`.
- Inside a transaction, a child `INSERT` sees a parent row inserted earlier in the same transaction when checking its foreign key.

## 0.5.0

- **Breaking:** `StorageMode.Disk` (the paged disk engine) and pre-release on-disk format upgrades were removed. Use `StorageMode.Lsm` for save data and `StorageMode.InMemory` for transient state.
- Native `DATETIME`, `EXPLAIN` with the executed operator, a sharded LSM block cache, typed reactive top-K and join views.
- `ReadOnlyMemory<float>` vectors (`[VectorDimensions]` models, parameters, `SearchNearest`), NULL keys held by LSM indexes, and ordinal text comparison on every path.

## 0.4.0-rc.1

- **Columnar Indexes & Out-of-Core SSTables (v0.3–v0.4):** `[Columnar]` / `CREATE INDEX ... USING COLUMNAR` contiguous buffers and persistent format-v8 SSTable segments with per-chunk min/max zone maps and delete vectors.
- **Compact In-Memory Storage:** `FlatRowStore` packs rows into fixed-width contiguous buffers (`PackedKeyReader<T>`) with sorted key ranges.
- **Typed SQL Execution & Vector Search:** Typed scans (`OR`, `LIKE`, UTF-8 byte matching, bounded top-k), multi-table `INNER`/`LEFT` hash joins, unboxed `GROUP BY`, diversity-heuristic `HNSW` defaults, SQL `WITH (...)` graph options, cost-based filtered exact-scan routing, eager-optimized SIMD distance kernels, and `WHERE ... HAVING` / `BIT` / culture fixes.

## 0.2.0-rc.3

- **Zero-Copy `RowRef` & `StreamRef`:** Stream table rows with 0 bytes allocated on the managed heap across 50,000+ rows for 60/120 FPS game simulation loops.
- **Roslyn Compile-Time LINQ Interceptors:** `174 ns` point lookups and streaming `Where` queries with flat heap growth.
- **3D Spatial & Vector3 API:** `WithinRadius`, `WithinBoundingBox`, and `NearestNeighbors` on typed tables with pure SIMD `System.Numerics`.
- **HNSW Vector Indexing:** Multi-threaded parallel HNSW graph construction with 100% graph connectivity and reachability guarantees.

## 0.2.0-rc.2

- The source generator is included: mark a model with `[DataVoTable]` and Unity writes its data access when it compiles.
- New `BIGINT` and `DOUBLE` columns hold real 64-bit values.
- **Breaking:** `CREATE TABLE` and `ALTER TABLE` now reject a type DataVo cannot store. Before, `DECIMAL` or `LONG` silently became text and `DATETIME` silently lost its time.

## 0.2.0-rc.1

- `DataVoContext.StreamQuery` reads large tables without holding the whole result in memory.
- A `SELECT` inside a transaction sees that transaction's own writes.
- `SELECT` lists can contain constants, with or without `FROM`.

## 0.1.0-preview.1

- First installable preview of DataVo Core for Unity.
- Disk and LSM storage for save data.
