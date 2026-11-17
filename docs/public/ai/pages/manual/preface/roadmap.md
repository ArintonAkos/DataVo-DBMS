# Roadmap

> Source route: /manual/preface/roadmap
> Source file: manual/preface/roadmap.md

DataVo's milestones are numbered by what they change in the engine. The latest published NuGet packages are **0.4.0-rc.1**, which ship both **v0.3** and **v0.4**; **v0.5** is planned next. The principles behind every milestone are the same: one process owns its data, durability is an explicit setting with a stated contract, `DataVo.Core` stays Native AOT clean, and a fast path either returns exactly what the general path would or declines.

## v0.2: the embedded engine (first 0.2.0-rc.3 line)

- **LSM storage.** A commit log, arena-backed MemTables, background SSTable flushes and compaction. `LsmStrictFsync = true` (the default) acknowledges a commit only after the log is on stable storage. Tables whose primary key is one `INT` or `GUID` column are clustered: rows are stored under their key, which roughly halves the footprint of an integer-keyed table compared with a separate primary-key index.
- **Native AOT.** `DataVo.Core` and `DataVo.Data` build with the trim and AOT analyzers (`IL2026`, `IL3050`) as errors.
- **Allocation-free reads.** In-memory primary-key lookups that skip SQL parsing and row dictionaries, and `StreamRef()`/`RowRef` table scans that allocate nothing.
- **Vector search.** `VECTOR(n)` columns, `HNSW` and `FLAT` indexes, cosine (`<=>`) and Euclidean (`<->`) distance with SIMD kernels.
- **Packaging.** `DataVo.Core`, `DataVo.Data`, `DataVo.EntityFrameworkCore` and `DataVo.Generators` on nuget.org as prereleases, published from CI with NuGet Trusted Publishing.

## v0.3: columnar indexes and compact rows (shipped in 0.4.0-rc.1)

- **Columnar indexes** (`[Columnar]` on a typed model, `CREATE INDEX ... USING COLUMNAR` in SQL). The marked columns are kept a second time as contiguous column buffers, and single-table `COUNT`, `SUM`, `AVG`, `MIN` and `MAX` with numeric `AND`/`OR` filters are answered from those buffers with SIMD kernels instead of from rows.
- **Compact in-memory rows.** `FlatRowStore` packs rows into fixed-width cells in contiguous buffers, read through `PackedKeyReader<T>`: 49.2 bytes per row in the measured table, 13 times less than the previous object-per-row layout, and a 52.1 ns primary-key lookup.

## v0.4: out-of-core columns, SQL and vector correctness, typed execution (published as 0.4.0-rc.1)

v0.4 makes the LSM engine's columnar data persistent, fixes every wrong answer a cross-engine oracle found, and moves the common SQL shapes onto typed paths. Its final measurements are in [`2026-11-16-v04-final/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-16-v04-final) and explained in [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite).

**Storage**

- **Columnar SSTables (format version 8).** Flush and compaction write the columnar index's columns into each `.sst` file as 4,096-row segments, with NULL and NaN masks, runs of deleted keys, and a min/max zone map per chunk. Only the unflushed tail of a table is held in memory, so opening a database does not rebuild the index, and a scan skips any chunk whose zone map rules out the predicate. Compaction merges segments as a stream instead of materializing them.
- **Shadow masks and delete vectors.** A row in an SSTable segment can be replaced or deleted by a newer file or by the unflushed tail. Each SSTable keeps a delete vector, a bit per row for the chunks that have such rows, built for one set of live files and extended incrementally when the set changes; the rows the tail shadows are marked per scan in a reused mask that grows as the tail grows. A columnar scan therefore never counts a replaced or deleted row.

**Correctness.** A benchmark that checks every answer against an oracle before timing it found five queries on which DataVo returned a wrong result. All five are fixed, with regression tests on both active engines; [Correctness findings](/manual/performance/datavo-vs-sqlite#correctness-findings) describes each.

- `B1`: a `WHERE` condition on the NULL-supplying side of a `LEFT JOIN` was applied before the join.
- `B2`: `DISTINCT` inside an aggregate (`COUNT(DISTINCT x)`) was dropped by the parser.
- `B3`: a filtered vector search over aliased tables fetched too few candidates.
- `B4`: `AND`/`OR` across two joined tables intersected row keys listed in different table orders.
- `B5`: a vector search joined to another table dropped the joined table's filter.
- `WHERE ... HAVING` without `GROUP BY` failed to parse; `HAVING` now ends the `WHERE` condition.
- A `BIT` column could not be filtered: `WHERE IsActive = true` failed to bind, and `= 1` or `= 'true'` silently matched no row. `TRUE`/`FALSE`, `1`/`0` and `'true'`/`'false'` now all compare as booleans.
- On the legacy Disk engine, an indexed `DOUBLE` equality found no row under a culture whose decimal separator is a comma, because the lookup key was formatted in that culture.

**Typed SQL execution**

- `OR` and `LIKE`/`NOT LIKE` on the typed single-table scan and on columnar indexes.
- A typed hash join for `INNER` and `LEFT` joins on integer keys over any number of tables, aggregated or returning rows, with `ORDER BY ... LIMIT` kept to a bounded top-N.
- Index-driven joins: when a selective dimension names few rows of a large table, the large table is read through its index on the join column.
- A joined table that contributes only its key (an anti-join such as `LEFT JOIN ... WHERE c.Id IS NULL`) is kept as a dense bit set when its keys are distinct and closely spaced, instead of a hash table.
- `GROUP BY` without boxing its keys, with `HAVING`, `ORDER BY` and `LIMIT` applied to the groups before any group becomes a row.
- On the LSM engine, text equality and `LIKE` are tested against the stored UTF-8 bytes rather than a decoded string per row.
- `ORDER BY ... LIMIT k` without an index compares sort keys in place and copies only rows that enter the top-k.
- Primary-key ranges on the InMemory engine are read from the key map's sorted key copy.

**Vector search**

- HNSW's neighbour-selection diversity heuristic is on by default, and connectivity repair no longer drops the last edge into a cluster. On strongly clustered embeddings this raised recall@10 from 82–94% to 100%.
- HNSW answers a search by an exact scan when the candidate count a filter requires makes the graph walk more expensive. The scan is exact, so recall can only rise.
- A filtered vector search tests the `WHERE` conditions on the stored candidate rows and builds only the rows that pass; over a join, candidates go through the typed join and joined tables are read by key.
- `CREATE INDEX ... USING HNSW WITH (m, ef_construction, ef_search, enable_diversity_heuristic)` sets the graph parameters from SQL and stores them in the catalog.
- The SIMD distance kernels and the Flat scan loop are compiled fully optimized on first use. Under tiered compilation the first searches of a process, sometimes a whole run, used unoptimized kernel code: 11–14 ms instead of about 2 ms for an exact search over 50,000 × 384 vectors.

## v0.5: interop and client-side ergonomics (planned)

v0.5 has two tracks. Neither is started; the scope below is the plan, not a commitment to dates.

**Track A: native interop.**

- A C ABI exported from a Native AOT build of the engine (`dv_open`, `dv_close`, `dv_exec`, `dv_prepare`, parameter binding, `dv_exec_prepared`, and a direct single-row read), so that C, Python, Rust or Go can embed DataVo without .NET.
- Zero-copy export of columnar data through the Apache Arrow C Data Interface (`ArrowArray`, `ArrowSchema`), from in-memory column buffers and from SSTable segments, for Polars, PyArrow, DuckDB and similar consumers.
- A reference Python binding with an end-to-end test and benchmark.

**Track B: client applications.**

- More of LINQ compiled at build time by `DataVo.Generators`, still with no Entity Framework dependency: `OrderBy`/`OrderByDescending`/`ThenBy`, `Take`/`Skip`, and `Select` projections that read only the projected columns, plus compiled single-table updates and deletes. Today `DataVoTable<T>` compiles `Where`, `Count`, `Any`, `Sum`, `Min`, `Max`, `FirstOrDefault`, `SingleOrDefault` and `ToList`.
- Two-dimensional geographic data: WGS84 latitude/longitude points with spherical (haversine) distance, bounding boxes that cross the ±180° meridian, and a 2D spatial index (R-tree, quadtree or a Morton-ordered B-tree) so that radius and box queries over a million points do not scan the table. The current spatial support is three-dimensional Cartesian (`Vector3`) and scans.
- Multi-target packages (`net8.0`, `net9.0`, `net10.0`, and `netstandard2.1` where the code allows), so that MAUI, Avalonia, WPF, WinUI, Unity and Blazor WebAssembly applications are not blocked by a `net10.0`-only asset, and a guide for migrating client applications from `Microsoft.Data.Sqlite` and EF Core.

## Later, and out of scope

| Item | Status | Notes |
| --- | --- | --- |
| PostgreSQL wire protocol bridge (`DataVo.Server`) | Planned, unscheduled | A companion process speaking a subset of the PostgreSQL protocol so that DBeaver, DataGrip or pgAdmin can inspect a database directory. |
| Long-term on-disk format guarantee | Planned | Needs a cross-version compatibility suite and migration tooling. Until then, keep a way to rebuild important data. |
| Unity runtime support | Evaluation target | A narrow Editor and macOS IL2CPP smoke passes on Unity 6000.5.4f1; Windows IL2CPP is unverified and Burst is unsupported by design. |
| Browser / WebAssembly package (`DataVo.Browser`) | Playground only | Runs this site's playground; not published on npm. |
| EF Core migrations | Not planned | `DataVo.EntityFrameworkCore` is a query and DML bridge. |
| Replication and high availability | Out of scope | DataVo is an embedded engine. |
