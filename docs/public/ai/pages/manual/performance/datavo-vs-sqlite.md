# DataVo vs SQLite: When Each Engine Wins

> Source route: /manual/performance/datavo-vs-sqlite
> Source file: manual/performance/datavo-vs-sqlite.md

This guide compares DataVo v0.4 (`StorageMode.Lsm` and `StorageMode.InMemory`) against SQLite 3.50.4 (`WAL`, `synchronous=NORMAL`, plus `sqlite-vec` v0.1.9) across **28 relational SQL shapes** over 1,000,000 orders and **5 filtered nearest-neighbour vector queries**, with DuckDB 1.5.3 as an analytical reference. Every query's output is verified bit-for-bit against an independent oracle before timing begins.

**The short version:** DataVo wins on primary-key ranges (**1.4–1.7×**), secondary-index lookups (**2.4×**), low-to-medium cardinality `GROUP BY` (**2.7–4.8×**), integer-key `INNER`/`LEFT` joins (**2.3–10.3×**), SIMD columnar aggregates (**4.8–13.7×**), and filtered vector search (**3.0–71×**). SQLite wins on unindexed full-table scans (**1.05–1.8×**), unindexed `ORDER BY ... LIMIT` (**1.3×**), `LEFT JOIN` anti-joins (**1.07×**), and high-cardinality indexed `GROUP BY` on LSM (**1.24×**). DuckDB beats both on full-table analytical scans (**3–340×**) and trails both on key lookups and ranges.

## Executive Decision Guide

Medians compare DataVo LSM (`LsmStrictFsync = false`) against SQLite (`WAL`, `synchronous=NORMAL`), single-threaded client, 1,000,000 orders (Apple M4 Pro, .NET 10, 17 November 2026 runs).

| Workload Shape | Faster Engine | Speedup | Query IDs |
| --- | --- | ---: | --- |
| Primary-key range read (100–10,000 rows) | **DataVo LSM** | 1.4–1.7× | R1–R4 |
| Primary-key range read (`InMemory` mode) | **SQLite** | 1.1–1.2× | R1–R4 |
| Indexed equality lookup (~1,000 rows) | **DataVo** | 2.4× | W1 |
| Unindexed full-scan filter (`=`, `OR`, `LIKE`) | **SQLite** | 1.05–1.35× | W2–W6 |
| Numeric `COUNT`/`SUM`/`MIN`/`MAX` with `[Columnar]` index | **DataVo** | 4.8–13.7× | C1, C2, C4, C6 |
| Numeric aggregate *without* a columnar index (or LSM `DOUBLE` `SUM`) | **SQLite** | 1.2–1.8× | C1–C4, C6 |
| Full-table `GROUP BY` (4–1,000 groups) | **DataVo** | 2.7–4.8× | G1, G2, C7 |
| High-cardinality indexed `GROUP BY` (~125k groups, `HAVING` + top-100) | **SQLite** (vs LSM) / **DataVo** (InMemory) | 1.24× / 1.33× | G3 |
| Unindexed `ORDER BY ... LIMIT 10` | **SQLite** | 1.3× | C8 |
| 2–3 table `INNER` / `LEFT` joins on integer keys | **DataVo** | 2.3–10.3× | J1–J4, J6, J7 |
| `LEFT JOIN ... WHERE c.Id IS NULL` (anti-join) | **SQLite** | 1.07× | J5 |
| Filtered top-10 vector search (exact, 384 / 1,536 dims) | **DataVo Flat** | 3.0–7.0× / 1.4–5.6× | V1–V4 |
| Filtered top-10 vector search (approximate, 100% recall on test data) | **DataVo HNSW** | 3.6–71× / 2.9–53× | V0–V4 |
| Full-table analytical scans & multi-threaded OLAP | **DuckDB** | 3–340× | All full scans |

## Three Rules Behind the Numbers

1. **Stay on a typed fast path:** DataVo is fast when a query runs on its typed single-table scan, typed integer-key join, SIMD columnar index, or hybrid vector path. Queries that fall back to the general evaluator (`DISTINCT`, CTEs, `RIGHT`/`FULL` joins, text/GUID joins, cross-table `OR`, or joins inside explicit transactions) allocate 2–4 KB per row and are substantially slower than SQLite. See [Query Planner & Fast Paths](/manual/internals/query-planner).
2. **SQLite's row decoder leads slightly on raw full scans:** When both engines touch all 1,000,000 rows without an index advantage, SQLite's record header offset table gives it a 1.05–1.8× edge over DataVo's sequential cell walk. DataVo wins full scans by doing less work: hashing instead of sorting for `GROUP BY`, or scanning 4,096-row columnar chunks with zone maps.
3. **Index coverage decides edge cases:** SQLite uses covering secondary indexes for high-cardinality `GROUP BY` and per-row probes. DataVo uses clustered PKs for ranges, secondary indexes for equality/selective inner joins, and single-column integer indexes for counting `GROUP BY` (nullable keys still require a null-flag pass on LSM).

## 28 Relational SQL Shapes (1,000,000 Orders) {#relational-results}

All relational queries run prepared statements with bound parameters against `Customers` (100k rows), `Products` (1k rows), and `Orders` (1M rows, 25% dangling `CustomerId`, 10% NULL `Discount`, exact multiples of `0.25` for bit-exact `DOUBLE` sums). Raw artifacts: [`2026-11-17-engine-perf-followups/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-17-engine-perf-followups).

::: details Setup & Storage Footprint (1,101,000 rows)
| Metric | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB |
| --- | ---: | ---: | ---: | ---: |
| Load 1,101,000 rows (ms) | 3,564 | 536 | 4,952 (incl. `ANALYZE`) | 1,014 |
| Close + reopen after load (ms) | 506 | – | 2.2 | 235 |
| Reopen again, nothing to flush (ms) | 8.0 | – | 0.4 | 3.7 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.3 |
| Create columnar index (ms) | 317 | 188 | – | – |

Nearly all of the 506 ms post-load reopen time is flushing trailing MemTables to SSTables on close; a clean reopen with nothing to flush takes 8.0 ms.
:::

::: details Joins (J1–J7)
Median latency per call (p99 in brackets). Ratio = SQLite / DataVo LSM (> 1 means DataVo is faster).

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| **J1** 3-table inner join, `COUNT`, `SUM`, no filter | 70.2 ms (82.9 ms) | 90.2 ms | 721 ms (740 ms) | 2.11 ms | **10.3×** |
| **J2** J1 + `WHERE c.Region = @r AND p.CategoryId = @cat` (~0.2%) | 21.9 ms (23.4 ms) | 10.8 ms | 84.4 ms | 1.51 ms | **3.85×** |
| **J3** J2 returning rows, `ORDER BY o.Amount DESC, o.Id LIMIT 50` | 16.1 ms (20.0 ms) | 12.8 ms | 84.0 ms | 1.86 ms | **5.22×** |
| **J4** `LEFT JOIN`: `COUNT(*)`, `COUNT(c.Id)`, `SUM` | 68.3 ms (78.9 ms) | 93.1 ms | 238 ms | 3.37 ms | **3.48×** |
| **J5** `LEFT JOIN ... WHERE c.Id IS NULL` (anti-join) | 39.1 ms (39.3 ms) | 81.8 ms | 36.6 ms | 3.30 ms | 0.936× |
| **J6** Orders × Customers, `GROUP BY c.Region` | 130 ms (141 ms) | 195 ms | 764 ms | 3.40 ms | **5.87×** |
| **J7** Products (2%) × Orders, `COUNT`, `SUM` | 6.70 ms (9.62 ms) | 6.20 ms | 15.3 ms | 1.05 ms | **2.29×** |

**Mechanisms:** SQLite executes J1, J4, and J6 as index nested-loop joins (1M B-tree descents). DataVo uses hash joins, index-driven probes (J2, J3, J7), and a 12.5 KB `DenseInt64Set` bit set for key-only tables (J1, J4, J5, cutting per-call allocation to 31–41 KB). SQLite narrowly wins J5 (36.6 ms vs 39.1 ms) because DataVo pays per-row cell walking plus rebuilding the bit set per call.
:::

::: details Filters (W1–W6)
| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| **W1** `WHERE ProductId = @p` (indexed, ~1,000 rows) | 341 µs (658 µs) | 184 µs (479 µs) | 825 µs (920 µs) | 1.13 ms | **2.42×** |
| **W2** `WHERE ProductTag = @p` (no index, ~1,000 rows) | 30.5 ms (33.4 ms) | 66.5 ms | 28.6 ms | 553 µs | 0.941× |
| **W3** `(Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= @lo AND Amount < @hi` | 64.0 ms (66.0 ms) | 76.3 ms | 61.1 ms | 2.11 ms | 0.955× |
| **W4** W3 `AND Discount IS NOT NULL AND Discount < @d` | 74.2 ms (78.1 ms) | 84.7 ms | 63.3 ms | 2.08 ms | 0.853× |
| **W5** `Sku LIKE 'S0042%'` (no index) | 47.6 ms (48.0 ms) | 33.2 ms | 42.2 ms | 2.85 ms | 0.888× |
| **W6** `Sku LIKE '%042-%'` (no index) | 68.7 ms (70.3 ms) | 41.9 ms | 50.7 ms | 4.16 ms | 0.738× |

**Mechanisms:** DataVo compares UTF-8 bytes directly in stored LSM rows without decoding `.NET` strings (16–24 KB allocated per 1M-row scan, down from 36–46 MB). SQLite stays 1.05–1.35× ahead on W2–W6 due to O(1) column-offset headers vs. DataVo's cell walk.
:::

::: details Primary-Key Ranges (R1–R4)
| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| **R1** `Id BETWEEN @lo AND @hi` (100 rows) | 13.6 µs (20.5 µs) | 22.9 µs (45.2 µs) | 18.5 µs (24.6 µs) | 178 µs | **1.36×** |
| **R2** `Id >= @lo AND Id < @hi` (100 rows) | 12.9 µs (19.3 µs) | 23.0 µs (31.6 µs) | 18.2 µs (23.5 µs) | 180 µs | **1.41×** |
| **R3** `BETWEEN` (10,000 rows) | 1.01 ms (12.0 ms) | 2.13 ms (15.2 ms) | 1.76 ms (1.95 ms) | 588 µs | **1.74×** |
| **R4** `>= AND <` (10,000 rows) | 991 µs (11.9 ms) | 1.99 ms (13.7 ms) | 1.74 ms (1.85 ms) | 588 µs | **1.76×** |

LSM seeks clustered SSTables sequentially (1.4–1.7× faster than SQLite). InMemory binary-searches a sorted key copy and performs random slot lookups (1.1–1.2× slower than SQLite). The 10,000-row p99 spike (11–14 ms) is GC from materializing 3.0–3.4 MB of result rows per call.
:::

::: details GROUP BY (G1–G3)
| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| **G1** `GROUP BY Status` (4 groups) | 49.8 ms (50.0 ms) | 63.7 ms | 239 ms | 1.15 ms | **4.80×** |
| **G2** `GROUP BY Bucket` (1,000 groups) | 73.0 ms (78.0 ms) | 48.0 ms | 210 ms | 820 µs | **2.87×** |
| **G3** `GROUP BY CustomerId HAVING COUNT(*) >= @k ORDER BY N DESC, CustomerId LIMIT 100` (~125k groups) | 32.5 ms (33.2 ms) | 19.8 ms | 26.3 ms | 4.47 ms | 0.809× |

v0.5.0 re-run (25 November 2026, `master` @ `dffb4e13`; raw output in [`2026-11-25-v050/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-25-v050)). G3 on LSM improved from 44.2 ms to 32.5 ms since v0.4 because LSM indexes now hold `NULL` keys, so the index-only count no longer scans `CustomerId` null flags.

**Mechanisms:** DataVo hashes G1/G2 without boxing (UTF-8 byte keys for `Status`, 27.2 KB allocated) while SQLite sorts into a temporary B-tree. On G3, DataVo streams counts from `ix_orders_customer` (InMemory beats SQLite by 1.33×, 909 KB allocated); SQLite still leads LSM by 1.24× through its covering index.
:::

::: details Columnar Index (C1–C8)
`CREATE INDEX cx_orders ON Orders (Amount, Qty, Bucket, Discount, ProductTag) USING COLUMNAR`. Every query allocates 16–48 KB per call on DataVo LSM.

| Query | DataVo (LSM) | DataVo (LSM) + index | DataVo (InMemory) + index | SQLite | DuckDB | SQLite / DataVo + index |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| **C1** `COUNT(*) WHERE Amount >= @lo AND Amount < @hi` | 53.5 ms | **3.18 ms**, pushed | 3.27 ms, pushed | 43.7 ms | 1.13 ms | **13.7×** |
| **C2** `SUM(Qty) WHERE Bucket < @b` | 67.2 ms | **3.44 ms**, pushed | 3.73 ms, pushed | 36.7 ms | 415 µs | **10.7×** |
| **C3** `SUM(Amount) WHERE Qty >= @q` (`DOUBLE` sum) | 65.1 ms | 65.9 ms, row scan | **3.05 ms**, pushed | 39.9 ms | 459 µs | 0.606× |
| **C4** `COUNT(*), SUM(Qty), MIN/MAX(Amount) WHERE Bucket < @b AND Discount IS NOT NULL` | 86.7 ms | **11.9 ms**, pushed | 8.48 ms, pushed | 57.4 ms | 1.38 ms | **4.82×** |
| **C5** `COUNT(*) WHERE Status = @s AND Amount >= @lo` (string col) | 40.0 ms | 39.9 ms, row scan | 45.9 ms, row scan | 42.7 ms | 984 µs | **1.07×** |
| **C6** `COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= @lo` | 64.1 ms | **4.77 ms**, pushed | 497 µs, pushed | 35.6 ms | 1.69 ms | **7.45×** |
| **C7** `GROUP BY Bucket`: `COUNT(*)`, `SUM(Qty)` (v0.5.0 re-run) | 74.7 ms | 75.1 ms, row scan | 47.4 ms, row scan | 201 ms | 833 µs | **2.67×** |
| **C8** `ORDER BY Amount DESC, Id LIMIT 10` | 53.0 ms | 52.9 ms, row scan | 46.7 ms, row scan | 41.6 ms | 1.31 ms | 0.786× |

**Why C3 is not pushed on LSM:** When an LSM columnar index has an unflushed tail, floating-point `SUM`/`AVG` falls back to the row scan to preserve bit-identical key-order addition. On InMemory (which is in key order), C3 is pushed at 3.05 ms.
:::

## Hybrid Vector & SQL Search {#hybrid-vector-and-sql}

Measured on `Docs (Id, Category, Price, AuthorId, Emb VECTOR(d))` with 128 normalized clusters, 200 held-out queries, and `Authors` (2,000 rows, 10 countries). DataVo runs prepared `ORDER BY Emb <=> @q LIMIT 10` on LSM (`FLAT` or `HNSW`, default settings) against `sqlite-vec` `vec0` metadata-filtered exact KNN.

- **100% Recall@10** across every shape, engine, and dimension; **0 short results** and **0 filter violations**.
- **384 dims (50,000 rows):** DataVo Flat is **3.0–8.3×** faster than `sqlite-vec`; HNSW is **3.6–70×** faster (`0.26 ms` unfiltered, `0.67–3.62 ms` filtered).
- **1,536 dims (20,000 rows):** DataVo Flat is **1.4–7.0×** faster; HNSW is **2.9–53×** faster (`0.60 ms` unfiltered, `1.40–6.10 ms` filtered).
- **Planner optimizations:** Stored-row candidate filtering cut per-query allocation 20–35× (to 183–232 KB at 384 dims); HNSW automatically cost-routes to an exact SIMD Flat scan when candidate expansion widens the beam past ~9% (384d) or ~20% (1,536d) of the dataset; `enable_diversity_heuristic` is enabled by default for clustered recall.

::: details Full Hybrid Vector Tables (384d & 1,536d) + HNSW Ablation
#### 384 Dimensions, 50,000 Rows
| Shape | DataVo Flat | DataVo HNSW | SQLite + sqlite-vec | sqlite-vec / Flat | sqlite-vec / HNSW |
| --- | ---: | ---: | ---: | ---: | ---: |
| Load + index build | 442 ms | 1,628 ms | 653 ms | | |
| **V0** Top-10, no filter | 2.19 ms (3.96 ms) | 0.26 ms (0.54 ms) | 18.2 ms (18.5 ms) | **8.3×** | **70×** |
| **V1** `Category = @c AND Price < 50` (~5%) | 2.64 ms (6.87 ms) | 2.48 ms (6.61 ms) | 9.78 ms (10.4 ms) | **3.7×** | **3.9×** |
| **V2** `Price < 50` (~50%) | 2.04 ms (4.15 ms) | 0.67 ms (1.92 ms) | 14.4 ms (15.0 ms) | **7.0×** | **22×** |
| **V3** V1 + `JOIN Authors` | 2.56 ms (6.16 ms) | 2.42 ms (5.63 ms) | 9.76 ms (10.5 ms) | **3.8×** | **4.0×** |
| **V4** `JOIN Authors WHERE a.Country = @k` (~10%) | 4.34 ms (8.13 ms) | 3.62 ms (6.10 ms) | 13.1 ms (16.1 ms) | **3.0×** | **3.6×** |

#### 1,536 Dimensions, 20,000 Rows
| Shape | DataVo Flat | DataVo HNSW | SQLite + sqlite-vec | sqlite-vec / Flat | sqlite-vec / HNSW |
| --- | ---: | ---: | ---: | ---: | ---: |
| Load + index build | 485 ms | 2,039 ms | 3,952 ms | | |
| **V0** Top-10, no filter | 4.57 ms (5.71 ms) | 0.60 ms (0.95 ms) | 32.0 ms (33.0 ms) | **7.0×** | **53×** |
| **V1** (~5%) | 4.69 ms (10.6 ms) | 4.66 ms (9.49 ms) | 15.4 ms (16.6 ms) | **3.3×** | **3.3×** |
| **V2** (~50%) | 4.16 ms (8.25 ms) | 1.40 ms (3.26 ms) | 23.5 ms (24.5 ms) | **5.6×** | **17×** |
| **V3** V1 + `JOIN Authors` | 4.61 ms (10.4 ms) | 4.52 ms (9.58 ms) | 15.4 ms (18.7 ms) | **3.3×** | **3.4×** |
| **V4** (~10%, author filter) | 12.2 ms (14.6 ms) | 6.10 ms (10.1 ms) | 17.4 ms (18.5 ms) | **1.4×** | **2.9×** |

#### HNSW Diversity Heuristic Ablation (128 Clusters)
| Construction Setting | Dims | Build | Recall@10 | 0%-Recall Queries (default / ef=512) | Median Query |
| --- | ---: | ---: | ---: | ---: | ---: |
| Nearest neighbours only (old default) | 384 | 1,690 ms | 83.6% | 20 / 19 | 0.158 ms |
| **Diversity heuristic (v0.4 default)** | 384 | 1,592 ms | **100.0%** | **0 / 0** | 0.166 ms |
| Nearest neighbours only (old default) | 1,536 | 1,800 ms | 87.5% | 15 / 12 | 0.284 ms |
| **Diversity heuristic (v0.4 default)** | 1,536 | 1,900 ms | **100.0%** | **0 / 0** | 0.421 ms |

*Note:* Commit `5cbcd14` marks `SimdDistanceKernels` with `[MethodImpl(MethodImplOptions.AggressiveOptimization)]` to eliminate tiered-JIT cold-start jitter on Flat V0 (stabilizing 15 reruns at 2.21–2.28 ms, p99 3.0–4.2 ms).
:::

## Oracle Verification & Caveats {#correctness-findings}

The initial 14 November oracle run caught **5 correctness bugs** (outer-join predicate pushdown `B1`, `COUNT(DISTINCT)` parsing `B2`, aliased vector filter extraction `B3`, multi-join key alignment `B4`, and cross-table vector join filters `B5`), plus two closeout fixes (short filtered vector fallback and `WHERE ... HAVING` without `GROUP BY`). All are fixed with regression tests on both LSM and InMemory engines.

::: details Correctness Findings Log (B1–B5)
| # | Query Shape | Bug Before Fix | Fix Commit |
| --- | --- | --- | --- |
| **B1** | `LEFT JOIN ... WHERE c.Id IS NULL` (J5) | Returned `0` instead of `250,395` (filtered before NULL padding) | `bfb1390`: filter after outer join |
| **B2** | `COUNT(DISTINCT CustomerId)` | Returned `2,000` instead of `175` (`DISTINCT` dropped) | `6743521`: parsed & routed to general path |
| **B3** | Vector top-10 + `JOIN` with table alias (V3) | Returned 0–2 rows (alias `d` didn't match table `Docs`) | `8ce4295`: resolve qualifiers via table service |
| **B4** | 3-table join with `AND` across two dimensions (J2/J3) | Returned `0` rows instead of `1,827` (mismatched key order) | `fd81cf8`: canonical table key ordering |
| **B5** | Vector top-10 + `JOIN Authors WHERE a.Country = @k` (V4) | 9/10 rows failed filter (`a.Country` dropped) | `8ce4295`: pre-filter, join, and expand candidates |
:::

**Scope limits:** Single Apple M4 Pro machine, warm OS page cache, read-only timed queries, and ADO.NET client overhead included for SQLite/DuckDB. On x64 Linux/Windows runners, DataVo's scan lead over SQLite is 2–2.8× narrower (see [Benchmark Results](/manual/performance/benchmarks#part-iii-platforms)). Treat differences under ~20% in single-run scenarios as within measurement noise.

## See Also
- [Benchmark Results](/manual/performance/benchmarks): Microbenchmarks, durability (`fsync`), reactive IVM, and cross-platform CI numbers
- [Reproducing Benchmarks](/manual/performance/reproducing-benchmarks): CLI commands to run `sql-matrix` and `hybrid-vector` locally
- [Query Planner & Fast Paths](/manual/internals/query-planner): How to keep queries on DataVo's typed fast paths
