# DataVo vs SQLite: When Each Engine Wins

> Source route: /manual/performance/datavo-vs-sqlite
> Source file: manual/performance/datavo-vs-sqlite.md

This guide compares DataVo v0.5.1 (`StorageMode.Lsm` and `StorageMode.InMemory`) against SQLite 3.50.4 (`WAL`, `synchronous=NORMAL`, plus `sqlite-vec` v0.1.9) across **28 relational SQL shapes** over 1,000,000 orders and **5 filtered nearest-neighbour vector queries**, with DuckDB 1.5.3 as an analytical reference. Every query's output is verified bit-for-bit against an independent oracle before timing begins.

**The short version:** DataVo wins on primary-key ranges (**1.3–1.7×**), secondary-index lookups (**2.6×**), low-to-medium cardinality `GROUP BY` (**3.3–4.7×**), integer-key `INNER`/`LEFT` joins (**2.3–10.7×**), SIMD columnar aggregates (**5.0–13.8×**), and filtered vector search (**1.4–71×**). SQLite wins on unindexed full-table scans (**1.15–2.0×**), unindexed `ORDER BY ... LIMIT` (**1.3×**), and `LEFT JOIN` anti-joins (**1.09×**); high-cardinality indexed `GROUP BY` on LSM is now a tie (**1.02×**). DuckDB beats both on full-table analytical scans (**6–360×**) and trails both on key lookups and ranges.

## Executive Decision Guide

Medians compare DataVo LSM (`LsmStrictFsync = false`) against SQLite (`WAL`, `synchronous=NORMAL`), single-threaded client, 1,000,000 orders (Apple M4 Pro, .NET 10, v0.5.1 run of 27 November 2026, `master` @ `fdea08a6`).

| Workload Shape | Faster Engine | Speedup | Query IDs |
| --- | --- | ---: | --- |
| Primary-key range read (100–10,000 rows) | **DataVo LSM** | 1.3–1.7× | R1–R4 |
| Primary-key range read (`InMemory` mode) | **SQLite** (100 rows) / **DataVo** (10,000 rows) | 1.1× / 1.0–1.15× | R1–R4 |
| Indexed equality lookup (~1,000 rows) | **DataVo** | 2.6× | W1 |
| Unindexed full-scan filter (`=`, `OR`, `LIKE`) | **SQLite** | 1.15–1.3× | W2–W6 |
| Numeric `COUNT`/`SUM`/`MIN`/`MAX` with `[Columnar]` index | **DataVo** | 5.0–13.8× | C1, C2, C4, C6 |
| Numeric aggregate *without* a columnar index (or LSM `DOUBLE` `SUM`) | **SQLite** | 1.5–2.0× | C1–C4, C6 |
| Full-table `GROUP BY` (4–1,000 groups) | **DataVo** | 3.3–4.7× | G1, G2, C7 |
| High-cardinality indexed `GROUP BY` (~125k groups, `HAVING` + top-100) | Tie (vs LSM) / **DataVo** (InMemory) | 1.02× / 1.55× | G3 |
| Unindexed `ORDER BY ... LIMIT 10` | **SQLite** | 1.3× | C8 |
| 2–3 table `INNER` / `LEFT` joins on integer keys | **DataVo** | 2.3–10.7× | J1–J4, J6, J7 |
| `LEFT JOIN ... WHERE c.Id IS NULL` (anti-join) | **SQLite** | 1.09× | J5 |
| Filtered top-10 vector search (exact, 384 / 1,536 dims) | **DataVo Flat** | 2.8–6.9× / 1.4–5.6× | V1–V4 |
| Filtered top-10 vector search (approximate, ≥ 99.95% mean recall on test data) | **DataVo HNSW** | 3.7–71× / 2.8–53× | V0–V4 |
| Full-table analytical scans & multi-threaded OLAP | **DuckDB** | 6–360× | All full scans |

## Three Rules Behind the Numbers

1. **Stay on a typed fast path:** DataVo is fast when a query runs on its typed single-table scan, typed integer-key join, SIMD columnar index, or hybrid vector path. Queries that fall back to the general evaluator (`DISTINCT`, CTEs, `RIGHT`/`FULL` joins, text/GUID joins, cross-table `OR`, or joins inside explicit transactions) allocate 2–4 KB per row and are substantially slower than SQLite. See [Query Planner & Fast Paths](/manual/internals/query-planner).
2. **SQLite's row decoder leads slightly on raw full scans:** When both engines touch all 1,000,000 rows without an index advantage, SQLite's record header offset table gives it a 1.15–2.0× edge over DataVo's sequential cell walk. DataVo wins full scans by doing less work: hashing instead of sorting for `GROUP BY`, or scanning 4,096-row columnar chunks with zone maps.
3. **Index coverage decides edge cases:** SQLite uses covering secondary indexes for high-cardinality `GROUP BY` and per-row probes. DataVo uses clustered PKs for ranges, secondary indexes for equality/selective inner joins, and single-column integer indexes for counting `GROUP BY` (LSM indexes record `NULL` keys, so the `NULL` group comes from the index; `InMemory` indexes count it from row null flags).

## 28 Relational SQL Shapes (1,000,000 Orders) {#relational-results}

All relational queries run prepared statements with bound parameters against `Customers` (100k rows), `Products` (1k rows), and `Orders` (1M rows, 25% dangling `CustomerId`, 10% NULL `Discount`, exact multiples of `0.25` for bit-exact `DOUBLE` sums). Raw artifacts (v0.5.1, 27 November 2026): [`2026-11-27-v051/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-27-v051) (`sql-matrix.md`, `hybrid-vector.md`, `hybrid-vector-1536.md`). Every engine returned the oracle's answer for every query and parameter set.

::: details Setup & Storage Footprint (1,101,000 rows)
| Metric | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB |
| --- | ---: | ---: | ---: | ---: |
| Load 1,101,000 rows (ms) | 3,737 | 535 | 5,243 (incl. `ANALYZE`) | 1,096 |
| Close + reopen after load (ms) | 388 | – | 2.9 | 258 |
| Reopen again, nothing to flush (ms) | 7.7 | – | 0.6 | 3.8 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.0 |
| Create columnar index (ms) | 328 | 182 | – | – |

Nearly all of the 388 ms post-load reopen time is flushing trailing MemTables to SSTables on close; a clean reopen with nothing to flush takes 7.7 ms.
:::

::: details Joins (J1–J7)
Median latency per call (p99 in brackets). Ratio = SQLite / DataVo LSM (> 1 means DataVo is faster).

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| **J1** 3-table inner join, `COUNT`, `SUM`, no filter | 71.8 ms (87.2 ms) | 93.4 ms | 765 ms (804 ms) | 2.12 ms | **10.7×** |
| **J2** J1 + `WHERE c.Region = @r AND p.CategoryId = @cat` (~0.2%) | 22.0 ms (24.1 ms) | 8.93 ms | 90.7 ms | 1.50 ms | **4.11×** |
| **J3** J2 returning rows, `ORDER BY o.Amount DESC, o.Id LIMIT 50` | 14.7 ms (19.8 ms) | 10.7 ms | 89.0 ms | 1.99 ms | **6.04×** |
| **J4** `LEFT JOIN`: `COUNT(*)`, `COUNT(c.Id)`, `SUM` | 69.0 ms (82.0 ms) | 92.5 ms | 240 ms | 3.45 ms | **3.48×** |
| **J5** `LEFT JOIN ... WHERE c.Id IS NULL` (anti-join) | 40.1 ms (42.9 ms) | 83.8 ms | 36.8 ms | 3.31 ms | 0.918× |
| **J6** Orders × Customers, `GROUP BY c.Region` | 158 ms (182 ms) | 231 ms | 795 ms | 3.41 ms | **5.03×** |
| **J7** Products (2%) × Orders, `COUNT`, `SUM` | 7.10 ms (7.55 ms) | 4.98 ms | 16.1 ms | 1.10 ms | **2.26×** |

**Mechanisms:** SQLite executes J1, J4, and J6 as index nested-loop joins (1M B-tree descents). DataVo uses hash joins, index-driven probes (J2, J3, J7), and a 12.5 KB `DenseInt64Set` bit set for key-only tables (J1, J4, J5, cutting per-call allocation to 31–40 KB). SQLite narrowly wins J5 (36.8 ms vs 40.1 ms) because DataVo pays per-row cell walking plus rebuilding the bit set per call.
:::

::: details Filters (W1–W6)
| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| **W1** `WHERE ProductId = @p` (indexed, ~1,000 rows) | 340 µs (779 µs) | 155 µs (427 µs) | 895 µs (1.19 ms) | 1.14 ms | **2.63×** |
| **W2** `WHERE ProductTag = @p` (no index, ~1,000 rows) | 35.1 ms (39.4 ms) | 66.1 ms | 28.3 ms | 536 µs | 0.805× |
| **W3** `(Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= @lo AND Amount < @hi` | 70.5 ms (78.5 ms) | 83.8 ms | 61.4 ms | 2.04 ms | 0.870× |
| **W4** W3 `AND Discount IS NOT NULL AND Discount < @d` | 84.7 ms (85.6 ms) | 91.7 ms | 65.3 ms | 2.11 ms | 0.771× |
| **W5** `Sku LIKE 'S0042%'` (no index) | 49.2 ms (50.9 ms) | 33.8 ms | 42.5 ms | 2.97 ms | 0.865× |
| **W6** `Sku LIKE '%042-%'` (no index) | 66.7 ms (77.0 ms) | 43.2 ms | 51.9 ms | 4.21 ms | 0.778× |

**Mechanisms:** DataVo compares UTF-8 bytes directly in stored LSM rows without decoding `.NET` strings (16–24 KB allocated per 1M-row scan, down from 36–46 MB). SQLite stays 1.15–1.3× ahead of LSM on W2–W6 (InMemory wins the `LIKE` scans W5–W6 by 1.2–1.26×) due to O(1) column-offset headers vs. DataVo's cell walk.
:::

::: details Primary-Key Ranges (R1–R4)
| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| **R1** `Id BETWEEN @lo AND @hi` (100 rows) | 14.0 µs (22.8 µs) | 20.6 µs (30.0 µs) | 18.3 µs (27.5 µs) | 172 µs | **1.31×** |
| **R2** `Id >= @lo AND Id < @hi` (100 rows) | 13.5 µs (20.8 µs) | 20.3 µs (34.1 µs) | 18.5 µs (33.8 µs) | 172 µs | **1.37×** |
| **R3** `BETWEEN` (10,000 rows) | 1.05 ms (12.0 ms) | 1.74 ms (13.8 ms) | 1.76 ms (2.04 ms) | 609 µs | **1.68×** |
| **R4** `>= AND <` (10,000 rows) | 1.03 ms (11.5 ms) | 1.55 ms (12.0 ms) | 1.79 ms (2.20 ms) | 597 µs | **1.74×** |

LSM seeks clustered SSTables sequentially (1.3–1.7× faster than SQLite). InMemory binary-searches a sorted key copy and performs random slot lookups: 1.1× slower than SQLite on 100-row ranges, level to 1.15× faster on 10,000-row ranges. The 10,000-row p99 spike (11–14 ms) is GC from materializing 3.0–3.4 MB of result rows per call.
:::

::: details GROUP BY (G1–G3)
| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| **G1** `GROUP BY Status` (4 groups) | 51.3 ms (63.0 ms) | 68.2 ms | 241 ms | 1.17 ms | **4.70×** |
| **G2** `GROUP BY Bucket` (1,000 groups) | 63.7 ms (67.7 ms) | 52.1 ms | 214 ms | 788 µs | **3.35×** |
| **G3** `GROUP BY CustomerId HAVING COUNT(*) >= @k ORDER BY N DESC, CustomerId LIMIT 100` (~125k groups) | 31.2 ms (34.6 ms) | 19.8 ms | 30.7 ms | 4.81 ms | 0.984× |

G3 on LSM improved from 44.2 ms (v0.4) to 31.2 ms because LSM indexes now hold `NULL` keys, so the index-only count no longer scans `CustomerId` null flags.

**Mechanisms:** DataVo hashes G1/G2 without boxing (UTF-8 byte keys for `Status`, 26.9 KB allocated) while SQLite sorts into a temporary B-tree. On G3, DataVo streams counts from `ix_orders_customer` (InMemory beats SQLite by 1.55×, 921 KB allocated); SQLite and LSM are level (1.02×), SQLite answering through its covering index.
:::

::: details Columnar Index (C1–C8)
`CREATE INDEX cx_orders ON Orders (Amount, Qty, Bucket, Discount, ProductTag) USING COLUMNAR`. Every query allocates 16–48 KB per call on DataVo LSM.

| Query | DataVo (LSM) | DataVo (LSM) + index | DataVo (InMemory) + index | SQLite | DuckDB | SQLite / DataVo + index |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| **C1** `COUNT(*) WHERE Amount >= @lo AND Amount < @hi` | 65.7 ms | **3.15 ms**, pushed | 3.32 ms, pushed | 43.5 ms | 1.37 ms | **13.8×** |
| **C2** `SUM(Qty) WHERE Bucket < @b` | 71.6 ms | **3.42 ms**, pushed | 3.70 ms, pushed | 36.7 ms | 488 µs | **10.7×** |
| **C3** `SUM(Amount) WHERE Qty >= @q` (`DOUBLE` sum) | 74.2 ms | 74.3 ms, row scan | **3.13 ms**, pushed | 40.2 ms | 532 µs | 0.540× |
| **C4** `COUNT(*), SUM(Qty), MIN/MAX(Amount) WHERE Bucket < @b AND Discount IS NOT NULL` | 89.6 ms | **11.9 ms**, pushed | 12.4 ms, pushed | 60.1 ms | 1.46 ms | **5.04×** |
| **C5** `COUNT(*) WHERE Status = @s AND Amount >= @lo` (string col) | 42.9 ms | 42.2 ms, row scan | 48.0 ms, row scan | 44.3 ms | 1.02 ms | **1.05×** |
| **C6** `COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= @lo` | 66.5 ms | **4.69 ms**, pushed | 562 µs, pushed | 36.0 ms | 1.68 ms | **7.67×** |
| **C7** `GROUP BY Bucket`: `COUNT(*)`, `SUM(Qty)` | 63.2 ms | 64.1 ms, row scan | 51.3 ms, row scan | 206 ms | 739 µs | **3.22×** |
| **C8** `ORDER BY Amount DESC, Id LIMIT 10` | 52.1 ms | 52.5 ms, row scan | 47.3 ms, row scan | 40.6 ms | 1.45 ms | 0.772× |

**Why C3 is not pushed on LSM:** When an LSM columnar index has an unflushed tail, floating-point `SUM`/`AVG` falls back to the row scan to preserve bit-identical key-order addition. On InMemory (which is in key order), C3 is pushed at 3.13 ms.
:::

## Hybrid Vector & SQL Search {#hybrid-vector-and-sql}

Measured on `Docs (Id, Category, Price, AuthorId, Emb VECTOR(d))` with 128 normalized clusters, 200 held-out queries, and `Authors` (2,000 rows, 10 countries). DataVo runs prepared `ORDER BY Emb <=> @q LIMIT 10` on LSM (`FLAT` or `HNSW`, default settings) against `sqlite-vec` `vec0` metadata-filtered exact KNN.

- **Recall@10:** 100% for every exact engine and every 1,536d shape; HNSW at 384d averages 100.0% with one V2 query returning 9 of its 10 true neighbours (min 90%). **0 short results** and **0 filter violations** anywhere.
- **384 dims (50,000 rows):** DataVo Flat is **2.8–8.2×** faster than `sqlite-vec`; HNSW is **3.7–71×** faster (`0.27 ms` unfiltered, `0.72–3.83 ms` filtered).
- **1,536 dims (20,000 rows):** DataVo Flat is **1.4–6.8×** faster; HNSW is **2.8–53×** faster (`0.61 ms` unfiltered, `1.41–6.32 ms` filtered).
- **Planner optimizations:** Stored-row candidate filtering cut per-query allocation 20–35× (to 183–232 KB at 384 dims); HNSW automatically cost-routes to an exact SIMD Flat scan when candidate expansion widens the beam past ~9% (384d) or ~20% (1,536d) of the dataset; `enable_diversity_heuristic` is enabled by default for clustered recall.

::: details Full Hybrid Vector Tables (384d & 1,536d) + HNSW Ablation
#### 384 Dimensions, 50,000 Rows
| Shape | DataVo Flat | DataVo HNSW | SQLite + sqlite-vec | sqlite-vec / Flat | sqlite-vec / HNSW |
| --- | ---: | ---: | ---: | ---: | ---: |
| Load + index build | 368 ms | 1,999 ms | 681 ms | | |
| **V0** Top-10, no filter | 2.31 ms (5.15 ms) | 0.27 ms (0.63 ms) | 19.0 ms (20.5 ms) | **8.2×** | **71×** |
| **V1** `Category = @c AND Price < 50` (~5%) | 2.82 ms (7.73 ms) | 2.51 ms (6.61 ms) | 10.6 ms (11.6 ms) | **3.7×** | **4.2×** |
| **V2** `Price < 50` (~50%) | 2.13 ms (4.99 ms) | 0.72 ms (2.10 ms) | 14.7 ms (20.6 ms) | **6.9×** | **20×** |
| **V3** V1 + `JOIN Authors` | 2.55 ms (6.11 ms) | 2.47 ms (5.36 ms) | 10.1 ms (11.0 ms) | **3.9×** | **4.1×** |
| **V4** `JOIN Authors WHERE a.Country = @k` (~10%) | 5.07 ms (11.1 ms) | 3.83 ms (5.95 ms) | 14.0 ms (16.6 ms) | **2.8×** | **3.7×** |

#### 1,536 Dimensions, 20,000 Rows
| Shape | DataVo Flat | DataVo HNSW | SQLite + sqlite-vec | sqlite-vec / Flat | sqlite-vec / HNSW |
| --- | ---: | ---: | ---: | ---: | ---: |
| Load + index build | 482 ms | 2,082 ms | 3,974 ms | | |
| **V0** Top-10, no filter | 4.75 ms (6.08 ms) | 0.61 ms (0.94 ms) | 32.1 ms (38.0 ms) | **6.8×** | **53×** |
| **V1** (~5%) | 4.86 ms (10.7 ms) | 4.62 ms (10.5 ms) | 15.3 ms (16.7 ms) | **3.2×** | **3.3×** |
| **V2** (~50%) | 4.19 ms (8.21 ms) | 1.41 ms (3.33 ms) | 23.5 ms (25.5 ms) | **5.6×** | **17×** |
| **V3** V1 + `JOIN Authors` | 4.59 ms (10.2 ms) | 4.50 ms (9.51 ms) | 15.5 ms (16.7 ms) | **3.4×** | **3.5×** |
| **V4** (~10%, author filter) | 12.4 ms (15.0 ms) | 6.32 ms (10.2 ms) | 17.4 ms (18.5 ms) | **1.4×** | **2.8×** |

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
