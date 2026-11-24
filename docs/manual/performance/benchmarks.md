---
title: Benchmark Results & Methodology
description: Microbenchmarks, durability costs, incremental view maintenance, cross-platform CI results, and benchmark methodology for DataVo against SQLite, DuckDB, and LiteDB.
---

# Benchmark Results & Methodology

This page summarizes DataVo's performance across **BenchmarkDotNet microbenchmarks**, **systems workloads** (durability, concurrency, reactive IVM, vector search, and 1M-row SQL), and **cross-platform CI runners** (macOS Apple Silicon, Linux x64, Windows x64), along with the methodology and caveats behind every number.

> **Looking for the latest v0.4 SQL & hybrid vector numbers?** See [DataVo vs SQLite: When Each Engine Wins](/manual/performance/datavo-vs-sqlite) for the 28-query 1M-row SQL matrix and 5 filtered vector shapes.

## Executive Summary

Headline rows below are dated per run. Point lookups and `[Columnar]` aggregates reflect the **20 November 2026** Apple M4 Pro refresh (`02fa240f`, `--job short`, matching [Part 1](/blog/part-1-where-it-wins-and-where-it-loses) and [Part 2](/blog/part-2-packed-buffers-aot-and-roslyn-codegen)); row-scan and systems rows cite their respective November 2026 runs.

| Workload | DataVo Result | Comparison | Run & Notes |
| --- | --- | --- | --- |
| **PK point lookup (100k rows)** | **55.0 ns** (InMemory, 56 B) · **216.2 ns** (LSM, 144 B) | SQLite: 716 ns / 1,654 ns (608 B) · DuckDB 1t: 62.7–63.6 µs | M4 Pro (`02fa240f`, 20 Nov 2026), compiled LINQ (`FlatRowStore` compact layout) |
| **Filtered aggregates (200k rows, `[Columnar]`)** | **27.1–89.6 µs** (0 B alloc, InMemory & LSM) | SQLite: 3,656–5,640 µs · DuckDB 1t: 123–701 µs | M4 Pro (`02fa240f`, 20 Nov 2026); 2.6–8.4× faster than 1-thread DuckDB, 55–162× faster than SQLite |
| **Filtered aggregates (200k rows, row scan)** | **386–526 µs** (InMemory, 0 B) · **2.0–2.3 ms** (LSM) | SQLite: 3.7–5.9 ms · DuckDB 1t: 137–716 µs | M4 Pro (`ce818d4`, 13 Nov 2026 full-job baseline); 8.8–9.8× faster than SQLite on M4 Pro, 3.5–4.7× on x64 CI |
| **Reactive IVM (3-table join, 50k updates)** | **191.7 ms** (3.1 µs P50) | DuckDB: 34.3 s (179×) · SQLite: 126.1 s (658×) | M4 Pro (12 Nov 2026); incremental Z-set updates vs. full-query polling |
| **Durable commits (`fsync` per update)** | **4.04 ms** P50 (Mac) · **2.42 ms** (Win) · **0.223 ms** (Linux) | SQLite `full+fullfsync`: 4.04 ms (Mac) · 2.51 ms (Win) · 0.097 ms (Linux) | 12 Nov 2026 baseline; hardware flush dominates on Mac/Win, SQLite leads 2.3× on fast-flush Linux VM |
| **Relaxed YCSB 80/20 (12 threads)** | **510,695 ops/s** (0.66 ms write P99) | SQLite WAL `NORMAL`: 261,618 ops/s (3.77 ms write P99) | M4 Pro (12 Nov 2026); 1.95× throughput, 5.7× lower write tail; neither mode is power-loss durable |
| **Disk footprint (1M rows)** | **27.7 MB** (LSM) | SQLite: 29.1 MB · LiteDB: 154.5 MB | M4 Pro (`02fa240f`, 21 Nov 2026); 5% smaller than SQLite, warm reopen in 1.7–4.5 ms |

## Part I: BenchmarkDotNet Microbenchmarks

Measured with `benchmarks/DataVo.Benchmarks` (Apple M4 Pro, .NET 10, 99.9% CI half-width ≤ 3% for DataVo/SQLite). Every benchmark verifies identical query answers across all engines before timing. The plots and expanded tables below record the **13 November 2026** full-job baseline (`ce818d4`); the **20 November 2026** (`02fa240f`) short-job refresh used in the Executive Summary is archived under `docs/public/benchmarks/2026-11-21-m4pro/`.

![Filtered aggregates over 200,000 rows](/benchmarks/bdn-filtered-aggregates.png)

![Point lookup and bulk insert](/benchmarks/bdn-lookup-and-insert.png)

::: details Full Part I Tables (13 Nov 2026 Full-Job Baseline: Row Scan, Columnar Index, Out-of-Core SSTable, Point Lookup, Bulk Load)
#### Filtered Aggregates (Row Scan, 200,000 rows, mean µs, 0 B alloc for DataVo)
| Query | Storage | DataVo (row scan) | SQLite | DuckDB (1 thread) | DuckDB (default) |
| --- | --- | ---: | ---: | ---: | ---: |
| `COUNT(*)` | Memory | 386 | 3,695 | 137 | 203 |
| `SUM(Gold)` | Memory | 526 | 4,817 | 204 | 226 |
| `MIN(Gold)` | Memory | 525 | 4,607 | 201 | 269 |
| `MAX(Ratio)` | Memory | 503 | 4,955 | 418 | 417 |
| `COUNT(*)` | File | 1,973 | 4,543 | 245 | 270 |
| `SUM(Gold)` | File | 2,115 | 5,935 | 374 | 327 |
| `MIN(Gold)` | File | 2,136 | 5,377 | 360 | 346 |
| `MAX(Ratio)` | File | 2,267 | 5,656 | 716 | 564 |

#### With a Columnar Index (`[Columnar]` on `Level`, `Gold`, `Ratio`, mean µs, 0 B alloc) {#with-a-columnar-index}
| Query | Storage | DataVo (`[Columnar]`) | SQLite | DuckDB (1 thread) | DuckDB (default) |
| --- | --- | ---: | ---: | ---: | ---: |
| `COUNT(*)` | Memory | **27.5** | 3,915 | 141 | 188 |
| `SUM(Gold)` | Memory | **77.8** | 4,777 | 204 | 228 |
| `MIN(Gold)` | Memory | **73.0** | 4,598 | 200 | 258 |
| `MAX(Ratio)` | Memory | **89.5** | 4,996 | 421 | 417 |
| `COUNT(*)` | File | **27.5** | 4,730 | 246 | 294 |
| `SUM(Gold)` | File | **78.2** | 5,574 | 365 | 331 |
| `MIN(Gold)` | File | **73.4** | 5,393 | 362 | 358 |
| `MAX(Ratio)` | File | **90.1** | 6,051 | 740 | 554 |

#### Out-of-Core Columnar Scans from SSTable Files (1,000,000 rows, LSM, mean µs, 0 B alloc)
| Query | Columns in Memory | Columns in SSTable Files (mmap) |
| --- | ---: | ---: |
| `COUNT(*)` | 135.8 | 135.7 |
| `SUM(Gold)` | 388.9 | 388.2 |
| `MIN(Gold)` | 361.8 | 362.6 |
| `MAX(Ratio)` | 453.9 | 460.8 |

#### Point Lookup by Primary Key (100,000 rows, shuffled keys)
| Storage | DataVo | SQLite | DuckDB (1t) | DuckDB | DataVo alloc | SQLite alloc | DuckDB alloc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | **52.1 ns** | 735.6 ns | 65,945 ns | 75,158 ns | **56 B** | 608 B | 2,144 B |
| File | **236.0 ns** | 1,609.4 ns | 64,877 ns | 72,314 ns | **144 B** | 608 B | 2,144 B |

#### Loading 100,000 Rows (10 timed runs ± SD)
| Storage | DataVo | SQLite | DuckDB (1t) | DuckDB | DataVo alloc | SQLite alloc | DuckDB alloc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 35.0 ± 6.8 ms | 82.2 ± 37.2 ms | 28.7 ± 5.6 ms | 38.5 ± 4.9 ms | 55 MB | 94 MB | 19 MB |
| File | 133.4 ± 35.4 ms | 76.8 ± 26.5 ms | 31.1 ± 5.9 ms | 29.0 ± 5.8 ms | 62 MB | 94 MB | 19 MB |
:::

## Part II: Systems Workloads

Application-shaped scenarios from `demos/Research.Benchmark` (single run per scenario; treat differences under ~20% as not established).

### Streamed Risk Views (Incremental View Maintenance)

Inserting 50,000 orders one-by-one and reading a live dashboard after each insert. DataVo maintains the view incrementally via Z-sets; SQLite and DuckDB re-run indexed queries.

![Streamed risk, complex VIP exposure](/benchmarks/ivm-complex-vip-total-time.png)

![Streamed risk, simple exposure](/benchmarks/ivm-simple-exposure-total-time.png)

- **Complex VIP 3-table join:** DataVo finishes in **191.7 ms** (3.1 µs P50), **179×** faster than DuckDB (`34.3 s`) and **658×** faster than SQLite (`126.1 s`), because incremental work is O(1) per order rather than O(N) re-joining.
- **Simple single-table exposure:** DataVo (`526.9 ms`, 6.4 µs P50) and SQLite (`574.1 ms`, 8.9 µs P50) are closely matched (1.1–1.4× across 4 runs), while DuckDB pays per-query setup overhead (`52.95 s`).

### Durability, Concurrency & Space

![Disk CRUD total time](/benchmarks/disk-crud-wal-time.png)

![Thread scaling](/benchmarks/thread-scaling-throughput.png)

![YCSB write P99](/benchmarks/ycsb-write-p99.png)

- **Strict durability (`fsync` per commit):** DataVo LSM Production (`84.6 s`, `4.04 ms` P50) and SQLite `WAL + full + fullfsync` (`84.9 s`, `4.04 ms` P50) tie on Apple Silicon because both wait on the same SSD cache flush. Group commit scales Production throughput **3.7×** from 1 to 32 threads.
- **Relaxed durability (OS buffer):** DataVo Relaxed beats SQLite WAL `NORMAL` by **1.4×** on serial updates (`113 ms` vs `155 ms`) and **1.95×** on 12-thread YCSB 80/20 (`510.7k` vs `261.6k ops/s`, with **5.7×** lower write P99).

### Vector Search (10,000 × 1,536d)

![Vector search total time](/benchmarks/vector-search-total-time.png)

![HNSW versus Flat](/benchmarks/vector-hnsw-vs-flat.png)

- On **uniform random** 1,536d vectors at 10k scale, exact **Flat** search (`298.7 ms` total, `1.96 ms` P50) beats both `sqlite-vec` (`472.8 ms`) and HNSW (`1,634 ms` total, 63.5% recall) because uniform high-dimensional distances concentrate.
- On **clustered** embeddings (realistic workloads), HNSW is **8×** faster per query at 10k vectors (`0.25 ms`, 95.9% recall) and **15×** faster at 50k (`0.68 ms`, 90.2% recall). Since v0.4, `enable_diversity_heuristic` is enabled by default, raising clustered Recall@10 to 100% on the hybrid benchmark dataset.

::: details Full Part II Raw Tables (IVM, Disk CRUD, Thread Scaling, YCSB, Space, Vector Recall, Flat/Doc/Concurrent)
#### Streamed Risk Views (50,000 insert + read iterations)
| Profile | Engine | Total (ms) | P50 | P99 | GC Allocated |
| --- | --- | ---: | ---: | ---: | ---: |
| Simple Exposure | DataVo | 526.9 | 6.4 µs | 21.3 µs | 364.6 MB |
| Simple Exposure | SQLite | 574.1 | 8.9 µs | 24.6 µs | 200.7 MB |
| Simple Exposure | DuckDB | 52,954.0 | 983 µs | 3,311 µs | 208.3 MB |
| Complex VIP Join | DataVo | **191.7** | **3.1 µs** | **4.0 µs** | 106.6 MB |
| Complex VIP Join | DuckDB | 34,339.1 | 660 µs | 974 µs | 131.3 MB |
| Complex VIP Join | SQLite | 126,064.4 | 2,527 µs | 4,375 µs | 115.6 MB |

#### Disk CRUD WAL (20,000 inserts + 20,000 committed updates)
| Engine | Total (ms) | P50 | P99 | GC Allocated |
| --- | ---: | ---: | ---: | ---: |
| DataVo (LSM Production) | 84,616 | 4.04 ms | 6.13 ms | 30.8 MB |
| SQLite (WAL, full, `fullfsync`) | 84,857 | 4.04 ms | 6.08 ms | 22.3 MB |
| DataVo (LSM Relaxed) | 113 | 4.5 µs | 7.5 µs | 22.9 MB |
| SQLite (WAL, normal) | 155 | 5.4 µs | 9.2 µs | 23.2 MB |

#### Thread Scaling (ops/s, 100k reads + 10k updates)
| Threads | DataVo Production | DataVo Relaxed | SQLite (WAL, normal) | LiteDB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2,686 | 1,333,227 | 335,281 | 49,027 |
| 4 | 5,506 | 1,101,274 | 589,921 | 68,873 |
| 8 | 8,777 | 1,025,118 | 577,691 | 67,864 |
| 32 | 9,840 | 1,011,191 | 555,357 | 66,020 |

#### YCSB Mixed 80/20 (12 threads, 100k ops)
| Engine | Total (ms) | Ops/s | Read P99 | Write P99 | GC Allocated |
| --- | ---: | ---: | ---: | ---: | ---: |
| DataVo (LSM Relaxed) | 195.8 | 510,695 | 0.9 µs | 0.66 ms | 34.7 MB |
| SQLite (WAL, normal) | 382.2 | 261,618 | 8.2 µs | 3.77 ms | 57.2 MB |
| LiteDB | 2,640.1 | 37,877 | 2.59 ms | 2.79 ms | 4,315.1 MB |
| DataVo (LSM Production) | 23,118.6 | 4,326 | 25.7 µs | 27.83 ms | 37.2 MB |

#### Space & Warm Reopen (1,000,000 records)
| Engine | Insert (ms) | Disk (MB) | Reopen (ms) | First Read (ms) | GC Allocated |
| --- | ---: | ---: | ---: | ---: | ---: |
| DataVo (LSM Production) | 731.8 | 27.7 | 4.3 | 3.6 | 400.7 MB |
| DataVo (LSM Relaxed) | 473.2 | 27.7 | 1.5 | 0.0 | 332.9 MB |
| SQLite (WAL, normal) | 712.5 | 29.1 | 0.36 | 1.2 | 633.4 MB |
| LiteDB | 2,987.1 | 154.5 | 0.61 | 9.4 | 15,107.2 MB |

#### Vector Search (10k × 1,536d, 100 top-10 queries) & Held-Out Recall
| Engine | Total (ms) | Query P50 | Query P99 | GC Allocated |
| --- | ---: | ---: | ---: | ---: |
| DataVo-Flat | 298.7 | 1.96 ms | 2.41 ms | 61.7 MB |
| SQLite (`sqlite-vec`) | 472.8 | 2.37 ms | 2.43 ms | 63.4 MB |
| DataVo (HNSW) | 1,634.0 | 1.77 ms | 2.25 ms | 62.8 MB |
| LiteDB | 76,706.5 | 747.4 ms | 778.0 ms | 208,914.6 MB |

| N | Distribution | Flat Query | HNSW Query | HNSW Recall@10 | HNSW Build | Flat Build |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | uniform | 0.24 ms | 0.45 ms | 99.9% | 238 ms | 27 ms |
| 10,000 | uniform | 2.06 ms | 1.92 ms | 63.5% | 1,169 ms | 135 ms |
| 50,000 | uniform | 10.1 ms | 3.0 ms | 23.8% | 10,414 ms | 162 ms |
| 1,000 | clustered | 0.24 ms | 0.31 ms | 100% | 63 ms | 10 ms |
| 10,000 | clustered | 2.00 ms | 0.25 ms | 95.9% | 548 ms | 34 ms |
| 50,000 | clustered | 10.3 ms | 0.68 ms | 90.2% | 5,658 ms | 236 ms |

#### Other Workloads (Flat CRUD, Deep Document, Concurrent Ops)
- **Flat CRUD (50k insert + 50k lookup, in-memory):** DataVo `63.9 ms` (`0.54 µs` P50) vs SQLite `128.8 ms` (`1.17 µs`) vs LiteDB `1,322.8 ms`.
- **Deep document (5k nested orders):** DataVo `83.6 ms` (`5.0 µs` P50) vs SQLite `140.8 ms` (`7.8 µs`) vs LiteDB `431.2 ms`.
- **Concurrent ops (8 readers + 2 writers, 5 s, M4 Pro):** DataVo `1,852,526 ops/s` (`48 µs` read P99, `0.11 ms` write P99) vs SQLite `372,220 ops/s` (`120 µs` read P99, `5.13 s` write P99).
:::

## SQL over 1,000,000 rows {#sql-over-1-000-000-rows}

The `sql-workload` scenario runs prepared SQL text over 1,000,000 file-backed rows (DataVo LSM Relaxed vs. SQLite WAL `NORMAL`, DuckDB, and LiteDB).

> **v0.4 Update:** Key range scans were upgraded to SSTable seeks (**12.9 µs** for 100 rows vs. SQLite's **18.2 µs**), and `ORDER BY ... LIMIT 10` eliminated per-row sort-key boxing (**48 KB** vs. 84 MB). See [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite) for the latest v0.4 measurements across all 28 SQL shapes.

::: details Historical 12 November `sql-workload` Table (0.2.0-rc.3 baseline)
| Operation | DataVo | SQLite | DuckDB | LiteDB |
| --- | ---: | ---: | ---: | ---: |
| Load orders and customers | 1.87 s | 3.24 s | 538.5 ms | 8.94 s |
| Point read by INT key | 1.4 µs | 2.0 µs | 150.0 µs | 23.2 µs |
| Point read by GUID key | 1.2 µs | 2.2 µs | 126.4 µs | 25.2 µs |
| Secondary-index lookup (~100 rows) | 110.8 µs | 86.6 µs | 265.0 µs | 413.2 µs |
| Key range scan (100 rows) | 45.7 µs | 7.4 µs | 167.9 µs | 549.2 µs |
| `COUNT` with filter (full scan) | 46.8 ms | 26.0 ms | 0.40 ms | 382.8 ms |
| `GROUP BY` (full scan) | 87.2 ms | 225.3 ms | 0.91 ms | 2.05 s |
| `JOIN` + `GROUP BY` | 133.3 ms | 353.0 ms | 1.5 ms | 451.0 ms |
| `ORDER BY ... LIMIT 10` | 58.6 ms | 36.9 ms | 1.2 ms | 923.5 ms |
| `UPDATE` by key (autocommit) | 9.0 µs | 6.5 µs | 175.1 µs | 42.0 µs |
| `DELETE` by key (autocommit) | 13.0 µs | 9.7 µs | 187.4 µs | 32.6 µs |
| Close (flush to disk) | 302.6 ms | 4.1 ms | 184.9 ms | 9.8 ms |
| Size on disk | 73.2 MB | 75.3 MB | 99.1 MB | 384.2 MB |
| Reopen and first point read | 4.9 ms | 0.5 ms | 5.3 ms | 0.4 ms |
:::

## Part III: Cross-Platform CI Results {#part-iii-platforms}

To verify portability beyond Apple Silicon, the full suite runs on GitHub Actions (`ubuntu-latest` x64, `windows-latest` x64, and `macos` M1 VM). Latest full CI refresh: **20 November 2026** (`master` @ `02fa240f`, run `44748508039`), plus a **21 November 2026** `concurrent-ops` re-run (`44916996459`).

![Mean time of a filtered COUNT and of a primary-key lookup across four machines](/benchmarks/ci-bdn-by-platform.png)

![Median latency of one durable update on three platforms](/benchmarks/ci-durable-update-latency.png)

![Total time of the vector scenario on three platforms](/benchmarks/ci-vector-search-time.png)

Key cross-platform findings:

- **Point lookups (20 Nov refresh):** Linux x64 `130.8 ns` (Memory, 56 B) / `288.3 ns` (LSM, 144 B) vs. SQLite `1,398 ns` / `3,643 ns`. Windows x64 `239.6 ns` (Memory) / `666.9 ns` (LSM) vs. SQLite `2,024 ns` / `6,741 ns`.
- **Columnar aggregates (20 Nov refresh, 200k rows):** Linux x64 `9.29 µs` (`COUNT`) and `27.7–40.6 µs` (`SUM`/`MIN`/`MAX`, 0 B alloc); Windows x64 `16.2 µs` and `84.3–102.9 µs`, outperforming both SQLite and 1-thread DuckDB on all platforms.
- **Concurrent ops (20–21 Nov CI):** The 20 Nov concurrency fixes (`44748508039`) eliminated writer starvation in that run, sustaining **1.53M ops/s** on 4-vCPU Linux (`302,878` writes, `0.125 ms` write P99) and Windows (`98,696` writes, `1.084 ms` write P99). However, the 21 Nov re-run (`44916996459`, adding a file-backed `SQLite WAL NORMAL` comparator) still observed intermittent writer starvation under 10-worker oversubscription on the 4-vCPU Windows runner (`0` writes in `9,297 ms`, while Linux completed `50,986` writes at `1.55M ops/s` and `0.212 ms` write P99, and `SQLite WAL NORMAL` completed `20,592` writes on Windows). Root cause (v0.5): the benchmark harness, not the engine. Its ten never-blocking workers ran as thread-pool work items, so on 4 cores the two writers (queued last) waited for the pool's slow thread injection and the `CancelAfter` timer that ends the run fired late on the same starved pool. Reproduced locally with `DOTNET_PROCESSOR_COUNT=4`: the old harness reports 0 writes and 4.0 s elapsed for a 3 s run; the fixed harness (dedicated threads, one start barrier, writers first, coordinator-owned deadline) reports 237,338 writes in 3.01 s. `WriterProgressUnderReadLoadTests` now pins per-300 ms writer progress under 2.5× oversubscribed continuous reads for `InMemory`, `Lsm` and the MVCC snapshot gate.
- **Durability differences:** On Linux VMs with fast virtual-disk flushes (`~0.1 ms`), SQLite's durable update (`0.097 ms`) leads DataVo Production (`0.223 ms`). On Windows (`~2.5 ms`) and macOS (`~4.0 ms`), hardware flush latency dominates and both engines tie.
- **SIMD paths:** `SimdDistanceKernels` auto-dispatches AVX2 (256-bit) on x64, AdvSimd/NEON (128-bit) on Arm64, and `TensorPrimitives` / `Vector<T>` on portable targets.

::: details Cross-Platform CI Tables, Unity IL2CPP Smoke & Historical Audit (July → November)
#### Latest CI Refresh (20 November 2026, `master` @ `02fa240f`, Run `44748508039`)
- **YCSB 80/20 (4 threads):** Linux DataVo Relaxed `230,912 ops/s` (`0.43 ms` write P99) vs SQLite `98,295 ops/s` (`3.20 ms`). Windows DataVo Relaxed `70,735 ops/s` (`1.05 ms` write P99) vs SQLite `33,256 ops/s` (`7.49 ms`).
- **Space & recovery (1M rows):** `28.15 MB` on disk vs SQLite `29.08 MB`; Relaxed warm reopen `1.65 ms` (Linux) / `3.86 ms` (Windows).

#### Platform Scaling Summary (12 November CI Run `43189115522`)
| Scenario | Engine | Linux x64 | Windows x64 | Apple M4 Pro (local) |
| --- | --- | ---: | ---: | ---: |
| Complex VIP (50k insert + read) | DataVo | 532 ms | 506 ms | 192 ms |
| | DuckDB | 116,020 ms | 134,356 ms | 34,339 ms |
| | SQLite | 234,852 ms | 249,796 ms | 126,064 ms |
| Vector search (10k × 1,536d) | DataVo Flat | 552 ms | 812 ms | 299 ms |
| | SQLite + `sqlite-vec` | 2,276 ms | 2,681 ms | 473 ms |
| Durable update P50 (50k updates) | DataVo Production | 0.223 ms | 2.42 ms | 4.04 ms (20k) |
| | SQLite (`full`) | 0.097 ms | 2.51 ms | 4.04 ms (20k) |

#### Unity IL2CPP Smoke (macOS Standalone, Aggressive Stripping)
`scripts/run-unity-macos-il2cpp-player-smoke.sh` passed on Unity `6000.5.4f1` (universal arm64/x86_64, `UnityLinker --rule-set=Aggressive`, 0 AOT/linker warnings), verifying Unity `Vector2`/`Vector3`/`Vector4`/`Quaternion` storage plus `Lsm` and `Disk` persistence. This is a correctness proof, not a timing benchmark.

#### What Changed Since July 2026 (v0.1 Alpha Audit) {#what-changed-since-july}
- **Strict durability:** July's `502 ms` ("417× faster than SQLite") did not wait for stable storage and was withdrawn; commit `befede4` enforced durable visibility (`84.6 s`, matching SQLite's `84.9 s`).
- **Flat vector allocation:** July's `10.3 MB` was not reproducible; current `61.7 MB` reflects one copy of the 10,000 × 6,144 B input vectors during insert.
- **Disk footprint (1M rows):** Dropped from `66.4 MB` to `27.7 MB` (5% smaller than SQLite) after clustered primary keys landed.
:::

## Methodology & Known Limits

All benchmarks use deterministic seeded datasets, Release builds on .NET 10, and pre-timing answer verification against either DataVo (Part I) or an independent mathematical oracle (`sql-matrix` / `hybrid-vector`).

| Durability Label | What a Commit Waits For | Power-Loss Durable |
| --- | --- | --- |
| **DataVo LSM Production** (`LsmStrictFsync = true`) | WAL reaches stable storage (`fsync`) | Yes |
| **SQLite WAL `full` + `fullfsync=1`** | WAL reaches stable storage (`fsync`) | Yes |
| **DataVo LSM Relaxed** (`LsmStrictFsync = false`) | OS page-cache write only | No |
| **SQLite WAL `NORMAL`** | OS page-cache write only | No |

Only configurations with matching durability should be compared. Key caveats:

- **Managed vs. native memory:** `GC.GetAllocatedBytesForCurrentThread` and `GC.GetTotalAllocatedBytes(precise: true)` measure .NET managed allocations only; SQLite's page cache and DuckDB's native buffers are excluded.
- **Warm OS page cache:** Reopen benchmarks run immediately after writing in the same process; they verify zero-scan open time, not cold-disk boot or crash replay.
- **Single-run variance:** Part I (BenchmarkDotNet) provides 99.9% confidence intervals; Part II and CI scenarios run once per workload, so treat margins under ~20% as noise.
- **API overhead:** SQLite and DuckDB are measured through ADO.NET (the standard .NET client path), while DataVo uses its native compiled LINQ and prepared-statement APIs.

## See Also
- [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite): Detailed v0.4 28-query SQL matrix and hybrid vector guide
- [Reproducing Benchmarks](/manual/performance/reproducing-benchmarks): CLI cookbook to run every benchmark suite locally
- [LSM Storage & Durability](/manual/fundamentals/lsm-and-durability): Configuring `LsmStrictFsync`, compaction, and WAL recovery
