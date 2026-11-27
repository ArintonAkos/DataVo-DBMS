---
title: Benchmark Results & Methodology
description: Microbenchmarks, durability costs, incremental view maintenance, cross-platform CI results, and benchmark methodology for DataVo against SQLite, DuckDB, and LiteDB.
---

# Benchmark Results & Methodology

This page summarizes DataVo's performance across **BenchmarkDotNet microbenchmarks**, **systems workloads** (durability, concurrency, reactive IVM, vector search, and 1M-row SQL), and **cross-platform CI runners** (macOS Apple Silicon, Linux x64, Windows x64), along with the methodology and caveats behind every number.

> **Looking for the SQL & hybrid vector matrix?** See [DataVo vs SQLite: When Each Engine Wins](/manual/performance/datavo-vs-sqlite) for the 28-query 1M-row SQL matrix and 5 filtered vector shapes.

## Executive Summary

Headline rows below come from the **v0.5.1** pass (**27 November 2026**, Apple M4 Pro, `master` @ `fdea08a6`; raw output in `docs/public/benchmarks/2026-11-27-v051/`), except the row-scan row, which keeps the 13 November 2026 full-job baseline. All DataVo rows use the two v0.5 storage modes, `InMemory` and `Lsm`.

| Workload | DataVo Result | Comparison | Run & Notes |
| --- | --- | --- | --- |
| **PK point lookup (100k rows)** | **43.7 ns** (InMemory, 56 B) · **228.6 ns** (LSM, 144 B) | SQLite: 710 ns / 1,610 ns (608 B) · DuckDB 1t: 72.9–73.3 µs | M4 Pro (v0.5.1, 27 Nov 2026, full job), compiled LINQ (`FlatRowStore` compact layout) |
| **Filtered aggregates (200k rows, `[Columnar]`)** | **27.5–93.7 µs** (0 B alloc, InMemory & LSM) | SQLite: 3,712–5,741 µs · DuckDB 1t: 138–714 µs | M4 Pro (v0.5.1, 27 Nov 2026, full job); 2.6–8.8× faster than 1-thread DuckDB, 54–165× faster than SQLite |
| **Filtered aggregates (200k rows, row scan)** | **386–526 µs** (InMemory, 0 B) · **2.0–2.3 ms** (LSM) | SQLite: 3.7–5.9 ms · DuckDB 1t: 137–716 µs | M4 Pro (`ce818d4`, 13 Nov 2026 full-job baseline); 8.8–9.8× faster than SQLite on M4 Pro, 3.5–4.7× on x64 CI |
| **Reactive IVM (3-table join, 50k updates)** | **238.0 ms** (4.0 µs P50) | DuckDB: 39.4 s (166×) · SQLite: 128.8 s (541×) | M4 Pro (v0.5.1, 27 Nov 2026); incremental Z-set updates vs. full-query polling |
| **Durable commits (`fsync` per update)** | **4.07 ms** P50 (Mac) · **2.57 ms** (Win) · **0.163 ms** (Linux) | SQLite `full+fullfsync`: 4.10 ms (Mac) · 2.66 ms (Win) · 0.079 ms (Linux) | v0.5.1 (27 Nov 2026, local + CI); hardware flush dominates on Mac/Win, SQLite leads 2.1× on the fast-flush Linux VM |
| **Concurrent ops (8 readers + 2 writers)** | **2.40M ops/s** (0.014 ms write P99) | SQLite WAL `NORMAL`: 355.4k ops/s (1.18 ms write P99) | M4 Pro (v0.5.1, 27 Nov 2026); 6.7× throughput, 26× more writes under continuous reads |
| **Relaxed YCSB 80/20 (12 threads)** | **346,866 ops/s** (1.06 ms write P99) | SQLite WAL `NORMAL`: 226,278 ops/s (3.40 ms write P99) | M4 Pro (v0.5.1, 27 Nov 2026); 3.2× lower write tail. Throughput varied 182k–1.05M ops/s over four runs (SQLite 226k–240k), so the throughput lead is not established; neither mode is power-loss durable |
| **Disk footprint (1M rows)** | **27.7 MB** (LSM) | SQLite: 29.1 MB · LiteDB: 154.5 MB | M4 Pro (v0.5.1, 27 Nov 2026); 5% smaller than SQLite, warm reopen in 0.8–2.7 ms |

## Part I: BenchmarkDotNet Microbenchmarks

Measured with `benchmarks/DataVo.Benchmarks` (Apple M4 Pro, .NET 10, 99.9% CI half-width ≤ 3% for DataVo/SQLite). Every benchmark verifies identical query answers across all engines before timing. The expanded tables and plots below are the **v0.5.1** full-job run (**27 November 2026**, `master` @ `fdea08a6`, `InMemory` and `Lsm` storage; raw reports in `docs/public/benchmarks/2026-11-27-v051/bdn/`), except the row-scan table, which keeps the **13 November 2026** full-job baseline (`ce818d4`) because the current suite measures the `[Columnar]` path.

![Filtered aggregates over 200,000 rows](/benchmarks/bdn-filtered-aggregates.png)

![Point lookup and bulk insert](/benchmarks/bdn-lookup-and-insert.png)

::: details Full Part I Tables (v0.5.1, 27 Nov 2026: Columnar Index, Out-of-Core SSTable, Point Lookup, Bulk Load; Row Scan from 13 Nov 2026)
#### Filtered Aggregates (Row Scan, 200,000 rows, mean µs, 0 B alloc for DataVo; 13 Nov 2026 baseline)
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
| `COUNT(*)` | Memory | **27.5** | 3,712 | 138 | 227 |
| `SUM(Gold)` | Memory | **78.1** | 4,731 | 203 | 239 |
| `MIN(Gold)` | Memory | **72.9** | 4,594 | 199 | 231 |
| `MAX(Ratio)` | Memory | **91.1** | 4,943 | 437 | 415 |
| `COUNT(*)` | File (LSM) | **27.8** | 4,571 | 245 | 270 |
| `SUM(Gold)` | File (LSM) | **78.5** | 5,541 | 365 | 330 |
| `MIN(Gold)` | File (LSM) | **73.5** | 5,367 | 358 | 379 |
| `MAX(Ratio)` | File (LSM) | **93.7** | 5,741 | 714 | 565 |

#### Out-of-Core Columnar Scans from SSTable Files (1,000,000 rows, LSM, mean µs, 0 B alloc)
| Query | Columns in Memory | Columns in SSTable Files (mmap) |
| --- | ---: | ---: |
| `COUNT(*)` | 137.1 | 137.0 |
| `SUM(Gold)` | 389.5 | 390.2 |
| `MIN(Gold)` | 367.5 | 364.8 |
| `MAX(Ratio)` | 454.9 | 454.5 |

Opening the flushed 1,000,000-row database and answering the first `SUM` takes **12.95 ms** (178 KB allocated).

#### Point Lookup by Primary Key (100,000 rows, shuffled keys)
| Storage | DataVo | SQLite | DuckDB (1t) | DuckDB | DataVo alloc | SQLite alloc | DuckDB alloc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | **43.7 ns** | 710.2 ns | 72,918 ns | 80,557 ns | **56 B** | 608 B | 2,144 B |
| File (LSM) | **228.6 ns** | 1,610.4 ns | 73,327 ns | 82,529 ns | **144 B** | 608 B | 2,144 B |

#### Loading 100,000 Rows (10 timed runs ± SD)
| Storage | DataVo | SQLite | DuckDB (1t) | DuckDB | DataVo alloc | SQLite alloc | DuckDB alloc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 38.6 ± 14.5 ms | 62.7 ± 0.3 ms | 31.1 ± 5.0 ms | 29.7 ± 7.0 ms | 52 MB | 94 MB | 19 MB |
| File (LSM) | 119.1 ± 44.5 ms | 77.0 ± 28.5 ms | 28.1 ± 6.2 ms | 28.4 ± 6.1 ms | 75 MB | 94 MB | 19 MB |

Bulk loads are single runs of a cold 100,000-row insert (`RunStrategy=Monitoring`), so their spread is wide: the DataVo `File` median is 126.1 ms. Treat the load rows as indicative, not as a ranking.
:::

## Part II: Systems Workloads

Application-shaped scenarios from `demos/Research.Benchmark` (v0.5.1 pass, 27 November 2026, Apple M4 Pro, `master` @ `fdea08a6`; single run per scenario, so treat differences under ~20% as not established).

### Streamed Risk Views (Incremental View Maintenance)

Inserting 50,000 orders one-by-one and reading a live dashboard after each insert. DataVo maintains the view incrementally via Z-sets; SQLite and DuckDB re-run indexed queries.

![Streamed risk, complex VIP exposure](/benchmarks/ivm-complex-vip-total-time.png)

![Streamed risk, simple exposure](/benchmarks/ivm-simple-exposure-total-time.png)

- **Complex VIP 3-table join:** DataVo finishes in **238.0 ms** (4.0 µs P50), **166×** faster than DuckDB (`39.4 s`) and **541×** faster than SQLite (`128.8 s`), because incremental work is O(1) per order rather than O(N) re-joining.
- **Simple single-table exposure:** DataVo (`563.2 ms`, 7.3 µs P50) and SQLite (`646.2 ms`, 9.8 µs P50) are closely matched (1.15× in this run), while DuckDB pays per-query setup overhead (`57.0 s`).

### Durability, Concurrency & Space

![Disk CRUD total time](/benchmarks/disk-crud-wal-time.png)

![Thread scaling](/benchmarks/thread-scaling-throughput.png)

![YCSB write P99](/benchmarks/ycsb-write-p99.png)

- **Strict durability (`fsync` per commit):** DataVo LSM Production (`87.0 s`, `4.07 ms` P50) and SQLite `WAL + full + fullfsync` (`88.2 s`, `4.10 ms` P50) tie on Apple Silicon because both wait on the same SSD cache flush. Group commit scales Production throughput **5.6×** from 1 to 16 threads (`2,339` → `13,028` ops/s) and **4.8×** at 32 threads.
- **Relaxed durability (OS buffer):** DataVo Relaxed beats SQLite WAL `NORMAL` by **2.3×** on serial updates (`90 ms` vs `211 ms`). On 12-thread YCSB 80/20 its write P99 is **3.2×** lower (`1.06 ms` vs `3.40 ms`; `0.77–1.25 ms` vs `3.41 ms` across three reruns), but throughput is unstable: `346.9k` ops/s in the main run and `182k`–`1.05M` in the reruns, against a steady `226k`–`240k` for SQLite. The raw reruns are in `ycsb-mixed-reruns.md`.

### Vector Search (10,000 × 1,536d)

![Vector search total time](/benchmarks/vector-search-total-time.png)

![HNSW versus Flat](/benchmarks/vector-hnsw-vs-flat.png)

- On **uniform random** 1,536d vectors at 10k scale, exact **Flat** search (`301.0 ms` total, `2.00 ms` P50) beats both `sqlite-vec` (`460.4 ms`) and HNSW (`2,096 ms` total, 62.3% recall) because uniform high-dimensional distances concentrate.
- On **clustered** embeddings (realistic workloads), HNSW is **7.3×** faster per query at 10k vectors (`0.27 ms`, 100% recall) and **17×** faster at 50k (`0.59 ms`, 99.8% recall), with `enable_diversity_heuristic` on by default since v0.4.

::: details Full Part II Raw Tables (IVM, Disk CRUD, Thread Scaling, YCSB, Space, Vector Recall, Flat/Doc/Concurrent)
#### Streamed Risk Views (50,000 insert + read iterations)
| Profile | Engine | Total (ms) | P50 | P99 | GC Allocated |
| --- | --- | ---: | ---: | ---: | ---: |
| Simple Exposure | DataVo | 563.2 | 7.3 µs | 20.8 µs | 356.7 MB |
| Simple Exposure | SQLite | 646.2 | 9.8 µs | 34.7 µs | 200.0 MB |
| Simple Exposure | DuckDB | 57,012.6 | 1,029 µs | 4,400 µs | 208.3 MB |
| Complex VIP Join | DataVo | **238.0** | **4.0 µs** | **8.3 µs** | 77.0 MB |
| Complex VIP Join | DuckDB | 39,408.1 | 706 µs | 3,028 µs | 131.3 MB |
| Complex VIP Join | SQLite | 128,794.5 | 2,544 µs | 4,469 µs | 115.6 MB |

#### Disk CRUD WAL (20,000 inserts + 20,000 committed updates)
| Engine | Total (ms) | P50 | P99 | GC Allocated |
| --- | ---: | ---: | ---: | ---: |
| DataVo (LSM Production) | 87,048 | 4.07 ms | 5.88 ms | 30.8 MB |
| SQLite (WAL, full, `fullfsync`) | 88,233 | 4.10 ms | 6.06 ms | 22.3 MB |
| DataVo (LSM Relaxed) | 90 | 2.8 µs | 10.0 µs | 22.9 MB |
| SQLite (WAL, normal) | 211 | 6.9 µs | 25.3 µs | 23.2 MB |

#### Thread Scaling (ops/s, 100k reads + 10k updates)
| Threads | DataVo Production | DataVo Relaxed | SQLite (WAL, normal) | LiteDB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2,339 | 1,633,223 | 318,828 | 49,059 |
| 4 | 4,803 | 1,012,395 | 443,080 | 66,005 |
| 8 | 7,363 | 526,350 | 453,386 | 62,765 |
| 16 | 13,028 | 1,579,126 | 523,953 | 63,832 |
| 32 | 11,159 | 1,483,774 | 565,621 | 61,037 |

Relaxed runs finish in 57–209 ms, so single-run Relaxed points (such as the 8-thread dip) are within noise.

#### YCSB Mixed 80/20 (12 threads, 100k ops)
| Engine | Total (ms) | Ops/s | Read P99 | Write P99 | GC Allocated |
| --- | ---: | ---: | ---: | ---: | ---: |
| DataVo (LSM Relaxed) | 288.3 | 346,866 | 1.0 µs | 1.06 ms | 34.7 MB |
| SQLite (WAL, normal) | 441.9 | 226,278 | 9.4 µs | 3.40 ms | 57.0 MB |
| LiteDB | 2,730.5 | 36,624 | 2.68 ms | 2.96 ms | 4,314.3 MB |
| DataVo (LSM Production) | 25,089.9 | 3,986 | 27.6 µs | 36.06 ms | 37.2 MB |

Three extra runs gave DataVo Relaxed 1,052,868 / 182,047 / 205,380 ops/s and SQLite 237,355 / 229,136 / 239,616 ops/s (`ycsb-mixed-reruns.md`).

#### Space & Warm Reopen (1,000,000 records)
| Engine | Insert (ms) | Disk (MB) | Reopen (ms) | First Read (ms) | GC Allocated |
| --- | ---: | ---: | ---: | ---: | ---: |
| DataVo (LSM Production) | 693.1 | 27.7 | 2.7 | 3.3 | 411.6 MB |
| DataVo (LSM Relaxed) | 391.3 | 27.7 | 0.83 | 0.0 | 345.0 MB |
| SQLite (WAL, normal) | 725.1 | 29.1 | 0.37 | 1.3 | 633.3 MB |
| LiteDB | 3,053.4 | 154.5 | 0.77 | 10.4 | 15,106.3 MB |

#### Vector Search (10k × 1,536d, 100 top-10 queries) & Held-Out Recall
| Engine | Total (ms) | Query P50 | Query P99 | GC Allocated |
| --- | ---: | ---: | ---: | ---: |
| DataVo-Flat | 301.0 | 2.00 ms | 2.61 ms | 60.9 MB |
| SQLite (`sqlite-vec`) | 460.4 | 2.38 ms | 3.58 ms | 63.5 MB |
| DataVo (HNSW) | 2,096.1 | 2.01 ms | 2.61 ms | 61.9 MB |
| LiteDB | 78,487.7 | 760.8 ms | 832.4 ms | 208,914.6 MB |

| N | Distribution | Flat Query | HNSW Query | HNSW Recall@10 | HNSW Build | Flat Build |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | uniform | 0.22 ms | 0.52 ms | 99.8% | 159 ms | 28 ms |
| 10,000 | uniform | 2.04 ms | 1.98 ms | 62.3% | 1,723 ms | 70 ms |
| 50,000 | uniform | 10.1 ms | 3.05 ms | 24.1% | 11,957 ms | 164 ms |
| 1,000 | clustered | 0.22 ms | 0.37 ms | 100% | 95 ms | 10 ms |
| 10,000 | clustered | 2.00 ms | 0.27 ms | 100% | 728 ms | 29 ms |
| 50,000 | clustered | 10.0 ms | 0.59 ms | 99.8% | 5,348 ms | 218 ms |

#### Other Workloads (Flat CRUD, Deep Document, Concurrent Ops)
- **Flat CRUD (50k insert + 50k lookup, in-memory):** DataVo `84.8 ms` (`0.79 µs` P50) vs SQLite `128.9 ms` (`1.17 µs`) vs LiteDB `1,322.9 ms`.
- **Deep document (5k nested orders):** DataVo `93.7 ms` (`6.8 µs` P50) vs SQLite `130.7 ms` (`7.8 µs`) vs LiteDB `415.0 ms`.
- **Concurrent ops (8 readers + 2 writers, 5 s):** DataVo `2,395,343 ops/s` (`2,282,173` writes, `7.9 µs` read P99, `0.014 ms` write P99, 2.6 GB managed allocation) vs SQLite WAL `NORMAL` `355,420 ops/s` (`87,348` writes, `151 µs` read P99, `1.18 ms` write P99). In-memory shared-cache SQLite completed only `2` writes in the same 5 s (shared-cache mode takes table-level locks, so continuous readers block the writers). v0.5.0 (25 Nov) gave DataVo `2,015,234 ops/s` with `666,421` writes and 7.3 GB allocated.
:::

## SQL over 1,000,000 rows {#sql-over-1-000-000-rows}

The `sql-workload` scenario runs prepared SQL text over 1,000,000 file-backed rows (DataVo LSM Relaxed vs. SQLite WAL `NORMAL`, DuckDB, and LiteDB).

Range-scan latency differs between the two suites: `sql-matrix` R1/R2 measure 100-row clustered-key ranges at **13.5–14.0 µs** (SQLite 18.3–18.5 µs), while `sql-workload` measures a range on a different table shape. See [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite) for all 28 SQL shapes.

::: details v0.5.1 `sql-workload` Table (27 November 2026, Apple M4 Pro, `master` @ `fdea08a6`)
Median per call (single steps are totals). Every operation returned identical results on all engines.

| Operation | DataVo (LSM Relaxed) | SQLite | DuckDB | LiteDB |
| --- | ---: | ---: | ---: | ---: |
| Load orders and customers | 1.91 s | 2.66 s | 517.4 ms | 8.18 s |
| Load GUID-keyed rows | 840.6 ms | 2.92 s | 649.8 ms | 6.20 s |
| Point read by INT key | 1.2 µs | 2.0 µs | 146.0 µs | 23.1 µs |
| Point read by GUID key | 1.2 µs | 2.3 µs | 124.2 µs | 25.3 µs |
| Secondary-index lookup (~100 rows) | 114.7 µs | 90.3 µs | 274.0 µs | 416.0 µs |
| Key range scan (100 rows) | 47.0 µs | 7.4 µs | 170.6 µs | 531.5 µs |
| `COUNT` with filter (full scan) | 36.8 ms | 25.1 ms | 0.42 ms | 382.4 ms |
| `GROUP BY` (full scan) | 52.8 ms | 217.8 ms | 0.96 ms | 1.86 s |
| `JOIN` + `GROUP BY` | 100.3 ms | 343.1 ms | 1.5 ms | 446.6 ms |
| `ORDER BY ... LIMIT 10` | 52.8 ms | 31.9 ms | 1.1 ms | 836.1 ms |
| `UPDATE` by key (autocommit) | 5.8 µs | 6.0 µs | 172.5 µs | 43.5 µs |
| `DELETE` by key (autocommit) | 10.9 µs | 9.1 µs | 173.2 µs | 32.2 µs |
| Close (flush to disk) | 296.3 ms | 3.7 ms | 175.2 ms | 10.4 ms |
| Size on disk | 73.2 MB | 75.3 MB | 99.1 MB | 384.2 MB |
| Reopen and first point read | 3.4 ms | 0.4 ms | 4.7 ms | 0.4 ms |
| Point read after reopen | 1.0 µs | 2.4 µs | 144.0 µs | 14.7 µs |
:::

## Part III: Cross-Platform CI Results {#part-iii-platforms}

To verify portability beyond Apple Silicon, the full suite runs on GitHub Actions (`ubuntu-latest` x64, `windows-latest` x64, and a `macos-latest` M1 VM). Latest refresh: **27 November 2026** (v0.5.1): research matrix run `46022086299` (`master` @ `1fb751d9`, which differs from `fdea08a6` only in the workflow file) and BenchmarkDotNet run `46021368594` (`fdea08a6`; Linux on an Intel Xeon Platinum 8370C, Windows on an AMD EPYC 9V45, both 4 vCPU), with the macOS M1 BenchmarkDotNet job re-run as `46034724009` after the first one hit its 90-minute timeout. Raw logs and reports: `docs/public/benchmarks/2026-11-27-v051/ci/`. The research and BenchmarkDotNet jobs run on separate VMs, so their hardware can differ.

![Mean time of a filtered COUNT and of a primary-key lookup across four machines](/benchmarks/ci-bdn-by-platform.png)

![Median latency of one durable update on three platforms](/benchmarks/ci-durable-update-latency.png)

![Total time of the vector scenario on three platforms](/benchmarks/ci-vector-search-time.png)

Key cross-platform findings:

- **Point lookups:** Linux x64 `93.0 ns` (Memory, 56 B) / `281.3 ns` (LSM, 144 B) vs. SQLite `1,396 ns` / `3,840 ns`. Windows x64 `296.5 ns` / `749.8 ns` vs. SQLite `2,063 ns` / `6,828 ns`. DataVo is **7.0–15×** faster on both x64 platforms.
- **Columnar aggregates (200k rows, 0 B alloc):** Linux x64 `8.7–8.9 µs` (`COUNT`) and `28.3–40.5 µs` (`SUM`/`MIN`/`MAX`); Windows x64 `16.1 µs` and `91.0–109.9 µs`. `COUNT(*)` is **480–826×** faster than SQLite, and every aggregate beats 1-thread DuckDB (**17–59×** on Linux, **10–71×** on Windows).
- **1M-row SQL matrix:** every engine returned the oracle's answer on both platforms. On Linux the SQLite / DataVo LSM ratios stay close to Apple Silicon: joins **2.3–9.7×** faster, `GROUP BY` (4–1,000 groups) **2.5–4.0×**, unindexed scans and `ORDER BY ... LIMIT` **1.1–1.5×** slower. On Windows DataVo's join lead widens to **3.4–22×**.
- **Concurrent ops (8 readers + 2 writers, 5 s):** writers now make progress on both 4-vCPU runners. Linux: DataVo `1.57M ops/s` (`1,180,510` writes, `0.020 ms` write P99) vs SQLite WAL `NORMAL` `825k ops/s` (`16,622` writes, `6.45 ms`). Windows: DataVo `1.22M ops/s` (`570,524` writes, `0.076 ms` write P99) vs SQLite WAL `NORMAL` `138k ops/s` (`19,200` writes, `10.5 ms`). The 21 November run had recorded `0` DataVo writes on Windows; the cause was the benchmark harness (thread-pool workers starving the writers and the end-of-run timer), fixed in v0.5 with dedicated threads, and `WriterProgressUnderReadLoadTests` pins writer progress under oversubscribed reads.
- **Durability differences:** On the Linux VM, with fast virtual-disk flushes, SQLite's durable update (`0.079 ms` P50) leads DataVo Production (`0.163 ms`) by 2.1×. On Windows (`2.57` vs `2.66 ms`) and macOS (`4.07` vs `4.10 ms`) the hardware flush dominates and both engines tie.
- **SIMD paths:** `SimdDistanceKernels` auto-dispatches AVX2 (256-bit) on x64, AdvSimd/NEON (128-bit) on Arm64, and `TensorPrimitives` / `Vector<T>` on portable targets.

::: details Cross-Platform CI Tables, Unity IL2CPP Smoke & Historical Audit (July → November)
#### Platform Scaling Summary (v0.5.1, 27 November 2026)
| Scenario | Engine | Linux x64 | Windows x64 | Apple M4 Pro (local) |
| --- | --- | ---: | ---: | ---: |
| Complex VIP (50k insert + read) | DataVo | 423 ms | 759 ms | 238 ms |
| | DuckDB | 86,624 ms | 133,073 ms | 39,408 ms |
| | SQLite | 179,066 ms | 245,945 ms | 128,794 ms |
| Vector search (10k × 1,536d) | DataVo Flat | 389 ms | 865 ms | 301 ms |
| | SQLite + `sqlite-vec` | 2,035 ms | 2,675 ms | 460 ms |
| Durable update P50 | DataVo Production | 0.163 ms (50k) | 2.57 ms (50k) | 4.07 ms (20k) |
| | SQLite (`full`) | 0.079 ms (50k) | 2.66 ms (50k) | 4.10 ms (20k) |
| YCSB 80/20 Relaxed | DataVo | 803,301 ops/s (`0.15 ms` write P99, 4 threads) | 277,875 ops/s (`0.34 ms`, 4 threads) | 346,866 ops/s (`1.06 ms`, 12 threads) |
| | SQLite WAL `NORMAL` | 113,283 ops/s (`3.17 ms`) | 40,471 ops/s (`1.87 ms`) | 226,278 ops/s (`3.40 ms`) |
| Space (1M rows) | DataVo LSM | 28.2 MB, Relaxed reopen 1.3 ms | 28.2 MB, Relaxed reopen 3.5 ms | 27.7 MB, Relaxed reopen 0.8 ms |
| | SQLite WAL `NORMAL` | 29.1 MB, reopen 0.57 ms | 29.1 MB, reopen 1.9 ms | 29.1 MB, reopen 0.37 ms |

#### Unity IL2CPP Smoke (macOS Standalone, Aggressive Stripping)
`scripts/run-unity-macos-il2cpp-player-smoke.sh` passed on Unity `6000.5.4f1` (universal arm64/x86_64, `UnityLinker --rule-set=Aggressive`, 0 AOT/linker warnings), verifying Unity `Vector2`/`Vector3`/`Vector4`/`Quaternion` storage plus `Lsm` persistence (the run predates v0.5, which removed the `Disk` mode). This is a correctness proof, not a timing benchmark.

#### What Changed Since July 2026 (v0.1 Alpha Audit) {#what-changed-since-july}
- **Strict durability:** July's `502 ms` ("417× faster than SQLite") did not wait for stable storage and was withdrawn; commit `befede4` enforced durable visibility (now `87.0 s`, matching SQLite's `88.2 s`).
- **Flat vector allocation:** July's `10.3 MB` was not reproducible; current `60.9 MB` reflects one copy of the 10,000 × 6,144 B input vectors during insert.
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
- [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite): Detailed 28-query SQL matrix and hybrid vector guide (v0.5.1 run)
- [Reproducing Benchmarks](/manual/performance/reproducing-benchmarks): CLI cookbook to run every benchmark suite locally
- [LSM Storage & Durability](/manual/fundamentals/lsm-and-durability): Configuring `LsmStrictFsync`, compaction, and WAL recovery
