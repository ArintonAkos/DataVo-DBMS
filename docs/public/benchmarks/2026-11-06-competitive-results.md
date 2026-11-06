# Competitive benchmark results (2026-11-06)

Machine: Apple Silicon Mac, macOS, .NET 10, Release build of master + the SQL workload. Every engine runs on a real file in a temp folder.
Reproduce with `dotnet demos/Research.Benchmark/src/Research.Benchmark.Host/bin/Release/net10.0/Research.Benchmark.Host.dll --scenario <name>`.

Durability labels: *LSM Production* and *SQLite (WAL,full)* fsync every commit; *LSM Relaxed* and *SQLite (WAL,normal)* do not.

## sql-workload:, 1,000,000 orders (10,000 customers, 1,000,000 GUID rows)

| Operation | DataVo (LSM) [DataVo.Core net10.0] | SQLite | DuckDB | LiteDB |
|---|---|---|---|---|
| Load orders + customers | 1.78 s | 2.61 s | 503.1 ms | 7.97 s |
| Load GUID-keyed rows | 769.5 ms | 2.76 s | 584.1 ms | 5.59 s |
| Point read by INT key | 21.9 µs (p99 58.2 µs) | 1.8 µs (p99 2.5 µs) | 149.2 µs (p99 190.8 µs) | 22.0 µs (p99 38.0 µs) |
| Point read by GUID key | 16.5 µs (p99 26.9 µs) | 2.0 µs (p99 2.8 µs) | 131.3 µs (p99 164.1 µs) | 23.6 µs (p99 43.9 µs) |
| Secondary-index lookup (~100 rows) | 232.1 µs (p99 464.4 µs) | 76.1 µs (p99 103.0 µs) | 271.4 µs (p99 365.4 µs) | 392.1 µs (p99 2.3 ms) |
| Key range scan (100 rows) | 1.68 s (p99 2.04 s) | 6.8 µs (p99 8.6 µs) | 180.1 µs (p99 226.3 µs) | 505.3 µs (p99 2.4 ms) |
| COUNT with filter (full scan) | 1.00 s (p99 1.12 s) | 24.0 ms (p99 24.6 ms) | 422.0 µs (p99 1.7 ms) | 388.9 ms (p99 458.0 ms) |
| GROUP BY (full scan) | 2.20 s (p99 2.29 s) | 207.9 ms (p99 208.9 ms) | 985.4 µs (p99 1.6 ms) | 1.72 s (p99 1.82 s) |
| JOIN + GROUP BY | 3.68 s (p99 3.77 s) | 329.0 ms (p99 329.8 ms) | 1.5 ms (p99 1.7 ms) | 435.9 ms (p99 447.0 ms) |
| ORDER BY ... LIMIT 10 | 3.13 s (p99 3.29 s) | 30.4 ms (p99 31.3 ms) | 1.1 ms (p99 1.4 ms) | 817.7 ms (p99 842.6 ms) |
| UPDATE by key (autocommit) | 28.2 µs (p99 44.6 µs) | 5.5 µs (p99 20.2 µs) | 168.9 µs (p99 221.6 µs) | 39.5 µs (p99 233.2 µs) |
| DELETE by key (autocommit) | 14.8 µs (p99 20.6 µs) | 7.9 µs (p99 20.8 µs) | 172.5 µs (p99 324.7 µs) | 31.1 µs (p99 256.5 µs) |
| Close (flush to disk) | 414.9 ms | 2.8 ms | 160.0 ms | 9.3 ms |
| Size on disk (MB) | 73.3 MB | 75.3 MB | 99.1 MB | 384.2 MB |
| Reopen + first point read | 4.2 ms | 0.4 ms | 4.5 ms | 0.3 ms |
| Point read after reopen | 7.0 µs (p99 19.0 µs) | 2.0 µs (p99 2.8 µs) | 144.8 µs (p99 236.0 µs) | 14.1 µs (p99 223.3 µs) |

Repeated operations show the median and p99 latency of one call; single steps show total time.

- **DataVo (LSM) [DataVo.Core net10.0]**: LSM mode, commit log without fsync per commit; queries as SQL text (parsed on every call), bulk load via InsertTypedBatch.
- **SQLite**: WAL, synchronous=NORMAL; prepared statements with parameters; bulk load in one transaction; GUIDs as 16-byte BLOB keys in a WITHOUT ROWID table.
- **DuckDB**: File database with its default WAL; appender for bulk load; prepared statements with parameters; GUIDs as UUID.
- **LiteDB**: Default journal; BsonDocument API with InsertBulk; GROUP BY and JOIN done in application code (LiteDB has no relational joins).

Result check (the same query must give the same answer everywhere):

- every other operation returned identical results on all engines.

## ycsb-mixed

| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|
| DataVo (LSM Production) [DataVo.Core net10.0] | 19480.961 | 5133.217 | 0.013375 | 24.127042 | 60.188 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] | 430.606 | 232230.748 | 0.000959 | 0.812583 | 57.711 |
| SQLite (WAL,normal) | 418.021 | 239222.431 | 0.007958 | 3.643875 | 57.343 |
| LiteDB | 2598.297 | 38486.744 | 2.213459 | 2.636917 | 4318.669 |

## space-recovery

| Engine Name | Insert Execution Time (ms) | Disk Size (MB) | Recovery Time (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|
| DataVo (LSM Production) [DataVo.Core net10.0] | 815.901 | 27.725 | 4.129 | 387.196 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] | 424.693 | 27.725 | 1.248 | 347.383 |
| SQLite (WAL,normal) | 715.099 | 29.082 | 0.383 | 633.243 |
| LiteDB | 2914.000 | 154.461 | 0.665 | 15106.530 |

## thread-scaling

| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|
| DataVo (LSM Production) [DataVo.Core net10.0] (1 threads) | 40325.716 | 2727.788 | n/a | n/a | 54.340 |
| DataVo (LSM Production) [DataVo.Core net10.0] (2 threads) | 36052.263 | 3051.126 | n/a | n/a | 51.770 |
| DataVo (LSM Production) [DataVo.Core net10.0] (4 threads) | 19920.350 | 5521.991 | n/a | n/a | 52.523 |
| DataVo (LSM Production) [DataVo.Core net10.0] (8 threads) | 12236.094 | 8989.797 | n/a | n/a | 51.465 |
| DataVo (LSM Production) [DataVo.Core net10.0] (16 threads) | 11245.630 | 9781.577 | n/a | n/a | 52.469 |
| DataVo (LSM Production) [DataVo.Core net10.0] (32 threads) | 8514.585 | 12919.010 | n/a | n/a | 51.470 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] (1 threads) | 98.392 | 1117973.663 | n/a | n/a | 52.839 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] (2 threads) | 80.219 | 1371246.213 | n/a | n/a | 50.281 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] (4 threads) | 86.018 | 1278806.571 | n/a | n/a | 50.281 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] (8 threads) | 84.727 | 1298281.312 | n/a | n/a | 50.282 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] (16 threads) | 87.354 | 1259249.762 | n/a | n/a | 50.284 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] (32 threads) | 91.949 | 1196314.048 | n/a | n/a | 50.285 |
| SQLite (WAL,normal) (1 threads) | 312.774 | 351691.187 | n/a | n/a | 64.201 |
| SQLite (WAL,normal) (2 threads) | 190.200 | 578337.375 | n/a | n/a | 62.036 |
| SQLite (WAL,normal) (4 threads) | 180.802 | 608399.004 | n/a | n/a | 62.041 |
| SQLite (WAL,normal) (8 threads) | 189.435 | 580674.110 | n/a | n/a | 62.052 |
| SQLite (WAL,normal) (16 threads) | 185.012 | 594554.422 | n/a | n/a | 62.064 |
| SQLite (WAL,normal) (32 threads) | 181.578 | 605798.928 | n/a | n/a | 62.035 |
| LiteDB (1 threads) | 2167.713 | 50744.737 | n/a | n/a | 4416.114 |
| LiteDB (2 threads) | 1566.484 | 70220.976 | n/a | n/a | 4351.378 |
| LiteDB (4 threads) | 1598.092 | 68832.061 | n/a | n/a | 4351.579 |
| LiteDB (8 threads) | 1608.674 | 68379.320 | n/a | n/a | 4351.241 |
| LiteDB (16 threads) | 1619.803 | 67909.509 | n/a | n/a | 4352.654 |
| LiteDB (32 threads) | 1670.831 | 65835.511 | n/a | n/a | 4352.178 |

## disk-crud-durable

| Engine Name | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|
| DataVo (LSM Production) [DataVo.Core net10.0] | 80838.897 | 4.004916 | 4.986958 | 41.924 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] | 119.104 | 0.004459 | 0.007000 | 33.965 |
| SQLite (WAL,normal) | 158.840 | 0.005625 | 0.008667 | 23.210 |
| SQLite (WAL,full) | 81162.422 | 4.004084 | 5.087667 | 22.298 |

## concurrent-ops

| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|
| DataVo [DataVo.Core net10.0] | 10001.827 | 2117259.056 | 0.043959 | 0.118042 | 9316.213 |
| SQLite | 10118.263 | 393989.265 | 0.113500 | 10117.240458 | 2190.906 |

## flat-crud

| Engine Name | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Insert GC Allocated (MB) | Lookup GC Allocated (MB) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|---:|
| DataVo [DataVo.Core net10.0] | 62.587 | 0.000541 | 0.000708 | 20.667 | 2.303 | 22.969 |
| LiteDB | 1315.949 | 0.017333 | 0.022584 | 637.801 | 1862.217 | 2500.018 |
| SQLite | 126.189 | 0.001125 | 0.001583 | 31.296 | 30.834 | 62.131 |

## vector-search

| Engine Name | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|
| DataVo [DataVo.Core net10.0] | 1108.765 | 0.827542 | 1.168625 | 33.440 |
| DataVo-HNSW-Diversity [DataVo.Core net10.0] | 5849.088 | 0.757875 | 0.952625 | 32.573 |
| DataVo-Flat [DataVo.Core net10.0] | 217.396 | 0.814417 | 1.205459 | 32.431 |
| LiteDB | 78356.183 | 384.937625 | 401.443958 | 208661.601 |
| SQLite | 358.091 | 1.192500 | 1.287041 | 34.240 |

## complex-vip

| Engine Name | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|
| DataVo [DataVo.Core net10.0] | 27.375 | 0.002708 | 0.003750 | 10.417 |
| DuckDB | 3319.237 | 0.619417 | 0.940667 | 13.126 |
| SQLite | 4106.970 | 0.811041 | 0.994333 | 11.588 |

## Reading notes

- **sql-workload** runs every query as SQL text in DataVo (parsed on each call); SQLite and DuckDB use prepared statements. All engines returned identical answers for every query.
- **DataVo key range scans and full-scan queries are the weak spot.** `Id >= a AND Id < b` does not use the key order (`StatementEvaluatorWOJoin.HandleNonIndexableStatement` scans the table), and scans materialize each row as a dictionary. That is why a 100-row range takes 1.7 s at 1M rows.
- **ycsb-mixed / thread-scaling** compare *LSM Production* (fsync per commit) with *SQLite (WAL,normal)* (no fsync), which is not like-for-like. *LSM Relaxed* is the matching row.
- **concurrent-ops**: SQLite's 10 s write p99 is writers waiting on its single write lock (busy timeout).
- **flat-crud** runs DataVo in memory.
- **vector-search** (10k × 768): sqlite-vec is brute force, so its "build" is only inserts; DataVo HNSW pays the graph build up front and answers queries faster.
