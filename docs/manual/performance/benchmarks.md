# Benchmark Results

This page reports measurements of DataVo 0.2.0-rc.3 against SQLite, DuckDB and LiteDB, taken on 12 November 2026 on one machine. It replaces the July 2026 (v0.1 alpha) page, whose numbers no longer describe the engine. Several of them could not be reproduced, and one comparison it made was not valid. [What changed since July](#what-changed-since-july) lists these.

Every number below comes from a file in [`docs/public/benchmarks/2026-11-12/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-12). The charts are generated from those files by `docs/scripts/generate-benchmark-plots.py`.

## Summary

| Claim | Evidence | Condition |
| --- | --- | --- |
| DataVo answers a filtered `COUNT`/`SUM`/`MIN`/`MAX` over 200,000 in-memory rows 9 to 10 times faster than SQLite, allocating nothing. | [Part I](#filtered-aggregates) | Compiled LINQ query against SQLite through ADO.NET. |
| DuckDB is still 1.2 to 2.8 times faster than DataVo on the same in-memory aggregates when held to one thread, and 3 to 8 times faster on file-backed data. | [Part I](#filtered-aggregates) | Column store against row store. |
| A primary-key lookup takes 242 ns on DataVo's LSM engine and 1,628 ns on SQLite. DuckDB takes about 75 µs. | [Part I](#point-lookup-by-primary-key) | 100,000 rows, shuffled keys. |
| With a device flush per commit, DataVo and SQLite cost the same: about 4.0 ms per durable update. | [Part II](#durability-what-an-fsync-costs) | Both flush the drive's cache on every commit. |
| Without that flush, DataVo (Relaxed) is 1.4 times faster than SQLite (WAL, normal) on serial updates and 1.95 times faster on a 12-thread mixed workload. | [Part II](#durability-what-an-fsync-costs) | Neither is power-loss durable. |
| When a query is kept up to date incrementally, DataVo is 179 times faster than DuckDB and 658 times faster than SQLite on a three-table join. On a single-table aggregate it is 1.1 times faster than SQLite. | [Part II](#streamed-risk-views-incremental-maintenance) | 50,000 insert-then-read iterations. |
| DataVo's SQL text path is about 2.6 times faster than SQLite on `GROUP BY` and on a join at 1,000,000 rows, and slower on `COUNT` with a filter, `ORDER BY ... LIMIT` and range scans. DuckDB is 49 to 118 times faster than DataVo on full scans. | [Part II](#sql-over-1000000-rows) | LSM engine, prepared statements. |
| On uniform random 1536-dimension vectors, exact Flat search is the right choice at 10,000 vectors: HNSW is no faster per query and finds 64% of the true neighbours. On clustered vectors HNSW is 8 times faster at 96% recall. | [Part II](#vector-search) | Both measured. |

## Test environment

| | |
| --- | --- |
| Machine | Apple M4 Pro, 12 cores, 48 GiB, local APFS volume |
| OS | macOS 26.3 (Darwin 25.3.0), arm64 |
| Runtime | .NET SDK 10.0.103, runtime 10.0.3, Arm64 RyuJIT |
| Code | Commit `7c0a931`. The engine is identical to `2d0dcb9`; the only difference is a type alias in the benchmark runners. |
| Comparison engines | `Microsoft.Data.Sqlite` 10.0.0, `DuckDB.NET.Data.Full` 1.5.3, LiteDB (as pinned by the benchmark host), `sqlite-vec` 0.1.9 |
| Conditions | Language-server processes from an IDE were running in the background. No CPU pinning. One run per scenario unless a table says otherwise. |

These are single-machine measurements without confidence intervals, except in Part I where BenchmarkDotNet provides them. A difference of under about 20% between two engines in Part II should not be treated as established. The [methodology page](./methodology.md) lists what each benchmark controls and what it does not.

## Part I: The BenchmarkDotNet specification

`benchmarks/DataVo.Benchmarks` runs the same `players` table and the same questions through DataVo, SQLite and DuckDB. Each benchmark checks that all engines return the same answer before timing begins. The suite takes about 15 minutes.

```bash
dotnet run -c Release --project benchmarks/DataVo.Benchmarks -- --filter '*'
```

**Configuration.** "Memory" means DataVo `InMemory`, SQLite `:memory:` and DuckDB `:memory:`. "File" means DataVo's LSM engine with default settings, SQLite in WAL mode with `synchronous=NORMAL`, and a DuckDB database file. None of the file configurations flushes on every commit. SQLite and DuckDB are called through ADO.NET, and that cost is part of their numbers, as the cost of DataVo's own API is part of DataVo's.

DuckDB appears twice. `DuckDB` uses its default of one thread per core. `DuckDB 1 thread` is a separate database opened with `SET threads = 1`, which is how DataVo and SQLite run: one thread per query. To compare engines on equal terms, use the `DuckDB 1 thread` column.

### Filtered aggregates

200,000 rows, half of them matching `WHERE Level >= @min`. DataVo runs a compiled LINQ query (`Where(...).Count()`, `.Sum()`, `.Min()`, `.Max()`). SQLite and DuckDB run a statement prepared once and read it with `ExecuteScalar`. Mean time per query in microseconds. DataVo allocates 0 bytes per query, SQLite 504 bytes, DuckDB about 1,150 bytes.

![Filtered aggregates over 200,000 rows](/benchmarks/bdn-filtered-aggregates.png)

| Query | Storage | DataVo | SQLite | DuckDB, 1 thread | DuckDB |
| --- | --- | ---: | ---: | ---: | ---: |
| `COUNT(*)` | Memory | 386 | 3,695 | 137 | 203 |
| `SUM(Gold)` | Memory | 526 | 4,817 | 204 | 226 |
| `MIN(Gold)` | Memory | 525 | 4,607 | 201 | 269 |
| `MAX(Ratio)` | Memory | 503 | 4,955 | 418 | 417 |
| `COUNT(*)` | File | 1,973 | 4,543 | 245 | 270 |
| `SUM(Gold)` | File | 2,115 | 5,935 | 374 | 327 |
| `MIN(Gold)` | File | 2,136 | 5,377 | 360 | 346 |
| `MAX(Ratio)` | File | 2,267 | 5,656 | 716 | 564 |

The half-width of the 99.9% confidence interval is at most 3% of the mean for every DataVo and SQLite row, and at most 6% for DuckDB (its default-thread `MAX` in memory). The full tables, with error and standard deviation, are in the `bdn/` folder of the raw results.

**Reading the result.**

- In memory DataVo is 8.8 to 9.8 times faster than SQLite. On file-backed storage it is 2.3 to 2.8 times faster.
- Held to one thread, DuckDB is 2.8, 2.6, 2.6 and 1.2 times faster than DataVo on `COUNT`, `SUM`, `MIN` and `MAX` in memory, and 8.1, 5.7, 5.9 and 3.2 times faster on files.
- DuckDB's extra threads do not help at this size. In memory its default mode is the same as or up to 48% slower than one thread on `COUNT`, `SUM` and `MIN` (I did not investigate why), and on files it ranges from 21% faster to 10% slower.

**Why DuckDB wins these.** DuckDB stores each column contiguously and processes it in vectors of 2,048 values. A query that needs `Level` and `Gold` reads only those two columns, and its inner loop runs over a dense array of one type. DataVo is a row store. Its in-memory engine keeps a packed image of each row, written the first time a compiled scan touches the table, and the scan reads rows in order without chasing objects. That removes the object-graph cost: DataVo's in-memory `COUNT` takes about 1.9 ns per row, where the previous revision of this page reported 1.3 to 4.0 ms for the same four queries, which is 6 to 20 ns per row. But each row's image still holds columns the query does not need, so the scan reads more bytes per row than a column store does. The 1.2 times gap on `MAX(Ratio)` is smaller than the others. The benchmark does not establish why; I have not profiled DuckDB's floating-point maximum.

On file-backed storage DataVo takes 4 to 5 times its in-memory time. The extra cost is iterating rows out of the LSM, not evaluating the query. DuckDB's file-backed times are 1.3 to 1.8 times its in-memory times.

This is a benchmark that plays to column stores. A workload that reads whole rows, or fetches one row at a time, reverses the ordering, as the next two benchmarks show.

### Point lookup by primary key

100,000 rows, keys looked up in shuffled order, each result read into a `Player`. DataVo runs a compiled LINQ query (`FirstOrDefault(id, static (p, k) => p.Id == k)`).

![Point lookup and bulk insert](/benchmarks/bdn-lookup-and-insert.png)

| Storage | DataVo | SQLite | DuckDB, 1 thread | DuckDB | DataVo alloc | SQLite alloc | DuckDB alloc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 566 ns | 723 ns | 74,282 ns | 85,962 ns | 1,008 B | 608 B | 2,144 B |
| File | 242 ns | 1,628 ns | 74,527 ns | 81,433 ns | 144 B | 608 B | 2,144 B |

On a file DataVo is 6.7 times faster than SQLite and about 300 times faster than DuckDB.

**Why DataVo is fast on the file path.** Three things are absent from the path, and each is documented in the source:

- *No SQL parsing or planning.* The source generator emits a static query plan, and a prepared handle is cached per context.
- *No dictionary or boxing.* A typed mapper decodes only the projected columns straight from the stored row bytes, so the 144 bytes allocated are essentially the `Player` itself.
- *No lock.* LSM point reads go through a concurrent map of latest rows whose buffers are immutable once published, so a reader never waits behind a writer (commit `2bae76f`).

SQLite's 1,628 ns is the cost of its B-tree descent plus the ADO.NET call, which allocates 608 bytes each time. DuckDB's roughly 75 µs is a fixed per-statement cost. DuckDB executes every statement through its vectorized pipeline, which is built for returning many rows, and a lookup of one row pays that setup in full. I did not profile DuckDB.NET to divide that cost between the managed wrapper and the engine.

**An anomaly I have not resolved.** DataVo's in-memory lookup (566 ns, 1,008 B) is slower and allocates seven times more than its LSM lookup (242 ns, 144 B). The in-memory engine should not be slower than the disk one. The extra allocation suggests that the in-memory path still materialises the row through a different, allocating route, but I have not confirmed that. Treat the in-memory lookup number as a known weakness, not as a design property.

### Loading 100,000 rows

Into an empty table, with the rows built inside the measurement: DataVo's `InsertTypedBatch`, a prepared SQLite `INSERT` inside one transaction, and DuckDB's appender. Ten timed invocations per engine, so these are noisy; the standard deviation is shown.

| Storage | DataVo | SQLite | DuckDB, 1 thread | DuckDB | DataVo alloc | SQLite alloc | DuckDB alloc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 35.0 ± 6.8 ms | 82.2 ± 37.2 ms | 28.7 ± 5.6 ms | 38.5 ± 4.9 ms | 55 MB | 94 MB | 19 MB |
| File | 133.4 ± 35.4 ms | 76.8 ± 26.5 ms | 31.1 ± 5.9 ms | 29.0 ± 5.8 ms | 62 MB | 94 MB | 19 MB |

Plus-or-minus values are standard deviations of ten invocations. In memory DataVo and DuckDB are within each other's noise, and both have a lower mean than SQLite, whose spread is wide. On a file DuckDB is 2.6 to 4.6 times faster than the two row stores. DataVo's file-backed load took 84 ms in an earlier run of this suite on the same day and 133 ms in this one. With a standard deviation of 35 ms the ordering of DataVo and SQLite on files is not established, and I do not claim one. DuckDB allocates the least, presumably because its appender copies rows into native memory.

## Part II: Real-world systems workloads

These scenarios come from `demos/Research.Benchmark`. They are closer to application workloads than Part I and have less statistical control: one run each, with latency percentiles computed over the run. Each scenario states which engines run which API. DataVo scenarios use typed APIs and prepared compiled queries after the schema is created, except where noted, so the SQL shown documents the logical workload and not necessarily the call that ran.

### Streamed risk views: incremental maintenance

A betting-risk dashboard needs an aggregate that is correct after every incoming order. Each iteration inserts one order and then reads the dashboard result. DataVo subscribes to the exact SQL once and reads the incrementally maintained view. SQLite and DuckDB re-run the full indexed query after every insert, with no trigger and no hand-maintained summary table.

This is a statement about two designs, not only two speeds. DataVo's read returns state that was updated as part of the insert. The polling engines recompute it. It also applies only to the query shapes DataVo can maintain incrementally. Anything else falls back to normal execution.

**Profile A, simple exposure.** `SELECT ... SUM(Stake) ... WHERE IsOpen = true GROUP BY MarketId, RunnerId` over a single table. 10,000 baseline orders, then 50,000 iterations.

| Engine | Total (ms) | P50 | P99 | GC allocated |
| --- | ---: | ---: | ---: | ---: |
| DataVo | 526.9 | 6.4 µs | 21.3 µs | 364.6 MB |
| SQLite | 574.1 | 8.9 µs | 24.6 µs | 200.7 MB |
| DuckDB | 52,954.0 | 983 µs | 3,311 µs | 208.3 MB |

I repeated this scenario three more times for DataVo and SQLite. DataVo took 507 to 527 ms in all four runs and SQLite 574 to 753 ms.

![Streamed risk, simple exposure](/benchmarks/ivm-simple-exposure-total-time.png)

On this profile DataVo is 1.1 to 1.4 times faster than SQLite, which is a small margin. It also allocates 1.8 times as much managed memory, about 7.3 KB per iteration, so it is not an allocation win here. SQLite's indexed recompute stays under 15 µs per iteration at this scale; I did not examine its query plan. DuckDB's 983 µs median looks like a per-query fixed cost, consistent with its roughly 75 µs point lookups in Part I plus scheduling a parallel query, but I did not profile it.

July's version of this page reported SQLite at 2,820 ms, five times slower than today's figure. The SQLite runner has not changed since, and I cannot reproduce the July number. I do not know whether the cause was different hardware or a loaded machine, so I have not used it.

**Profile B, complex VIP exposure.** `SELECT m.Category, SUM(o.Stake) FROM Orders o JOIN Accounts a ... JOIN Markets m ... WHERE a.IsVip = true GROUP BY m.Category`. 10,000 baseline orders, 1,000 accounts, 50 markets, then 50,000 iterations.

| Engine | Total (ms) | P50 | P99 | GC allocated |
| --- | ---: | ---: | ---: | ---: |
| DataVo | 191.7 | 3.1 µs | 4.0 µs | 106.6 MB |
| DuckDB | 34,339.1 | 660 µs | 974 µs | 131.3 MB |
| SQLite | 126,064.4 | 2,527 µs | 4,375 µs | 115.6 MB |

![Streamed risk, complex VIP exposure](/benchmarks/ivm-complex-vip-total-time.png)

DataVo is 179 times faster than DuckDB and 658 times faster than SQLite. The gap is large because of what each engine does per iteration. The polling engines join the whole `Orders` table against two dimension tables and aggregate it, and `Orders` grows from 10,000 to 60,000 rows during the run, so their per-iteration cost grows with the table. DataVo applies only the new order: it looks up the order's account and market, and if the account is VIP it adds the stake to that category's total. That work does not depend on how many orders already exist. SQLite's 2.5 ms median is the cost of a three-table join over tens of thousands of rows. DuckDB's parallel execution handles the same join in 0.66 ms, but it pays its fixed per-query overhead on top.

### Durability: what an fsync costs

`disk-crud-wal` inserts 20,000 records and then runs 20,000 point updates, committing each update separately.

| Engine | Total (ms) | P50 | P99 | GC allocated |
| --- | ---: | ---: | ---: | ---: |
| DataVo (LSM Production) | 84,616 | 4.04 ms | 6.13 ms | 30.8 MB |
| SQLite (WAL, full, `fullfsync`) | 84,857 | 4.04 ms | 6.08 ms | 22.3 MB |
| DataVo (LSM Relaxed) | 113 | 4.5 µs | 7.5 µs | 22.9 MB |
| SQLite (WAL, normal) | 155 | 5.4 µs | 9.2 µs | 23.2 MB |

![Disk CRUD total time](/benchmarks/disk-crud-wal-time.png)

The two durable engines are indistinguishable: about 4.2 ms per update including the insert phase, and a median of 4.04 ms for the update alone. That is the cost of making one commit reach stable storage on this SSD, and neither engine can avoid it. The 750-fold gap between durable and non-durable DataVo is the price of that guarantee, not an inefficiency. The SQLite run sets `PRAGMA fullfsync=1`, so both engines ask the drive to flush its cache.

Without the flush, DataVo (Relaxed) and SQLite (WAL, normal) differ by 1.4 times in DataVo's favour on this serial workload, and the median and P99 latencies are within 1.2 times of each other. Relaxed mode acknowledges a write after it reaches the operating system's buffer. A power loss or kernel crash can lose the most recent writes. Use it for caches, rebuildable data and research, not as a substitute for the durable mode.

**Group commit.** When several threads commit at the same time they can share one flush. `thread-scaling` runs 100,000 reads and 10,000 updates at 1 to 32 threads:

![Thread scaling](/benchmarks/thread-scaling-throughput.png)

| Threads | DataVo Production | DataVo Relaxed | SQLite (WAL, normal) | LiteDB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2,686 | 1,333,227 | 335,281 | 49,027 |
| 2 | 2,962 | 1,276,043 | 559,224 | 68,793 |
| 4 | 5,506 | 1,101,274 | 589,921 | 68,873 |
| 8 | 8,777 | 1,025,118 | 577,691 | 67,864 |
| 16 | 9,550 | 1,028,862 | 569,653 | 65,402 |
| 32 | 9,840 | 1,011,191 | 555,357 | 66,020 |

Operations per second. Durable DataVo scales 3.7 times from one to 32 threads, which is consistent with concurrent writers sharing each flush (I did not count flushes). Relaxed DataVo does not scale: throughput falls 24% from one thread to 32. The whole run takes 83 to 109 ms, so thread start-up is a visible share of it. I have not profiled where the lost throughput goes. The machine has 12 cores, so 16 and 32 threads are oversubscribed. SQLite benefits from concurrent readers in WAL mode and reaches its peak at four threads. SQLite's durable (`full`) mode is not in this table, so the durable DataVo row has no like-for-like comparison here.

**YCSB mixed.** 100,000 preloaded records, then 80,000 reads and 20,000 updates across 12 threads.

![YCSB write P99](/benchmarks/ycsb-write-p99.png)

| Engine | Total (ms) | Ops/s | Read P99 | Write P99 | GC allocated |
| --- | ---: | ---: | ---: | ---: | ---: |
| DataVo (LSM Relaxed) | 195.8 | 510,695 | 0.9 µs | 0.66 ms | 34.7 MB |
| SQLite (WAL, normal) | 382.2 | 261,618 | 8.2 µs | 3.77 ms | 57.2 MB |
| LiteDB | 2,640.1 | 37,877 | 2.59 ms | 2.79 ms | 4,315.1 MB |
| DataVo (LSM Production) | 23,118.6 | 4,326 | 25.7 µs | 27.83 ms | 37.2 MB |

DataVo Relaxed has 1.95 times SQLite's throughput, 5.7 times lower write P99 and 9.3 times lower read P99. The read result follows from the lock-free LSM read path described above: readers do not queue behind writers. The write P99 of the durable row, 27.8 ms, is what 20,000 flushes cost when 12 threads queue for them. Compare Production only with other durable engines.

### Space and cold open

1,000,000 records of `(Id INT PRIMARY KEY, Name VARCHAR(40), Value INT, Score FLOAT)`, inserted in one batch, closed cleanly and reopened.

| Engine | Insert (ms) | Disk (MB) | Reopen (ms) | First read after reopen (ms) | GC allocated |
| --- | ---: | ---: | ---: | ---: | ---: |
| DataVo (LSM Production) | 731.8 | 27.7 | 4.3 | 3.6 | 400.7 MB |
| DataVo (LSM Relaxed) | 473.2 | 27.7 | 1.5 | 0.0 | 332.9 MB |
| SQLite (WAL, normal) | 712.5 | 29.1 | 0.36 | 1.2 | 633.4 MB |
| LiteDB | 2,987.1 | 154.5 | 0.61 | 9.4 | 15,107.2 MB |

DataVo's file is 5% smaller than SQLite's and 5.6 times smaller than LiteDB's. A million rows insert in 473 to 732 ms. The 258 ms difference between Production and Relaxed is consistent with Production flushing the log before acknowledging the batch. I did not count flushes, but a per-row flush would take minutes, so it is at most a few per batch.

**What the reopen figure does and does not mean.** This is a clean-close reopen inside the same process, immediately after the files were written, so the operating system's page cache is warm. It is not a measurement of recovery after a crash, and it is not a cold-cache boot. It shows that opening an LSM database does not read the data: DataVo maps its SSTables and loads their indexes, which takes 1.5 to 4.3 ms for a million rows. SQLite opens a file faster, in 0.36 ms, because it reads only the header and a few pages. The time to the first answer is 7.9 ms for DataVo Production, 1.5 ms for DataVo Relaxed and 1.6 ms for SQLite. Crash recovery, meaning replaying a write-ahead log after a hard stop, is covered by the crash-injection tests in `DataVo.Tests`, not by this benchmark.

### Vector search

10,000 vectors of 1,536 dimensions (uniform random components in [-1, 1], normalised), then 100 top-10 queries. The query vectors are existing vectors from the set, so each query's nearest neighbour is itself. Insert, index build and queries are all inside the total time. The SQLite row uses `sqlite-vec` 0.1.9; it is an exact brute-force scan.

![Vector search total time](/benchmarks/vector-search-total-time.png)

| Engine | Total (ms) | Query P50 | Query P99 | GC allocated |
| --- | ---: | ---: | ---: | ---: |
| DataVo-Flat | 298.7 | 1.96 ms | 2.41 ms | 61.7 MB |
| SQLite (`sqlite-vec`) | 472.8 | 2.37 ms | 2.43 ms | 63.4 MB |
| DataVo (HNSW) | 1,634.0 | 1.77 ms | 2.25 ms | 62.8 MB |
| DataVo (HNSW, diversity heuristic) | 6,682.1 | 1.78 ms | 1.93 ms | 61.7 MB |
| LiteDB | 76,706.5 | 747.4 ms | 778.0 ms | 208,914.6 MB |

**What the GC column does and does not say.** The 61.4 MB that every DataVo and SQLite row shares is the vector data itself: 10,000 vectors of 6,144 bytes. This is one copy of the input made during insert. It is not a measure of search overhead. DataVo-Flat allocated 10.3 MB in the July run and allocates 61.7 MB now. I measured the commit before today's row-image change and got the same 62 MB, so that change is not the cause, and I did not bisect further. The July figure is not reproducible on the current engine, and the previous page's "10.3 MB" claim is withdrawn. LiteDB's 208,915 MB is 3,400 times the size of the input, which is consistent with deserialising every stored document, with its 1,536 floats, on every query. I did not profile it.

**Why Flat is the right choice at 10,000 uniform random vectors.** Three measurements support this, and the first two are in the table above.

1. *Build cost.* Flat builds nothing. Total time is 5.5 times lower than HNSW's.
2. *Query cost.* On this data HNSW queries are not meaningfully faster: 1.77 ms median against 1.96 ms. A Flat query costs about 196 ns per 1,536-dimension distance times 10,000 vectors. The HNSW query takes about as long, which means it evaluates distances for most of the graph. I inferred this from timing and did not count the evaluations.
3. *Quality.* Flat is exact. The table above does not show HNSW's recall, so I measured it separately with held-out queries, where a query's own vector is not in the set (`vector-recall-experiment.txt`, reproducible with `demos/Research.Benchmark/tools/VectorRecall`):

![HNSW versus Flat](/benchmarks/vector-hnsw-vs-flat.png)

| N | Data | Flat query | HNSW query | HNSW recall@10 | HNSW build | Flat build |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | uniform | 0.24 ms | 0.45 ms | 99.9% | 238 ms | 27 ms |
| 10,000 | uniform | 2.06 ms | 1.92 ms | 63.5% | 1,169 ms | 135 ms |
| 50,000 | uniform | 10.1 ms | 3.0 ms | 23.8% | 10,414 ms | 162 ms |
| 1,000 | clustered | 0.24 ms | 0.31 ms | 100% | 63 ms | 10 ms |
| 10,000 | clustered | 2.00 ms | 0.25 ms | 95.9% | 548 ms | 34 ms |
| 50,000 | clustered | 10.3 ms | 0.68 ms | 90.2% | 5,658 ms | 236 ms |

Uniform random vectors in 1,536 dimensions are the worst case for a graph index. Pairwise distances concentrate around one value, so the graph offers no useful direction toward a query's neighbours. HNSW with its default `EfSearch` of 64 then either explores most of the graph or misses neighbours, and at 10,000 vectors it does a bit of both: recall is 63.5%. Real embeddings are clustered, and the clustered rows show what happens then: HNSW answers a 10,000-vector query 8 times faster than Flat at 96% recall, and 15 times faster at 50,000. Its 514 ms of extra build time at 10,000 clustered vectors pays for itself after about 290 queries.

So the claim that holds is narrower than "Flat beats HNSW up to 10,000 vectors": Flat wins at that size for the benchmark's random data, and for any workload that builds the index and runs few queries. For clustered embeddings that serve many queries, HNSW wins at 10,000 vectors as well. Flat's query time grows linearly, 0.24 ms, 2.0 ms and 10 ms at 1,000, 10,000 and 50,000 vectors, so for latency-sensitive workloads the crossover comes sooner than 10,000. Measure with your own embeddings. The recall experiment above is a template.

### SQL over 1,000,000 rows

`sql-workload` runs the operations of a small application through SQL text with prepared statements, against a million-row orders table in file-backed engines (DataVo LSM without a flush per commit, SQLite WAL `NORMAL`, DuckDB default, LiteDB default). Single-step rows show total time; repeated rows show the median and P99 of one call.

| Operation | DataVo | SQLite | DuckDB | LiteDB |
| --- | ---: | ---: | ---: | ---: |
| Load orders and customers | 1.87 s | 3.24 s | 538.5 ms | 8.94 s |
| Point read by INT key | 1.4 µs | 2.0 µs | 150.0 µs | 23.2 µs |
| Point read by GUID key | 1.2 µs | 2.2 µs | 126.4 µs | 25.2 µs |
| Secondary-index lookup (about 100 rows) | 110.8 µs | 86.6 µs | 265.0 µs | 413.2 µs |
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

DataVo is faster than SQLite on point reads, `GROUP BY` (2.6 times) and the join (2.6 times). It is slower on `COUNT` with a filter (1.8 times), `ORDER BY ... LIMIT` (1.6 times), key range scans (6 times), autocommit writes, close and reopen. DuckDB is 49 to 118 times faster than DataVo and 31 to 249 times faster than SQLite on the four full-scan queries (`COUNT`, `GROUP BY`, join, `ORDER BY`). A million-row `COUNT` in 0.4 ms is 0.4 ns per row, which means DuckDB is not examining each row at scalar speed. I have not inspected its plans, so I cannot say how much comes from column vectors and how much from per-row-group statistics. DataVo's `COUNT` through SQL is also 4.7 times slower per row than its compiled, file-backed scan in Part I: 46.8 ns per row against 9.9 ns.

The same scenario on 6 November 2026 took 1.0 s for the `COUNT`, 2.2 s for the `GROUP BY` and 3.7 s for the join, so the SQL path has improved by 21 to 28 times in a week. [`2026-11-06-competitive-results.md`](https://github.com/ArintonAkos/DataVo-DBMS/blob/master/docs/public/benchmarks/2026-11-06-competitive-results.md) keeps that earlier table.

### Other workloads

**Flat CRUD** inserts 50,000 records and then runs 50,000 point lookups, in memory.

| Engine | Total (ms) | P50 | P99 | GC allocated |
| --- | ---: | ---: | ---: | ---: |
| DataVo | 63.9 | 0.54 µs | 0.71 µs | 23.0 MB |
| SQLite | 128.8 | 1.17 µs | 1.42 µs | 62.1 MB |
| LiteDB | 1,322.8 | 17.5 µs | 20.7 µs | 2,500.0 MB |

**Deep document** saves 5,000 nested orders (5 items and 2 addresses each) and loads them back.

| Engine | Total (ms) | P50 | P99 | GC allocated |
| --- | ---: | ---: | ---: | ---: |
| DataVo | 83.6 | 5.0 µs | 13.8 µs | 25.0 MB |
| SQLite | 140.8 | 7.8 µs | 12.4 µs | 46.1 MB |
| LiteDB | 431.2 | 48.3 µs | 105.5 µs | 332.3 MB |

**Concurrent ops** preloads 100,000 records and runs 8 readers and 2 writers for 5 seconds.

| Engine | Ops/s | Read P99 | Write P99 | GC allocated |
| --- | ---: | ---: | ---: | ---: |
| DataVo | 1,852,526 | 48 µs | 0.11 ms | 4,494.6 MB |
| SQLite | 372,220 | 120 µs | 5,134.7 ms | 1,052.5 MB |

DataVo's 4.5 GB over 5 seconds is about 500 bytes per operation, including the preload. It allocates 4.3 times as much as SQLite here while running 5 times as many operations. SQLite's 5.1-second write P99 is a writer that waited out its busy timeout while readers held the database; it is a property of this contention setup, not a typical SQLite write latency. July reported a 4.5 ms write P99 for DataVo in this scenario; I cannot reproduce it, and today's is 0.11 ms.

## Part III: Platforms

### What runs where

All numbers on this page are CoreCLR .NET 10 on macOS arm64. Nothing here was measured on Linux x64, Windows, or under Unity's IL2CPP, and the platform notes below describe code paths and test results, not performance.

**SIMD.** Vector distance kernels (`DataVo.Core/Utils/SimdDistanceKernels.cs`) select a path at runtime: 256-bit AVX where the CPU supports it, otherwise 128-bit Arm `AdvSimd` (NEON), otherwise `TensorPrimitives`. There is no AVX-512 kernel, so an AVX-512 machine runs the 256-bit path. There is no GPU or Metal code path. On Apple Silicon the vector results above come from the NEON path, which processes four floats per instruction against eight for AVX. The portable `netstandard2.1` build, which is the Unity target, compiles a `System.Numerics.Vector<T>` fallback instead of the intrinsics.

**Linux x64.** The only Linux measurements are from the GitHub Actions workflow on 4 July 2026, an Ubuntu 24.04 runner on the v0.1 alpha engine. They are not re-run for this page. A Linux x64 run needs the `benchmark.yml` workflow, which can be started from the Actions tab (`workflow_dispatch`). The `docker-compose.yml` under `demos/Research.Benchmark` provides Postgres and Redis only. On Apple Silicon Docker would run arm64 Linux, so it would not exercise AVX. The Linux plots in the README (`linux-*.png`) show those July figures and are labelled accordingly.

**Windows.** The same workflow runs on a Windows runner. No Windows numbers were produced for this revision. Strict-fsync scenarios are much slower on Windows hosted runners, as the reproduction notes warn.

### Unity IL2CPP

`scripts/check-unity-support-status.sh` passed: the support-status claims in the README and the manual match the table below. `scripts/run-unity-macos-il2cpp-player-smoke.sh` then built and ran a real IL2CPP player.

| Item | Result |
| --- | --- |
| Editor | Unity 6000.5.4f1 |
| Player | macOS standalone, IL2CPP scripting backend, universal binary (x86_64 and arm64) |
| Smoke report | `status: passed`, 3 scenarios (`unity-il2cpp-smoke-result.json`) |
| Managed stripping | UnityLinker ran with `--rule-set=Aggressive` |
| Build warnings | Two, both from Xcode's `strip` about invalidating a code signature. No linker or AOT warnings. |
| Persistence | `Lsm` and the legacy `Disk` stores were written, closed, reopened, updated and deleted from inside the player, with index queries returning correct rows after reopen |

What the player exercises: an in-memory table storing Unity `Vector2`, `Vector3`, `Vector4`, `Quaternion` and `Unity.Mathematics` values, read back through `SELECT` and the typed `Get<T>` accessor; a failing `VECTOR(2)` to `Vector3` conversion; the package scenarios; and persistence in `Lsm` and the legacy `Disk` mode. `Disk` mode is deprecated and is not benchmarked on this page.

What it does not show. This is a smoke test, not a benchmark, and no IL2CPP timing was collected. IL2CPP translates C# to C++ ahead of time, so the performance of DataVo's hot loops under it, including the SIMD fallback, is unmeasured. Direct Burst calls are unsupported by design. Windows x64 IL2CPP is unverified. The README states that Unity is an evaluation target, not a supported runtime, and that remains accurate. `scripts/run-unity-macos-il2cpp-arena-cross-engine.sh` produces IL2CPP performance numbers for a reactive-view workload. It launches many player processes, and I did not run it for this revision.

## What changed since July

| Item | July page | Now |
| --- | --- | --- |
| Strict-durability disk updates | 502 ms for 20,000 updates, "417 times faster than SQLite `full+fullfsync`" | 84,616 ms. SQLite `full` takes 84,857 ms. July's median update latency of 42 µs is far below the roughly 4 ms a device flush takes, so those writes were not waiting for stable storage and the 417 times comparison was invalid. Lsm writes now become visible only after they are durable (commit `befede4`, 5 November). |
| DataVo-Flat allocation | 10.3 MB | 61.7 MB. This is one copy of the input. The July value is not reproducible. |
| Simple-exposure SQLite | 2,820 ms | 574 ms (574 to 753 ms over four runs). Not reproducible. Hardware and load differ. |
| Relaxed thread scaling, 1 thread | 1,215,413 ops/s | 1,333,227 ops/s. |
| Space on disk, 1,000,000 records | 66.4 MB, 13.8% larger than SQLite | 27.7 MB, 5% smaller than SQLite. Clustered primary keys landed after July. |
| Flat CRUD | 404 ms (DataVo), 689 ms (SQLite) | 64 ms, 129 ms. Every engine was about 6 times slower in July, which points to a different machine or a loaded one (the July page did not record the chip). Compare engines within a run, not across the two. |

## Limits of this evidence

- One machine, one operating system, one run per Part II scenario. Part I has confidence intervals; Part II does not.
- The Part II workloads are small enough that some finish in around 100 ms. At that size, warm-up, the garbage collector and thread start-up are visible.
- The comparison engines are driven through ADO.NET (SQLite, DuckDB) and LiteDB's own API from .NET. A native C client would remove some of their overhead.
- DuckDB is built for analytical scans, not point operations. Using it for lookups or single-row writes, as several benchmarks here do, measures a design mismatch.
- Streamed-view results compare an incrementally maintained query with a recomputed one, and apply only to query shapes DataVo can maintain.
- DataVo is a 0.2.0 release candidate. SQLite and DuckDB are mature engines with broad SQL coverage, sophisticated optimizers, and years of production hardening.

To reproduce anything here, see [Reproducing Benchmarks](./reproducing-benchmarks.md).
