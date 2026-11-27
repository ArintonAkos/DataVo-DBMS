# I wrote an embedded database and Postgres sync engine in C#. Here is where it wins and where it loses.

> Source route: /blog/part-1-where-it-wins-and-where-it-loses
> Source file: blog/part-1-where-it-wins-and-where-it-loses.md

*Part 1 of 7.*

When you build a local-first .NET application today—a desktop tool that has to stay responsive offline, a local AI assistant, or a real-time simulation—you rarely pick one database. You assemble three. You ship SQLite for relational state, bolt on a separate store for vector embeddings, and write a custom JSON-over-WebSocket layer to keep that local cache reconciled with PostgreSQL. Each boundary costs you something: native binaries per target OS, P/Invoke marshalling on every call, duplicated schemas, and hand-rolled conflict handling.

I wanted to know what happens if you collapse all of that into a single managed .NET 10 process.

Over the last several months, I have been building two pieces to test that idea. **[DataVo-DBMS](https://github.com/ArintonAkos/DataVo-DBMS)** is an MIT-licensed embedded relational, columnar, and vector database written from scratch in pure C# (compiling cleanly under Native AOT). **[DataVo-Sync](https://github.com/ArintonAkos/DataVo-Sync)** is a separate, proprietary commercial add-on that keeps a local DataVo store synchronized with PostgreSQL in both directions, including writes made while offline.

This first post is a map of the whole system: why it is structured the way it is, what kinds of applications it actually fits, and what the numbers look like when measured against SQLite, DuckDB, and ElectricSQL across macOS, Linux, and Windows.

At a glance, the approach pays off in four specific places:

- **In-process reads without an interop boundary:** Using build-time Roslyn code generation, primary-key lookups take **43.7 ns** in memory and **228.6 ns** on LSM disk files (**7.0–16× faster** than SQLite on an Apple M4 Pro), allocating **56–144 B** per call.
- **Continuous views instead of repeated re-queries:** Maintaining a live 3-table `JOIN + GROUP BY` view across 50,000 incoming writes using Z-set incremental view maintenance takes **238 ms**, compared to **39.4 s** in DuckDB (**166×**) and **128.8 s** in SQLite (**541×**).
- **Filtered vector search inside ordinary SQL:** Top-10 search over 50,000 × 384-dimensional embeddings with SQL `JOIN` and `WHERE` filters runs in **0.27–3.83 ms** at **≥ 99.95% mean recall@10**, in the same transaction log as the relational tables.
- **Compact offline-first Postgres sync:** End-to-end PostgreSQL 17 replication transfers **45.5 bytes per row** over the wire (**8.0× smaller** than ElectricSQL `1.8.1`) with **5.1–6.9× lower** client GC allocations, converging automatically after a burst of 500 offline writes.

It also loses in predictable places: unindexed substring scans (**1.29× slower** than SQLite on `LIKE '%042-%'`), whole-table analytical scans without `[Columnar]` (**6–360× slower** than DuckDB), Linux single-row durable `StrictFsync` latency (**0.163 ms** vs **0.079 ms**, **2.1× slower** than SQLite WAL `FULL`), and sync hub cold-start time (**2.4 s** to load 100k rows). Those losses are reported alongside the wins below, because an embedded database is only useful once you know where its boundaries are.

::: info Refreshed for v0.5.1
The at-a-glance figures above come from the v0.5.1 benchmark pass (27 November 2026: Apple M4 Pro locally, Linux x64 on GitHub Actions). The sections below keep the dated runs they name. Current tables live in [Benchmark Results](/manual/performance/benchmarks) and [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite).
:::

::: tip Try or bookmark DataVo
Explore or star the MIT-licensed engine at [`ArintonAkos/DataVo-DBMS`](https://github.com/ArintonAkos/DataVo-DBMS), or jump to the self-hosted Postgres sync quickstart at [`ArintonAkos/DataVo-Sync`](https://github.com/ArintonAkos/DataVo-Sync).
:::

## Why put all of this in one process?

The default embedded relational database is SQLite, and in most projects it remains the right default. It is small, extraordinarily well tested, and backed by two decades of production failure experience. Nothing in this series argues against that.

DataVo exists for four narrower situations where the usual stack starts to show friction:

- **Local-first desktop and mobile apps (MAUI, Avalonia, WPF) backed by PostgreSQL.** As argued in Kleppmann et al.'s *Local-First Software* <a class="cite-badge" href="#ref-1">1</a>, an application feels fundamentally different when the primary copy of the data lives on the user's device and the network runs in the background (as shown in Figure 1). When the server-side store is already PostgreSQL, keeping a local relational replica that accepts offline writes and syncs compressed deltas removes an entire tier of loading spinners and ad-hoc caching code.
- **Offline AI and local RAG in C#.** Real applications rarely ask for "the ten nearest vectors in the universe." They ask for the ten nearest document chunks belonging to a specific tenant, folder, or permission group. Keeping `VECTOR(n)` columns in the same tables and transaction log as relational metadata lets a single `SELECT ... JOIN ... WHERE ... ORDER BY Embedding <=> @q LIMIT 10` answer that question without running Qdrant or `pgvector` locally.
- **Live dashboards, leaderboards, and game HUDs (Unity and real-time simulations).** Applications with continuously mutating state often run the same aggregate or multi-table join on every frame or tick. Updating a materialized view from small `+1` / `-1` write deltas avoids recomputing the entire query when only one row changed.
- **Single-binary Native AOT .NET deployments.** SQLite is a C library: shipping it means per-platform native binaries and a boundary the .NET JIT cannot inline across. `DataVo.Core` and `DataVo.Data` compile with trimming and Native AOT analyzers treated as errors, so an application can publish with `PublishAot=true` and zero `.dll`/`.so`/`.dylib` dependencies. That is a packaging property rather than a speed claim, and it only matters if the engine underneath is correct and fast enough.

<div class="sketch-diagram light-only">

![Figure 1: Traditional .NET local-first assembly vs. single-process consolidation in DataVo](/blog/part-1/fig-01-single-process-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 1: Traditional .NET local-first assembly vs. single-process consolidation in DataVo](/blog/part-1/fig-01-single-process-dark.svg)

</div>

*Figure 1: Traditional .NET local-first assembly vs. single-process consolidation in DataVo.*

It is equally important to be clear about what DataVo is **not**. It is not a client/server database: a lock file (`datavo.lock`) gives one process exclusive ownership of a database directory. Transactions provide snapshot isolation rather than serializability <a class="cite-badge" href="#ref-2">2</a>. Its SQL dialect intentionally leaves out features teams expect from full-scale SQL engines: no `CASE`/`CAST`/`COALESCE` or date functions, no window functions beyond `RANK()`, and no `CREATE VIEW`, triggers, or stored procedures. At `0.5.1` (the current release on NuGet) it is pre-1.0 software whose on-disk format does not yet carry a cross-version compatibility guarantee: standalone local databases require an export and re-import across breaking format changes, whereas `DataVo-Sync` setups treat PostgreSQL as the source of truth and can simply wipe and re-hydrate the local replica (**~0.7 s** for 100k rows).

## How the engine is put together

### Two storage layouts, plus opt-in columnar segments

DataVo organizes data in two ways to strike a balance between fast point lookups and analytical scans (as shown in Figure 2):

- **InMemory (`FlatRowStore`)** packs rows as fixed-width cells inside contiguous byte buffers instead of allocating one heap object per row (as illustrated in Figure 2a). On the benchmark schema, this reduces memory footprint from **640.3 bytes/row** (the object-per-row layout it replaced) down to **49.2 bytes/row** with shared strings. Integer primary keys map directly to row slots via a hash map. Nothing survives process exit.
- **LSM** follows the log-structured merge tree described by O'Neil et al. <a class="cite-badge" href="#ref-3">3</a> (and surveyed in *Designing Data-Intensive Applications* <a class="cite-badge" href="#ref-2">2</a>). Writes append to a commit log (`LsmStrictFsync = true` waits for stable storage before acknowledging), enter an arena-backed MemTable, flush at 32 MB into immutable memory-mapped SSTables on a background thread, and compact asynchronously. When a table has a single `INT` or `GUID` primary key, rows are stored **clustered** under that key, so a key lookup or range scan seeks straight to the row instead of probing a secondary index and performing a separate row fetch.
- **Columnar SSTables (`[Columnar]` / `CREATE INDEX ... USING COLUMNAR`)** store selected columns a second time as dense, contiguous typed arrays (Figure 2b). On LSM, flush and compaction write those columns into each SSTable in 4,096-row segments paired with min/max zone maps and `NULL`/`NaN` bitmasks. Analytical queries like `COUNT(*) WHERE Amount >= @lo` skip excluded chunks via zone maps and scan surviving chunks directly from memory-mapped files, requiring no index rebuild when the database reopens. This borrows the core insight from Abadi et al.'s column-store work <a class="cite-badge" href="#ref-4">4</a>, kept opt-in per column because rows remain DataVo's primary layout.

<div class="sketch-diagram light-only">

![Figure 2: Memory footprint and on-disk columnar segment layouts](/blog/part-1/fig-02-storage-layout-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 2: Memory footprint and on-disk columnar segment layouts](/blog/part-1/fig-02-storage-layout-dark.svg)

</div>

*Figure 2: Memory footprint (a) and on-disk columnar segment layouts (b).*

### Typed execution versus the general fallback

Every incoming query is first offered to a set of **typed execution paths** that read cells directly from packed buffers (as mapped in Figure 3): single-table scans (`AND`, `OR`, `LIKE`, grouping, bounded top-k, key ranges, index lookups), integer-key hash joins (`INNER` and `LEFT`), columnar aggregates, and the vector planner. Each typed path guarantees that it either produces the exact same answer as the general evaluator or declines cleanly.

Queries that no typed path accepts fall back to the general evaluator, which materializes a dictionary per row and allocates kilobytes per row read. Which side of that boundary a query lands on explains far more of the benchmark numbers below than any micro-optimization. When this post was written, `EXPLAIN` reported only the general planner's `Volcano` vs `Legacy` mode rather than whether a typed bypass accepted the query. *Update: since `0.5.0`, [`EXPLAIN`](/manual/sql/statements/explain) reports the physical operator that actually produced the rows and whether it is a typed fast path.*

<div class="sketch-diagram light-only">

![Figure 3: Typed execution versus the general evaluator fallback](/blog/part-1/fig-03-typed-vs-general-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 3: Typed execution versus the general evaluator fallback](/blog/part-1/fig-03-typed-vs-general-dark.svg)

</div>

*Figure 3: Typed execution versus the general evaluator fallback.*

### Vector search, incremental views, and Postgres sync

- **Vector search (`VECTOR(n)`):** `FLAT` computes exact distances using SIMD intrinsics (`AVX2` / Arm `AdvSimd`). `HNSW` implements Malkov and Yashunin's hierarchical navigable small-world graph <a class="cite-badge" href="#ref-5">5</a>. Because filtering inside a graph traversal is tricky, a filtered query does not push predicates into the graph edges; instead it requests roughly `LIMIT / selectivity` candidates, evaluates the relational filter on stored rows, and expands the search if too few survive. When walking the graph would cost more than scanning, or when expansion still falls short, it switches to an exact filter-first scan and ranks only the passing rows, preventing short results.
- **Z-set incremental views:** Following Budiu et al.'s DBSP model <a class="cite-badge" href="#ref-6">6</a>, a table is represented as a *Z-set* where every row carries an integer weight (`+1` for an insert, `-1` for a delete). Writes emit small Z-set deltas that update materialized views without re-running the underlying query. This applies only to query shapes the reactive planner supports; unsupported shapes execute normally.
- **DataVo-Sync:** Because DataVo-Sync is a commercial product, I keep its internal architecture private and describe only the external behaviour tested here. It connects a sync server reading PostgreSQL logical replication to an embedded client library writing into a local DataVo store (`InMemory` or `Lsm`), streaming compressed columnar deltas over WebSockets (`wire v2` + Brotli) and queuing offline local writes for upload and conflict resolution.

## How the measurements were collected

Before looking at any chart, four ground rules govern how to read the numbers below:

1. **Matching durability.** A database that acknowledges writes from RAM will always beat one waiting on the drive. Every disk comparison pairs equivalent durability guarantees:

   | DataVo | SQLite | What a commit guarantees |
   | --- | --- | --- |
   | `LsmStrictFsync = true` ("Production") | WAL, `synchronous=FULL` | Waits for stable storage on every commit |
   | `LsmStrictFsync = false` ("Relaxed") | WAL, `synchronous=NORMAL` | Survives process crash; may lose recent commits on power loss |

   Where a benchmark harness lacked a matched `SQLite WAL FULL` row, I state that explicitly.

2. **No interpolation.** Every figure comes directly from a logged run or CI artifact listed under [Data sources](#data-sources).
3. **Single-run noise.** Only the BenchmarkDotNet suites carry confidence intervals. On single-run suites, I treat differences under ~20% as unestablished.
4. **Shared CI runners.** GitHub Actions runners are shared virtual machines; compare engines *within* a run, not across runs or platforms.

<details>
<summary>Test environments (macOS M4 Pro, Linux x64 CI, Windows x64 CI, Sync CI)</summary>

| Environment | Hardware | Runtime | Used for |
| --- | --- | --- | --- |
| Apple M4 Pro | 12 cores, 48 GiB, APFS SSD | .NET 10.0.3, macOS 26.3 | BDN lookups/aggregates, concurrent-ops & space-recovery (`02fa240f`, 20–21 Nov), SQL/vector matrix (17 Nov), sync/update workloads (12 Nov) |
| Linux x64 CI | AMD EPYC 9V45, 4 logical / 2 physical cores | .NET 10.0.15, Ubuntu 26.04 | BenchmarkDotNet and research matrix, run `44748508039` (20 Nov); `concurrent-ops` WAL re-run `44916996459` (21 Nov) |
| Windows x64 CI | Xeon Platinum 8370C, 4 logical / 2 physical cores (Hyper-V) | .NET 10.0.15, Windows 11 24H2 | runs `44748508039` (20 Nov) and `concurrent-ops` WAL re-run `44916996459` (21 Nov) |
| Sync CI | Ubuntu 26.04 x64, **2 vCPU**, PostgreSQL 17.12, ElectricSQL `1.8.1` in Docker | .NET 10.0.15 | DataVo-Sync E2E and 1,000-client stress, run `44756364158` (20 Nov) |

</details>

## Point lookups and columnar scans

In a game loop, an in-process cache, or a local telemetry aggregator, the cost of crossing into a native database driver—parsing SQL, binding parameters, marshalling strings—often dwarfs the lookup itself. `DataVo.Generators` is a Roslyn incremental source generator that compiles LINQ predicates at build time directly into packed-byte readers, staying completely inside managed code and Native AOT safe:

```csharp
[DataVoTable("players")]
public sealed class Player
{
    [PrimaryKey] public int Id { get; set; }
    [Columnar]   public int Level { get; set; }
    [Columnar]   public long Gold { get; set; }
}

// Compiled at build time to zero-allocation packed reads / columnar scans:
int highLevel = db.Table<Player>().Where(50, static (p, min) => p.Level >= min).Count();
```

As charted in Figure 4, removing the native boundary pays off on frequent, tiny queries:

![Figure 4: Primary-key point lookup and filtered COUNT(*) latencies on the Apple M4 Pro and CI platforms](/blog/part-1/01-cross-platform-lookup-and-count.png)

*Figure 4: Primary-key point lookup and filtered COUNT(\*) latencies on the Apple M4 Pro and CI platforms.*

| Primary-key lookup (100k rows) | DataVo, memory | DataVo, file (LSM) | SQLite, memory | SQLite, file | DuckDB, 1 thread, file |
| --- | ---: | ---: | ---: | ---: | ---: |
| **Apple M4 Pro** (`02fa240f`, 20 Nov) | **55.0 ns** | **216.2 ns** | 715.7 ns | 1,654.0 ns | 62.7 µs |
| **Linux x64 CI** (20 Nov) | **130.8 ns** | **288.3 ns** | 1,398.2 ns | 3,642.7 ns | 204.1 µs |
| **Windows x64 CI** (20 Nov) | **239.6 ns** | **666.9 ns** | 2,024.0 ns | 6,741.1 ns | 380.7 µs |

| Filtered `COUNT(*)`, `[Columnar]` (200k rows) | DataVo, memory | DataVo, file (LSM) | SQLite, memory | SQLite, file | DuckDB, 1 thread, file |
| --- | ---: | ---: | ---: | ---: | ---: |
| **Apple M4 Pro** (`02fa240f`, 20 Nov) | **27.11 µs** | **27.44 µs** | 3,655.8 µs | 4,449.5 µs | 229.8 µs |
| **Linux x64 CI** (20 Nov) | **9.29 µs** | **8.81 µs** | 4,553.2 µs | 5,603.4 µs | 538.4 µs |
| **Windows x64 CI** (20 Nov) | **16.20 µs** | **15.94 µs** | 8,607.6 µs | 13,446.1 µs | 1,102.2 µs |

Per lookup, DataVo allocates **56 B** in memory (just the returned entity) and **144 B** on LSM, versus **608 B** for SQLite through `Microsoft.Data.Sqlite`. The columnar `COUNT(*)` allocates **0 B** on the managed heap (versus **504 B** for SQLite).

Two caveats keep this result honest. First, this table compares DataVo's build-time compiled LINQ path against SQLite and DuckDB over ADO.NET; when all engines execute SQL text prepared statements, integer-key lookups narrow considerably (**1.4 µs** vs **2.0 µs** on the M4 Pro). Second, if a column is *not* marked `[Columnar]`, DataVo falls back to scanning packed rows, where single-threaded DuckDB is substantially faster (**137 µs** vs **386 µs** on the M4 Pro).

## Maintaining views incrementally

Consider a reactive dashboard, trading blotter, or live game leaderboard that joins three tables (`Orders × Accounts × Markets`) with a `GROUP BY`, while new rows arrive continuously (visualized in Figure 5). In a traditional embedded database, every refresh re-executes the entire join unless you maintain custom summary tables by hand (for example, with SQLite `AFTER INSERT/UPDATE/DELETE` triggers). With declarative Z-set incremental view maintenance (IVM), each write pushes a tiny `+1` / `-1` delta through the view operator graph and updates only the affected groups without trigger boilerplate:

<div class="sketch-diagram light-only">

![Figure 5: Full view recomputation vs. incremental Z-set delta propagation](/blog/part-1/fig-04-zset-ivm-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 5: Full view recomputation vs. incremental Z-set delta propagation](/blog/part-1/fig-04-zset-ivm-dark.svg)

</div>

*Figure 5: Full view recomputation vs. incremental Z-set delta propagation.*

Over 10,000 orders, 1,000 accounts, and 50 markets running **50,000 insert-and-read iterations** (`complex-vip`), avoid-the-join gains grow with write density, as mapped in Figure 6:

![Figure 6: Cumulative time to apply 50,000 updates to a 3-table JOIN + GROUP BY view](/blog/part-1/06-incremental-views-complex-vip.png)

*Figure 6: Cumulative time to apply 50,000 updates to a 3-table JOIN + GROUP BY view on macOS, Linux, and Windows.*

In C#, the subscription uses `SubscribeZeroAlloc` (or the owned `Subscribe` overload) and drains deltas on demand via `DispatchPendingNotifications()` (see [Live Reactive Views (Z-set IVM)](/manual/fundamentals/reactive-views)):

```csharp
using var view = db.SubscribeZeroAlloc(
    "SELECT m.Category, SUM(o.Stake) AS TotalExposure FROM Orders o " +
    "JOIN Accounts a ON o.AccountId = a.Id JOIN Markets m ON o.MarketId = m.Id " +
    "WHERE a.IsVip = true GROUP BY m.Category",
    (in QueryChangeRef delta) => ApplyDeltas(delta.Added, delta.Updated, delta.Removed));
db.DispatchPendingNotifications(); // pull-drain deltas on the caller thread
```

- **Apple M4 Pro (12 Nov):** DataVo **191.7 ms** (p50 `3.1 µs`) vs DuckDB **34.3 s** (**179×**) vs SQLite **126.1 s** (**658×**).
- **Linux x64 CI (20 Nov):** DataVo **599.9 ms** (p50 `10.9 µs`) vs DuckDB **112.9 s** (**188×**) vs SQLite **237.1 s** (**395×**).
- **Windows x64 CI (20 Nov):** DataVo **545.5 ms** (p50 `9.6 µs`) vs DuckDB **135.5 s** (**248×**) vs SQLite **247.8 s** (**454×**).

This speedup is specifically a property of incremental maintenance on multi-table views supported by the reactive planner (documented in full in [Live Reactive Views (Z-set IVM)](/manual/fundamentals/reactive-views)). On a simple single-table aggregate without joins, where recomputation is already cheap, the gap over SQLite shrinks to roughly **1.1×**.

## Vector search with SQL filters

In local RAG tools and desktop AI agents, vector similarity almost always comes wrapped in relational filters and joins, filtering by user, workspace, category, or author metadata before returning the top results:

```sql
SELECT d.Id, d.Title, d.Embedding <=> @query AS distance
FROM Docs d
JOIN Authors a ON d.AuthorId = a.Id
WHERE a.Country = @country AND d.Category = @category
ORDER BY distance ASC
LIMIT 10;
```

On **50,000 × 384-dimensional clustered embeddings** (Apple M4 Pro, 17 Nov), comparing DataVo's `FLAT` and `HNSW` indexes against SQLite with `sqlite-vec` (`vec0` exact scan):

| Query shape | DataVo Flat (exact) | DataVo HNSW | `sqlite-vec` (exact) |
| --- | ---: | ---: | ---: |
| `V0` no filter | 2.19 ms | **0.26 ms** | 18.2 ms |
| `V1` filter, ~5% of rows | 2.64 ms | **2.48 ms** | 9.78 ms |
| `V2` filter, ~50% of rows | 2.04 ms | **0.67 ms** | 14.4 ms |
| `V3` `V1` + `JOIN` | 2.56 ms | **2.42 ms** | 9.76 ms |
| `V4` `JOIN`, ~10% survive | 4.34 ms | **3.62 ms** | 13.1 ms |

Every query shape above achieved **100% recall@10**, zero short result sets, and zero rows violating the filter. At 1,536 dimensions, distance computation dominates more heavily and margins narrow; the closest case is `V4`, where `FLAT` is **1.4× faster** than `sqlite-vec`.

Note also that HNSW is not free to construct: at 10,000 × 1,536-dimensional vectors, building the graph takes **6–10 seconds**. If a session runs 100 queries or fewer, an exact `FLAT` scan (**571–793 ms** total) finishes sooner overall; HNSW pays back its build cost only after roughly **290+ queries** (running at **2.38 ms** p50 per query).

## Twenty-eight SQL queries, and five wrong answers

To test general SQL behaviour rather than cherry-picked microbenchmarks, the research matrix runs 28 prepared statements over identical generated data (**1,000,000 orders**, **100,000 customers**, **1,000 products**). Before any timing starts, the runner verifies every engine's output against an exact oracle computed directly from the data generator, favoring DataVo on joins and aggregates while highlighting SQLite’s strength in scan performance (Figure 7).

![Figure 7: DataVo execution time relative to SQLite across 28 standard SQL and 5 vector shapes on the Apple M4 Pro](/blog/part-1/05-sql-matrix-vs-sqlite.png)

*Figure 7: DataVo execution time relative to SQLite across 28 standard SQL and 5 vector shapes on the Apple M4 Pro.*

Using a ±20% equivalence band against SQLite WAL `NORMAL` on the M4 Pro (17 Nov follow-up run), **18 SQL shapes favour DataVo, 6 are within 20%, and 4 favour SQLite**:

- **Where DataVo pulls ahead:** 3-table inner join `J1` (**10.28×**, `70.2 ms` vs `721.4 ms`), `Orders × Customers GROUP BY` `J6` (**5.87×**), low-cardinality `GROUP BY` `G1`/`G2` (**3.3–4.7×**), columnar aggregates `C1`/`C2`/`C4`/`C6` (**4.8–13.7×**), secondary index lookup `W1` (**2.42×**), and clustered primary-key ranges `R1–R4` (**1.35–1.76×**).
- **Where SQLite wins:** unindexed substring search `LIKE '%042-%'` (`W6`, SQLite **1.35×** faster), unindexed `ORDER BY ... LIMIT 10` (`C8`, **1.3×**), and high-cardinality 125k-group `GROUP BY` over a nullable indexed key (`G3`, **1.4×**).

### Five wrong answers caught before timing

The first run of this oracle on 14 November, against the unmodified engine, caught **five queries where DataVo returned the wrong answer** on both storage engines:

| # | Query shape | DataVo returned | Correct |
| --- | --- | --- | --- |
| `B1` | `LEFT JOIN ... WHERE c.Id IS NULL` | `0` | `250,395` |
| `B2` | `COUNT(DISTINCT CustomerId)` | `2,000` | `175` |
| `B3` | Filtered vector search over aliased tables | `0–2` rows (`recall@10` 5%) | `10` rows |
| `B4` | `AND` across two joined tables | `0` rows | `1,827` rows |
| `B5` | Vector search joined to a filtered table | `10` rows (9 failing filter on avg) | `10` rows from that country |

Each had a distinct bug underneath: a `WHERE` filter on the `NULL`-supplying side of an outer join evaluated before `NULL` padding existed; a parser dropping `DISTINCT` inside an aggregate; a planner comparing a table alias string against a raw table name; row keys emitted in a different order depending on join drive side; and a vector join path applying only the portion of `WHERE` that referenced the vector table.

All five were fixed and covered by regression tests in `0.4.0-rc.1` (and carry forward into the current `0.5.1` NuGet release). I include them here because speed numbers mean little without them: a fast wrong answer is not fast. Rigger and Su's *Pivoted Query Synthesis* <a class="cite-badge" href="#ref-7">7</a> and Slutz's *Massive Stochastic Testing of SQL* <a class="cite-badge" href="#ref-8">8</a> are the intellectual ancestors of this check, though here the oracle is an exact computation from the generator rather than a second database.

## Durability, concurrency, and recovery

Storage benchmarks expose operating-system behaviour as much as engine design, particularly around `fsync` and lock contention.

### Durable single-row updates and YCSB mixed workloads

| 50,000 point updates (median latency) | Linux x64 CI | Windows x64 CI | Apple M4 Pro (20k, 12 Nov) |
| --- | ---: | ---: | ---: |
| **DataVo Production** (`StrictFsync`) | 0.2405 ms | **3.9163 ms** | **4.0396 ms** |
| **SQLite WAL `FULL`** | **0.1076 ms** | **3.9218 ms** | **4.0419 ms** |
| **DataVo Relaxed** | **0.0086 ms** | 0.0272 ms | **0.0045 ms** |
| **SQLite WAL `NORMAL`** | 0.0114 ms | **0.0134 ms** | 0.0054 ms |

Where the operating system and drive enforce a genuine multi-millisecond flush (Apple's APFS SSD and the Windows runner's `FlushFileBuffers`), both durable engines hit the exact same **~3.9–4.0 ms** wall. Where flush is cheap (~0.1 ms on the Linux CI virtual disk), engine overhead becomes visible and SQLite WAL `FULL` is roughly **2.2× faster** per commit than DataVo Production.

Under mixed 80% read / 20% write YCSB load:

| YCSB 80/20 mixed | DataVo Relaxed | SQLite WAL `NORMAL` | DataVo Production |
| --- | ---: | ---: | ---: |
| **Linux x64 CI** (4 threads) | **230,912 ops/s** (write p99 `0.43 ms`) | 98,295 ops/s (`3.20 ms`) | 35,645 ops/s (`1.13 ms`) |
| **Windows x64 CI** (4 threads) | **70,735 ops/s** (`1.05 ms`) | 33,256 ops/s (`7.49 ms`) | 1,360 ops/s (`48.67 ms`) |
| **Apple M4 Pro** (12 threads, 12 Nov) | **510,695 ops/s** (`0.66 ms`) | 261,618 ops/s (`3.77 ms`) | 4,326 ops/s (`27.83 ms`) |

### Space and clean reopen

Inserting **1,000,000 records** in one batch, closing the database, and reopening it in the same process with a warm OS page cache (a clean reopen, not crash recovery):

| Platform & Engine | Batch insert | On disk | Reopen | First read after |
| --- | ---: | ---: | ---: | ---: |
| Apple M4 Pro · DataVo Production (`02fa240f`, 21 Nov) | 704.9 ms | **27.725 MB** | 4.5 ms | 3.8 ms |
| Apple M4 Pro · DataVo Relaxed (`02fa240f`, 21 Nov) | **403.1 ms** | **27.725 MB** | 1.7 ms | **< 0.1 ms** |
| Apple M4 Pro · SQLite `WAL NORMAL` (`02fa240f`, 21 Nov) | 697.4 ms | 29.082 MB | **0.4 ms** | 1.2 ms |
| Linux x64 · DataVo Production (20 Nov) | 1,365.8 ms | **28.155 MB** | 27.3 ms | 10.4 ms |
| Linux x64 · DataVo Relaxed (20 Nov) | **946.4 ms** | **28.155 MB** | 1.6 ms | **0.1 ms** |
| Linux x64 · SQLite `WAL NORMAL` (20 Nov) | 1,839.1 ms | 29.082 MB | **0.6 ms** | 3.9 ms |
| Windows x64 · DataVo Production (20 Nov) | 1,811.5 ms | **28.155 MB** | 40.3 ms | 14.1 ms |
| Windows x64 · DataVo Relaxed (20 Nov) | **1,299.3 ms** | **28.155 MB** | 3.9 ms | **0.1 ms** |
| Windows x64 · SQLite `WAL NORMAL` (20 Nov) | 2,605.2 ms | 29.082 MB | **1.8 ms** | 5.4 ms |

*(As with YCSB, the `WhitepaperEngineMatrix` harness does not currently instantiate an `SQLite WAL FULL` comparator in `space-and-recovery`; the matched pair in this table is `DataVo Relaxed` against `SQLite WAL NORMAL`.)*

### Concurrent operations, and two caveats worth flagging

The concurrent-ops scenario runs **8 reader threads and 2 writer threads for 5 seconds** against 100,000 in-memory records.

On 12 November (`0.2.0-rc.3`), DataVo's writers starved completely (**0 writes completed**) on the 4-core CI runners. Between 12 November and 20 November (`02fa240f`), a series of concurrency fixes landed—commit-ordered snapshots, bounded waits on the snapshot gate, corrected write-lock owner accounting, and coordination between row and table write locks—so writers completed on both CI runners in `44748508039` (`302,878` writes on Linux, `98,696` writes on Windows). However, in our 21 November re-measurement (`44916996459`), starvation still triggered intermittently under heavy oversubscription (10 active workers on 4 logical cores): DataVo completed `224,522` writes on Apple M4 Pro and `50,986` writes on Linux x64, but stalled at **0 writes** (`9,297 ms` total runtime) on Windows x64 CI.

*Update: the writer-stall root causes were fixed in `0.5.0` and `0.5.1` (single-row inserts now take a table intent lock plus per-key locks instead of an exclusive table lock; see [INSERT Intent & Key Locking](/manual/internals/insert-intent-row-locking)). `WriterProgressUnderReadLoadTests` now requires every writer to complete writes in every 300 ms window under continuous reads, on the Linux, macOS and Windows CI runners.*

We can visualize concurrent contention under multiple threads in Figure 8:

![Figure 8: Throughput and latency under 8-reader, 2-writer contention](/blog/part-1/04-concurrent-ops.png)

*Figure 8: Throughput and latency under 8-reader, 2-writer contention.*

| Platform | Engine | Reported ops/s | Reads | Writes | Read p99 | Write p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| **Apple M4 Pro** (`02fa240f`, 21 Nov) | DataVo (`InMemory`) | **1,087,329** | 5,214,990 | **224,522** | 0.190 ms | **0.620 ms** |
| **Apple M4 Pro** (`02fa240f`, 21 Nov) | SQLite (`WAL, normal`) | 389,341 | 1,857,503 | 91,458 | 0.142 ms | 1.299 ms |
| **Apple M4 Pro** (`02fa240f`, 21 Nov) | SQLite (`shared-cache`) | 381,982 | 1,914,864 | 2 | **0.111 ms** | 5,012 ms |
| **Linux x64 CI** (`44916996459`, 21 Nov) | DataVo (`InMemory`) | **1,552,810** | 7,719,876 | **50,986** | **0.006 ms** | **0.212 ms** |
| **Linux x64 CI** (`44916996459`, 21 Nov) | SQLite (`WAL, normal`) | 819,171 | 4,048,881 | 49,367 | 0.178 ms | 2.515 ms |
| **Linux x64 CI** (`44916996459`, 21 Nov) | SQLite (`shared-cache`) | 336,952 | 1,721,428 | 2 | 0.095 ms | 5,105 ms |
| **Windows x64 CI** (`44916996459`, 21 Nov) | DataVo (`InMemory`) | **1,879,968** | 17,477,611 | **0** *(starved)* | **0.006 ms** | *(0 writes)* |
| **Windows x64 CI** (`44916996459`, 21 Nov) | SQLite (`WAL, normal`) | 339,416 | 2,375,126 | **20,592** | 0.206 ms | **3.796 ms** |
| **Windows x64 CI** (`44916996459`, 21 Nov) | SQLite (`shared-cache`) | 613,042 | 3,855,821 | 1 | 0.012 ms | 6,286 ms |

Two distinct behaviours jump out of the full 21 November matrix:

1. **Why SQLite `shared-cache` completes only 1–2 writes everywhere.** `SqliteConcurrentOpsEngine` originally opened only `mode=memory&cache=shared`, which does not support WAL and takes table-level locks (`SQLITE_LOCKED`), so 8 continuous readers starve both writers until `PRAGMA busy_timeout = 3000` expires repeatedly (`5,012–6,286 ms` write p99). Adding a file-backed `SQLite (WAL, normal)` comparator (`journal_mode=WAL`, `synchronous=NORMAL`) eliminates SQLite's starvation across every OS (**91,458** writes on Apple M4 Pro, **49,367** on Linux CI, and **20,592** on Windows CI).
2. **Where DataVo wins and where it loses against SQLite `WAL, normal`.** Where DataVo's writers are not starved, DataVo beats `SQLite (WAL, normal)` cleanly: on Apple M4 Pro it delivers **2.79×** higher overall throughput (`1,087,329` vs `389,341` ops/s), **2.45×** more completed writes, and **2.1×** lower write p99 latency (`0.620 ms` vs `1.299 ms`); on Linux x64 CI it delivers **1.90×** higher overall throughput (`1,552,810` vs `819,171` ops/s), comparable completed writes (`50,986` vs `49,367`), **29.7×** lower read p99 (`0.006 ms` vs `0.178 ms`), and **11.9×** lower write p99 (`0.212 ms` vs `2.515 ms`). On the 4-vCPU Windows Hyper-V runner, however, `SQLite (WAL, normal)` sustained steady progress (`20,592` writes at `3.796 ms` p99) while DataVo's writers starved in this run (`0` writes; `9,297 ms` runtime), so mixed-contention reliability under Windows 4-vCPU oversubscription remains an open fix.

## Synchronising with PostgreSQL

For field-service tablets, desktop POS terminals, or collaborative .NET apps, the local engine is only half the story; the other half is keeping it in step with PostgreSQL over unreliable networks.

All sync measurements below ran on CI (`44756364158`, Ubuntu 26.04 x64, **2 vCPU**, PostgreSQL 17.12, loopback sockets) with nothing mocked: real PostgreSQL logical replication, real Kestrel WebSockets, and `electricsql/electric:latest` (`electric-1.8.1`) in Docker. On synced local replicas, point lookups take **4.06–5.19 µs** on DataVo versus **8.27–8.86 µs** on SQLite WAL.

![Figure 9: Warm initial sync of 100,000 rows over a local WebSocket link](/blog/part-1/02-initial-sync-100k.png)

*Figure 9: Warm initial sync of 100,000 rows over a local WebSocket link.*

| 100k-row warm initial sync (10k warm) | Sync time | Wire B/row | Client GC alloc |
| --- | ---: | ---: | ---: |
| **DataVo-Sync → `inmemory`** | **605 ms** (162 ms) | **45.5 B** | **36.4 MB** |
| **DataVo-Sync → `lsm`** | **726 ms** (226 ms) | **45.5 B** | **49.1 MB** |
| **DataVo-Sync → `lsm-strict`** | **706 ms** (80 ms) | **45.5 B** | **49.1 MB** |
| Custom JSON/WS → SQLite `WAL NORMAL` | 832 ms (195 ms) | 214.8 B | 129.9 MB |
| Custom JSON/WS + deflate → SQLite `NORMAL` | 852 ms (219 ms) | 58.5 B | 130.2 MB |
| Custom JSON/WS → SQLite `WAL FULL` | 702 ms (168 ms) | 214.8 B | 129.9 MB |
| Custom JSON/WS + deflate → SQLite `FULL` | 909 ms (218 ms) | 58.5 B | 130.0 MB |
| ElectricSQL `1.8.1` → SQLite `WAL NORMAL` | 1,167 ms (348 ms) | 362.7 B | 251.4 MB |

Four observations stand out across the sync suite (as seen in Figure 9):

- **Wire size and client memory.** Over loopback, wall-clock sync times are relatively close, but wire payload and GC pressure diverge sharply. DataVo-Sync transfers **45.5 B/row** (**4.7× smaller** than plain JSON and **8.0× smaller** than ElectricSQL) while allocating **36.4–49.1 MB** on the client (**2.6–3.6× less** than custom JSON/SQLite and **5.1–6.9× less** than ElectricSQL's `251.4 MB` / `259 MB` peak RSS). On cellular or metered field connections, wire size dominates sync time.
- **Live commit propagation (PG commit → visible locally).** DataVo-Sync `lsm` reaches **1.98 ms p50** (and **1.09 ms p50** in Scenario B), compared to **3.44 ms p50** for ElectricSQL's HTTP long-poll and **1.33–1.53 ms p50** for the custom direct-push JSON/WebSocket baseline.
- **Offline burst and convergence.** Two replicas (`A` and `B`) sync 1,000 rows from PostgreSQL; `A` disconnects, commits **500 local writes** (400 inserts and 100 updates), and reconnects. Across **6 of 6 CI runs**, PostgreSQL, `A`, and `B` converged identically row-for-row across all 1,400 rows: peer `B` observed `A`'s writes in **80.9–243.4 ms**, and PostgreSQL committed them in **188.6–259.0 ms**.
- **Fan-out and 1,000-client hub stress.** Fanning 20,000 rows out to 50 concurrent WebSocket clients finishes in **7.13 s** (**140,218 aggregate rows/s**). Pushing the hub alone (PostgreSQL bridge off) from **10 → 100 → 1,000 concurrent WebSocket clients** on a single 2-vCPU VM holds **9.8k–14.3k rows/s** aggregate throughput, graphed in Figure 10:

![Figure 10: WebSocket sync scaling from 10 to 1,000 concurrent clients on a 2-vCPU hub](/blog/part-1/03-websocket-client-scaling.png)

*Figure 10: WebSocket sync scaling from 10 to 1,000 concurrent clients on a 2-vCPU hub.*

At 1,000 concurrent clients saturating a 2-vCPU VM, median latency rises to **1,220 ms** and p99 to **4.6 s**. This stress test has no SQLite or ElectricSQL comparator.

## Where it loses, and choosing the right tool

Every storage engine is a set of trade-offs. Beyond the SQLite wins on `W6`, `C8`, and `G3` and the Linux durable-commit gap noted earlier, three limits are worth knowing before trying DataVo today:

- **Whole-table analytics without `[Columnar]`:** On full-table scans over non-columnar columns, **DuckDB** is **3–340× faster**.
- **Windows durable throughput:** Strict `FlushFileBuffers` caps single-commit latency at ~4 ms on Windows (affecting both DataVo Production and SQLite `FULL`), holding DataVo Production YCSB throughput on Windows to **1,360 ops/s**.
- **Sync hub startup and synced range scans:** The DataVo-Sync hub takes **2.4 s** to load 100k rows into memory on cold startup, and 100-row primary-key range aggregates on synced replicas are currently faster on SQLite (**16.8 µs** vs **133–277 µs**).

Put those limits together with the wins, and the decision boundary is straightforward:

- **Use SQLite** if you want the default embedded SQL database with the broadest SQL support, the largest ecosystem, and two decades of failure experience. DataVo's dialect is its own and its `0.5.1` on-disk format may still change.
- **Use DuckDB** if your workload is whole-table OLAP analytics over large datasets or Parquet/CSV files. It is one to three orders of magnitude faster than either DataVo or SQLite on those queries.
- **Use ElectricSQL** if you want read-path sync from PostgreSQL into a browser or app from an established, maintained product. My comparison covers one workload (initial sync and live latency over loopback), not its full feature set. In this setup it served as a read path; DataVo-Sync adds an offline write path, which is a different scope rather than a better implementation of the same one.
- **Try DataVo** if you are building in .NET and specifically want managed Native AOT packaging with zero native binaries, SQL + `VECTOR(n)` search + Z-set incremental views in one transaction log, or bidirectional offline-first PostgreSQL sync.

## Getting started

```bash
dotnet add package DataVo.Core
dotnet add package DataVo.Generators   # optional: zero-alloc compiled LINQ
```

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DataDirectory = "./app_data" });
db.Execute("CREATE TABLE Notes (Id INT PRIMARY KEY, Tag VARCHAR(32), Embedding VECTOR(384))");
```

- **Open-source engine and documentation:** [github.com/ArintonAkos/DataVo-DBMS](https://github.com/ArintonAkos/DataVo-DBMS) · [Full Manual](https://arintonakos.github.io/DataVo-DBMS/)
- **Commercial `DataVo-Sync` add-on:** Supports PostgreSQL and Supabase today (with SQL Server available on request). For evaluation builds or licensing, [request Early Access](https://tally.so/r/RWD8G9) or email `datavodb@proton.me`.

::: tip Source Code
The source code, benchmark harnesses, and raw CI logs for this series are in [`ArintonAkos/DataVo-DBMS`](https://github.com/ArintonAkos/DataVo-DBMS).
:::

## What comes next

- **[Part 2.](/blog/part-2-packed-buffers-aot-and-roslyn-codegen)** Packed byte buffers, Native AOT, and Roslyn codegen: how a 55 ns point lookup works with zero native binaries, prepared statements, and typed execution paths.
- **Part 3.** What an fsync costs, across operating systems: the 4 ms tie, the 0.1 ms Linux case, and the Windows scaling problem.
- **Part 4.** Z-sets, from incremental views to a 36 MB sync: wire protocol, offline conflict convergence, and reproducible Docker benchmarks against PostgreSQL 17 and ElectricSQL 1.8.1.
- **Part 5.** HNSW, the JIT, and the exact fallback: recall on clustered data, and why a graph search sometimes loses to a scan.
- **Part 6.** Differential oracle testing: how our matrix caught five wrong-answer bugs before we tuned a single microsecond.
- **Part 7.** Building an offline-first .NET 10 app end-to-end: combining Native AOT, filtered `VECTOR(n)` search, live Z-set views, and bidirectional Postgres sync in a real desktop/mobile architecture.

## References

<ol class="ref-list">
  <li id="ref-1">Martin Kleppmann, Adam Wiggins, Peter van Hardenberg, Mark McGranaghan. "Local-First Software: You Own Your Data, in spite of the Cloud." <em>Onward! 2019</em> (ACM SIGPLAN).</li>
  <li id="ref-2">Martin Kleppmann. <em>Designing Data-Intensive Applications.</em> O'Reilly Media, 2017.</li>
  <li id="ref-3">Patrick O'Neil, Edward Cheng, Dieter Gawlick, Elizabeth O'Neil. "The Log-Structured Merge-Tree (LSM-Tree)." <em>Acta Informatica</em> 33(4), pp. 351–385, 1996.</li>
  <li id="ref-4">Daniel J. Abadi, Samuel R. Madden, Nabil Hachem. "Column-Stores vs. Row-Stores: How Different Are They Really?" <em>SIGMOD 2008</em>.</li>
  <li id="ref-5">Yu. A. Malkov, D. A. Yashunin. "Efficient and Robust Approximate Nearest Neighbor Search Using Hierarchical Navigable Small World Graphs." <em>IEEE Transactions on Pattern Analysis and Machine Intelligence</em>, vol. 42, no. 4, pp. 824–836, 2020 (online 2018).</li>
  <li id="ref-6">Mihai Budiu, Frank McSherry, Leonid Ryzhyk, Val Tannen. "DBSP: Automatic Incremental View Maintenance for Rich Query Languages." <em>Proceedings of the VLDB Endowment</em> 16(7), pp. 1601–1614, 2023.</li>
  <li id="ref-7">Manuel Rigger, Zhendong Su. "Testing Database Engines via Pivoted Query Synthesis." <em>OSDI 2020</em>.</li>
  <li id="ref-8">Donald R. Slutz. "Massive Stochastic Testing of SQL." <em>VLDB 1998</em>.</li>
</ol>

### Data sources

- DataVo-DBMS: `README.md`, `docs/manual/performance/benchmarks.md`, `docs/manual/performance/datavo-vs-sqlite.md`, and raw results under `docs/public/benchmarks/` (`2026-11-12`, `2026-11-16-v04-final`, `2026-11-17-engine-perf-followups`, `2026-11-21-m4pro`, `2026-11-21-ci`).
- Apple M4 Pro BenchmarkDotNet (`02fa240f`, 20 Nov 2026, `--job short`), local M4 Pro `concurrent-ops` and `space-and-recovery` (`02fa240f`, 21 Nov 2026, under `docs/public/benchmarks/2026-11-21-m4pro/`), CI run `44748508039` (`master` @ `02fa240f`, 20 Nov 2026, Linux & Windows x64), and CI re-run `44916996459` (`21 Nov 2026`, Linux & Windows x64 `concurrent-ops` with `SQLite WAL NORMAL` under `docs/public/benchmarks/2026-11-21-ci/`).
- DataVo-Sync CI run `44756364158` (20 Nov 2026, Ubuntu 26.04 x64, PG 17.12, ElectricSQL `1.8.1`): `compare.md`, `e2e.md`, `stress-10-100-1000.md`.
