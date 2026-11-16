# DataVo vs SQLite: When Each Engine Wins

This page compares DataVo with SQLite on 28 relational query shapes over 1,000,000 orders and on five filtered nearest-neighbour queries, with DuckDB as an analytical reference. It records where each engine is faster and why. Measured on 14 November 2026 on an Apple M4 Pro. The first run that day found five queries on which DataVo returned a wrong answer. All five are fixed, and every number on this page comes from a second run against the fixed engine. The raw output of both runs is in [`docs/public/benchmarks/2026-11-14/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-14); the first run is in its `pre-fix/` folder. On 15 November `OR` and `LIKE` moved onto DataVo's typed scan (commit `9ffb413`) and the SQL matrix was run again; the filter rows W3–W6 and C6 come from that run, whose output is in [`docs/public/benchmarks/2026-11-15/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-15). Later that day the general join path stopped joining once per comparison (commit `b26e70e`) and the typed join learned `LEFT JOIN`, three or more tables and `ORDER BY ... LIMIT` (commit `80db3ba`); the join rows J1–J7 come from a third run against that engine, in [`docs/public/benchmarks/2026-11-15-joins/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-15-joins). Every other row was within run-to-run noise of the earlier numbers, which are kept. In the evening HNSW's diversity heuristic became the default (commit `8e472ab`) and its connectivity repair was fixed (`52b78cf`); the vector section comes from a run against that engine, in [`docs/public/benchmarks/2026-11-15-hnsw/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-15-hnsw). Later still, the typed join stopped hashing whole rows it does not read, learned to return joined rows without an `ORDER BY ... LIMIT`, and began reading a large table through an index when a small dimension selects few of its rows (commits `ee787ba`–`269f226`); that run is in [`docs/public/benchmarks/2026-11-15-left-join/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-15-left-join). On 16 November `GROUP BY` stopped boxing its keys, the LSM engine stopped copying rows it reads by key, a bound query vector stopped being parsed once per row, and the InMemory engine learned to read primary-key ranges (commits `534edbc`–`b952574`); the join, range, `GROUP BY` and vector rows now come from a final run of both scenarios against that engine, in [`docs/public/benchmarks/2026-11-16-backlog/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-16-backlog). The filter and columnar rows are within run-to-run noise there and keep their earlier numbers.

**The short version.** DataVo is faster than SQLite on primary-key and secondary-index lookups, on full-table `GROUP BY` with up to about a thousand groups, on inner joins of two or three tables, aggregated or returning a top-N, including a join driven by a selective dimension through an index, on a `LEFT JOIN` aggregate, on aggregates its columnar index can answer, and on vector search, including a vector search joined to another table. SQLite is 2.3 times faster on `GROUP BY` over about 125,000 groups of a key that has an index, which it reads in key order, and 1.5 times faster on a `LEFT JOIN` anti-join, which it answers with an index probe per row where DataVo hashes the other table. On full scans filtered with `OR` or `LIKE` SQLite is 1.5 to 2 times faster. Before 15 November, `OR`, `LIKE`, joins of three tables, `LEFT JOIN` and joins returning rows left DataVo's typed paths and were 7 to 109 times slower. Every engine now returns the exact answer on every relational query. See [Correctness findings](#correctness-findings) for what was wrong and what the fixes cost.

## Decision guide

Ratios compare medians, DataVo's LSM engine against SQLite (WAL, `synchronous=NORMAL`), one thread each, 1,000,000 orders unless stated.

| Your workload | Faster engine | By how much | Evidence |
| --- | --- | ---: | --- |
| Primary-key range read, 100 or 10,000 rows | DataVo LSM | 1.4–1.7× | [R1–R4](#primary-key-ranges) |
| Primary-key range read on the **InMemory** engine | SQLite | 1.2× | [R1–R4](#primary-key-ranges) |
| Equality on an indexed column, about 1,000 rows | DataVo | 2.7× | [W1](#filters) |
| Equality on a column without an index | SQLite | 1.2× | [W2](#filters) |
| `OR` in `WHERE`, full scan | SQLite | 1.7–2.0× | [W3, W4, C6](#filters) |
| `OR` of numeric comparisons, columnar index on those columns, `COUNT`/`SUM`/... | DataVo | 7.6× | [C6](#columnar-index) |
| `LIKE`, prefix or substring, no index | SQLite | 1.5–1.8× | [W5, W6](#filters) |
| `GROUP BY` over the whole table, 4 to 1,000 groups | DataVo | 2.9–3.2× | [G1, G2, C7](#group-by-cardinality) |
| `GROUP BY` over about 125,000 groups, on an indexed key, `HAVING` + top-100 | SQLite | 2.3× | [G3](#group-by-cardinality) |
| Two-table inner join + `GROUP BY` (fact × dimension) | DataVo | 5.0× | [J6](#joins) |
| Three-table inner join, aggregate over all rows | DataVo | 8.7× | [J1](#joins) |
| Three-table inner join filtered on both dimensions, aggregate or `ORDER BY ... LIMIT 50` | DataVo | 3.9–4.6× | [J2, J3](#joins) |
| `LEFT JOIN` aggregate (`COUNT(*)`, `COUNT(c.Id)`, `SUM`) | DataVo | 2.9× | [J4](#joins) |
| `LEFT JOIN ... WHERE c.Id IS NULL` (anti-join) | SQLite | 1.5× | [J5](#joins) |
| Small dimension filtered, joined to a large table through an index | DataVo | 2.2× | [J7](#joins) |
| `COUNT`/`SUM`/`MIN`/`MAX` with an AND of numeric ranges, columnar index on those columns | DataVo | 5.0–13.9× | [C1, C2, C4](#columnar-index) |
| The same, without a columnar index | SQLite | 1.5–2.1× | [C1–C4](#columnar-index) |
| `SUM`/`AVG` of a `DOUBLE`/`FLOAT` column on the LSM engine, columnar index | SQLite | 2.0× | [C3](#columnar-index): the LSM engine does not push floating-point sums down |
| `ORDER BY ... LIMIT 10` over the whole table | SQLite | 1.6× | [C8](#columnar-index) |
| Top-10 nearest neighbours, one table, with or without a filter, exact | DataVo Flat | 2.2–8.7× | [V0–V2](#hybrid-vector-and-sql) |
| Top-10 nearest neighbours, no filter or a broad one, approximate (HNSW) | DataVo HNSW | 14–73× over sqlite-vec's exact scan; recall 99.9–100% on this data | [V0, V2](#hybrid-vector-and-sql) |
| Top-10 nearest neighbours **joined to another table**, exact | DataVo Flat | 1.2–2.1× | [V3, V4](#hybrid-vector-and-sql) |
| Analytical scans, joins and aggregates that read the whole table | DuckDB | 3–2,600× over both | every table below; DuckDB is the slowest of the three on key ranges and index lookups |

Two rules follow from the reasons behind these numbers, explained in each section:

1. **DataVo is fast when a query stays on one of its typed paths:** one table with a `WHERE` built from `AND`, `OR`, `LIKE` and `column op value` comparisons; or INNER and `LEFT` joins of any number of tables on integer keys, aggregated or returning rows; or an index lookup. Anything else (an `OR` across two joined tables, `RIGHT`/`FULL` joins, joins on text or GUID keys, `DISTINCT`, computed expressions) is evaluated row by row with a dictionary per row, at 2–4 KB of allocation per row read, and is many times slower than SQLite.
2. **SQLite wins wherever an index can replace a scan or a hash table,** because its planner uses indexes for joins, grouping and ordering. DataVo's planner uses a secondary index to drive an INNER join from a selective dimension (J7) and its primary key for key ranges, but not to drive a `GROUP BY` or an outer join.

## Correctness findings

Each query is checked against an exact answer computed from the data generator before it is timed. SQLite and DuckDB returned that answer on every query in both runs. The first run, against the unmodified engine (commit `9625ac1`), found five queries on which DataVo, on both its LSM and InMemory engines, did not. Each has its own fix commit with regression tests on both engines. In the second run DataVo returned the oracle's answer on every relational query and parameter set, and the vector shapes returned no row that fails the filter.

| # | Query | Before the fix | Correct | Cause | Fix |
| --- | --- | --- | --- | --- | --- |
| B1 | `SELECT COUNT(*) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id WHERE c.Id IS NULL` (J5) | 0 | 250,395 | The general join path evaluates each `WHERE` comparison by filtering the table it names and joining outward from the survivors (`StatementEvaluator`). On the null-supplied side of an outer join that filter runs before the NULL padding exists. Starting from that table also flips which side the join keeps, so `LEFT JOIN ... WHERE c.Region = 'R1'` returned customers without orders as extra rows. | `bfb1390`: a comparison filters before the join only when that is equivalent (all joins inner, or a comparison on the FROM table with joins that keep it). Otherwise the full join is built once and its rows are filtered. |
| B2 | `SELECT COUNT(DISTINCT CustomerId) FROM Orders` | 2,000 (the row count) | 175 | `DISTINCT` inside an aggregate's parentheses was dropped by the parser; the query ran as `COUNT(CustomerId)`. Found by a probe on 2,000 rows; not in the timed matrix. | `6743521`: `COUNT/SUM/AVG/MIN/MAX(DISTINCT x)` are parsed and computed on the general aggregation path. The typed, Volcano, columnar and typed-join fast paths decline them, and reactive subscriptions reject them with an error. |
| B3 | Top-10 by `d.Emb <=> @q` with `JOIN Authors a` and `WHERE d.Category = @c AND d.Price < @p` (V3) | 0–2 rows, recall@10 5% | 10 rows | The planner extracts the documents' own filter to size the candidate fetch. It compared the column qualifier `d` with the table name `Docs`, so with aliases nothing was extracted and only 10 candidates were fetched. | `8ce4295`: qualifiers are resolved through the table service. |
| B4 | `... JOIN Customers c ... JOIN Products p ... WHERE c.Region = @r AND p.CategoryId = @cat` (J2, J3) | 0 rows | 1,827 rows (J2) | Each comparison is evaluated from its own table, and the partial results are intersected on their joined row keys. Those keys listed the per-table row ids in an order that depended on the starting table (`[order, customer, product]` from one side, `[order, product, customer]` from the other), so `AND` across two different joined tables matched almost nothing and `OR` gave wrong counts. | `fd81cf8`: keys are rewritten into one canonical table order after every join. |
| B5 | Top-10 by `d.Emb <=> @q` with `JOIN Authors a` and `WHERE a.Country = @k` (V4) | 10 rows, of which 9 on average fail the filter | 10 rows from that country | The vector join path applied only the part of `WHERE` that names the vector table; the `a.Country` predicate was dropped. An inner join also returned fewer than 10 rows when a candidate had no join partner. | `8ce4295`: candidates are pre-filtered, joined and filtered by the whole `WHERE`. The candidate count grows with the observed survival rate until the `LIMIT` is filled, else the query falls back to the exact general path. |

**A correction to the first report.** The first version of this page described B4 as an equality filter on a dimension column combined with dangling foreign keys. That diagnosis was wrong. The probe that suggested it had data in which every order of the filtered customers also had a dangling key, so 0 was the right answer there. The real trigger is an `AND` or `OR` of comparisons on two different joined tables. Dangling keys play no part.

**What the fixes cost.** J2 and J3 take the same time as before (2.3 s and 2.2 s on LSM, against 2.35 s and 2.17 s), because they already ran two full joins. J5 now builds the full join once and filters its 1,000,000 rows, and takes 4.0 s where the wrong answer took 2.0 s. Vector joins fetch more candidates and join them: on 50,000 × 384 vectors, Flat V3 takes 5.3 ms (was 2.1 ms with a wrong answer) and V4 8.2 ms (was 2.9 ms). Every other query is within the run-to-run variation of the first run.

## How the comparison was made

### Engines and settings

| Engine | Configuration |
| --- | --- |
| DataVo (LSM) | `StorageMode.Lsm`, `LsmStrictFsync = false` (Relaxed: the commit log is not flushed per commit). Loaded with `InsertTypedBatch`, then closed and reopened so every query reads from SSTables (the operating system's page cache is warm). |
| DataVo (InMemory) | `StorageMode.InMemory`, loaded with `InsertTypedBatch`. |
| SQLite | `Microsoft.Data.Sqlite` 10.0.0 with the bundled SQLite 3.50.4. File database, `journal_mode=WAL`, `synchronous=NORMAL`, no connection pooling. Loaded in one transaction, then `ANALYZE`, then reopened. |
| DuckDB | `DuckDB.NET.Data.Full` 1.5.3, file database, default thread count (12). Loaded with the appender, then reopened. It is a column store and is shown as the analytical reference, not as a like-for-like competitor. |
| SQLite + sqlite-vec | `sqlite-vec` v0.1.9 (the `vec0.dylib` checked into the repository root), loaded into the same SQLite build, WAL, `synchronous=NORMAL`. |

Neither DataVo Relaxed nor SQLite `NORMAL` makes a commit durable against power loss; they are the comparable pair. Every query runs as a statement prepared once per engine and executed with bound parameters (`@name`; DuckDB `$name`). Every engine reads every cell of every result row, so result materialization is part of every engine's time. One thread issues the queries. DuckDB may use its own threads inside a query; DataVo and SQLite do not.

### Data

Every column is a function of the row id (`SqlMatrixData.cs`), so the data is identical in each engine and in the oracle.

```sql
Customers (Id INT PRIMARY KEY, Name VARCHAR(16), Segment INT, Region VARCHAR(4))          -- 100,000 rows, 8 regions
Products  (Id INT PRIMARY KEY, Name VARCHAR(16), CategoryId INT, Price DOUBLE)              -- 1,000 rows, 50 categories
Orders    (Id INT PRIMARY KEY, CustomerId INT, ProductId INT, ProductTag INT, Status VARCHAR(12),
           Amount DOUBLE, Qty INT, Discount DOUBLE, Bucket INT, Sku VARCHAR(16))             -- 1,000,000 rows
CREATE INDEX ix_orders_customer ON Orders (CustomerId);
CREATE INDEX ix_orders_product  ON Orders (ProductId);
```

- 25% of orders reference a customer id between 100,001 and 125,000, which has no row, so outer joins have unmatched rows.
- `ProductTag` always equals `ProductId` but has no index, so W1 and W2 differ only in the index.
- `Discount` is NULL in about 10% of rows. `Bucket` has 1,000 values; `Status` four upper-case values; `Sku` looks like `S0042-PAID`.
- Every `DOUBLE` is a multiple of 0.25 below 1,000, so every sum is exact in any order of addition, and answers are compared exactly, bit for bit, never with a tolerance.
- DataVo's `FLOAT` is a 32-bit type, so the DataVo schema uses `DOUBLE` to match SQLite's `REAL` and DuckDB's `DOUBLE`.

### Answer checks and timing

Before timing a query, the runner executes it with each of its parameter sets and compares a normalised digest of the rows (integers as integers whatever their CLR type, other numbers in round-trip form, rows sorted unless the SQL fixes the order) with the oracle's. A mismatch is reported and the cell is marked `✗`; it is not a result. Then come unmeasured warm-up calls and the timed calls: 10 for full-table queries, 100 for 10,000-row ranges, 1,000 for selective queries, cycling through the parameter sets, with a full garbage collection before the timed block. Medians and p99 come from those calls; with 10 calls the p99 is the slowest call. Managed allocation is `GC.GetAllocatedBytesForCurrentThread` over the timed calls; SQLite's and DuckDB's native memory is not visible to it.

### Machine

Apple M4 Pro, 12 cores, 48 GiB, macOS 26.3 (Darwin 25.3.0), .NET 10.0.3, Arm64 RyuJIT. Engine commit `6743521` (`9625ac1` plus the four fix commits) for the 14 November runs, `9ffb413` for the filter rows, and `b952574` for the join, range and `GROUP BY` rows and the vector section; benchmark code `638324d`, unchanged across all runs. One run per scenario, no CPU pinning; the last runs each started with no other build or test process and a load average under 2.5 (a 15 November 384-dimension vector run, started seconds after a build, was discarded; see its `environment.txt`). The 14 November SQL matrix took 21 minutes and peaked at 5.6 GB resident, the last one 2 minutes and 1.1 GB; the vector scenarios took 39 and 62 seconds at about 1 GB. As on the [results page](./benchmarks.md), treat differences under about 20% as not established.

### Reproducing

```bash
dotnet build demos/Research.Benchmark/src/Research.Benchmark.Host -c Release
DLL=demos/Research.Benchmark/src/Research.Benchmark.Host/bin/Release/net10.0/Research.Benchmark.Host.dll

dotnet "$DLL" --scenario sql-matrix                       # 1,000,000 orders; --records, --customers, --products,
                                                          # --scan-reps, --medium-reps, --selective-reps, --engine datavo,datavo-mem,sqlite,duckdb
SQLITE_VEC_PATH=$PWD/vec0.dylib \
dotnet "$DLL" --scenario hybrid-vector                    # 50,000 x 384; --vectors, --dimensions, --queries, --engine datavo-flat,datavo-hnsw,sqlite
SQLITE_VEC_PATH=$PWD/vec0.dylib \
dotnet "$DLL" --scenario hybrid-vector --vectors 20000 --dimensions 1536
```

Results go to standard output as Markdown, progress to standard error.

## Results

Median latency of one call, p99 in brackets. The last column is SQLite's median divided by DataVo LSM's: above 1, DataVo is faster.

### Setup

| | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB |
| --- | ---: | ---: | ---: | ---: |
| Load 1,101,000 rows (ms) | 3,671 | 545 | 4,892 (incl. `ANALYZE`) | 1,021 |
| Reopen (ms) | 455 | – | 2.0 | 233 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.0 |
| Create the columnar index (ms) | 316 | 186 | – | – |

### Joins

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| J1 Three-table inner join, `COUNT`, `SUM`, no filter | 84.5 ms (92.6 ms) | 98.5 ms | 738 ms (747 ms) | 2.08 ms | **8.73×** |
| J2 J1 + `WHERE c.Region = @r AND p.CategoryId = @cat` (~0.2%) | 22.0 ms (25.9 ms) | 11.1 ms | 86.1 ms | 1.49 ms | **3.91×** |
| J3 J2 returning rows, `ORDER BY o.Amount DESC, o.Id LIMIT 50` | 18.7 ms (22.9 ms) | 11.8 ms | 86.7 ms | 1.93 ms | **4.65×** |
| J4 `LEFT JOIN`: `COUNT(*)`, `COUNT(c.Id)`, `SUM` | 83.5 ms (86.1 ms) | 98.3 ms | 238 ms | 3.40 ms | **2.85×** |
| J5 `LEFT JOIN ... WHERE c.Id IS NULL` | 56.4 ms (57.1 ms) | 89.2 ms | 36.5 ms | 3.30 ms | 0.65× |
| J6 Orders × Customers, `GROUP BY c.Region` | 154 ms (172 ms) | 207 ms | 773 ms | 3.37 ms | **5.01×** |
| J7 Products (one category, 2%) × Orders, `COUNT`, `SUM` | 7.03 ms (8.05 ms) | 6.10 ms | 15.5 ms | 1.04 ms | **2.21×** |

| Allocated per call, DataVo LSM | Before `ee787ba` | After `269f226` (15 November) | Now |
| --- | ---: | ---: | ---: |
| J1 | 41.2 MB | 6.2 MB | 3.0 MB |
| J2 / J3 | 11.0 / 11.5 MB | 9.9 / 16.6 MB | 5.0 / 11.6 MB |
| J4, J5 | 40.8 MB each | 2.9 MB each | 2.9 MB each |
| J6 | 63.7 MB | 47.3 MB | 24.4 MB |
| J7 | 58.2 KB | 6.2 MB | 30.2 KB |

Before commits `b26e70e` and `80db3ba`, J1–J5 took 2.2–5.0 s on DataVo LSM (0.01–0.14× SQLite) and allocated 2.1–3.7 GB per call. The `80db3ba` run measured J1 181 ms, J2/J3 53 ms, J4 201 ms, J5 165 ms, J6 212 ms and J7 37.2 ms on LSM.

**Why.** All seven joins take DataVo's typed join (`Select.TypedJoin.cs`). It handles INNER and `LEFT` equalities on integer columns over any number of tables, per-table `WHERE` conditions (including `OR` and `LIKE` within one table), aggregates with or without `GROUP BY`, and joined rows, either the top rows of an `ORDER BY ... LIMIT` or every row. One table is streamed: the largest when every join is INNER, the FROM table when a `LEFT JOIN` must keep its rows or rows are returned without a top-N. Every other table is hashed by its join key, and each streamed row is extended table by table; a `LEFT` step pads an unmatched row with NULLs. A condition on an INNER-joined table filters it while it is hashed. A condition on a `LEFT`-joined table is tested after the join on the matched row or on the NULLs, which is SQL's meaning (correctness finding B1). Only the columns the conditions, keys, groups, aggregates and sort keys read are decoded, and those a joined row reads are decoded only once a streamed row matches.

**What changed in the last run (`ee787ba`–`269f226`).** Until then a hashed table kept every column of every row, as a copied array in a list per key, so that it could build whole rows. J1, J4 and J5 hashed all 100,000 customers this way and allocated 41 MB per call. A hashed table now keeps only the columns read after the join (`Select.TypedJoinBuild.cs`): those in the SELECT list, GROUP BY, aggregates and ORDER BY, the conditions of a `LEFT`-joined table, and the key of the next table in a chain. When that is just the join key, as in J1, J2, J4, J5 and J7, it keeps only a count of rows per key. J5's anti-join therefore reads the orders and one integer per customer, and takes 57 ms instead of 165 ms. When every join is INNER and a hashed table has few keys, the streamed table may instead be read through a single-column index on its join column (`Select.TypedJoinIndex.cs`). That happens when an estimate from 32 of the keys says the index names at most an eighth of the table's rows. The rows are read in key order, the order a scan visits them, so results come out in the same order. J7 now reads the 20,000 orders of the 20 matching products, as SQLite does (`SCAN p | SEARCH o USING INDEX ix_orders_product`), and is 2.2 times faster than SQLite. J2 and J3 read the orders of their 20 products the same way, then filter the customers by hash.

SQLite still wins J5: it scans the orders and probes the customers' primary key, while DataVo builds a hash of 100,000 keys and probes it, which costs it 57 ms against SQLite's 37 ms. J1, J4 and J6 remain DataVo's largest margins: SQLite runs them as index nested-loop joins (`SCAN c | SEARCH o USING INDEX ix_orders_customer | SEARCH p USING INTEGER PRIMARY KEY`, and for J6 a temporary B-tree for `GROUP BY`), a B-tree probe per joined row, where DataVo probes an in-memory hash. J6 boxed its group key, the customer's region, into an `object[]` for each of its 750,000 joined rows; since `534edbc` a one-column group key is looked up by its value, and J6 went from 185 ms and 47 MB to 154 ms and 24 MB, most of what is left being the hash of 100,000 customers with their regions. On LSM the index-driven joins allocated far more than on InMemory (J7 6.2 MB against 0.3 MB): the LSM engine copied every row it read by key into a new array, and every index lookup built two new lists. Since `c690598` it visits such a row from a reused buffer, an index lookup appends to the caller's list, and the planner keeps the rows of the keys it sampled instead of looking them up twice; J7 allocates 30 KB per call. J2 and J3 still allocate more on LSM than on InMemory (5.0 and 11.6 MB against 1.9 and 4.8 MB): hashing the filtered customers decodes their region and name strings from the row bytes, the per-row string cost described under [Filters](#filters).

**Joins that return rows.** Before `65d6434`, a join returning rows without an `ORDER BY ... LIMIT` ran on the general join path, which builds a dictionary per row of every table. These shapes are not in the matrix; [`join-rows.txt`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-15-left-join/join-rows.txt) times four of them on the same data, DataVo LSM:

| Query | Before (general path) | Now (typed join) |
| --- | ---: | ---: |
| `SELECT o.Id, c.Region ... LEFT JOIN Customers c ... WHERE c.Region = 'R1'` (93,684 rows) | 4.59 s, 3.36 GB | 457 ms, 303 MB |
| The same with `LIMIT 100` | 4.37 s, 3.35 GB | 18.6 ms, 24.6 MB |
| `SELECT o.Id, c.Name ... JOIN ... WHERE c.Region = 'R1' AND o.Qty = 1` (9,316 rows) | 1.90 s, 2.04 GB | 81.2 ms, 62.5 MB |
| `SELECT o.Id ... LEFT JOIN ... WHERE c.Id IS NULL` (250,395 rows) | 4.63 s, 3.47 GB | 994 ms, 751 MB |

The remaining time is mostly building the returned rows, a dictionary each, which the API returns.

The general join path (`StatementEvaluator`) still serves the shapes the typed join does not take. Since commit `b26e70e` it joins once per query: conjuncts on one table filter that table before the join when that is equivalent, and the others are tested on the joined rows. It used to run a complete join for each `WHERE` comparison.

### Filters

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| W1 `WHERE ProductId = @p` (indexed, ~1,000 rows) | 303 µs (1.10 ms) | 185 µs (451 µs) | 820 µs (938 µs) | 1.12 ms | **2.70×** |
| W2 `WHERE ProductTag = @p` (no index, ~1,000 rows) | 35.1 ms (37.1 ms) | 68.3 ms | 28.7 ms | 0.52 ms | 0.82× |
| W3 `(Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= @lo AND Amount < @hi` | 100 ms (104 ms) | 108 ms | 60.4 ms | 2.21 ms | 0.60× |
| W4 W3 `AND Discount IS NOT NULL AND Discount < @d` | 112 ms (143 ms) | 110 ms | 62.8 ms | 2.23 ms | 0.56× |
| W5 `Sku LIKE 'S0042%'` (no index) | 73.7 ms (74.6 ms) | 67.2 ms | 42.2 ms | 3.04 ms | 0.57× |
| W6 `Sku LIKE '%042-%'` (no index) | 77.7 ms (78.2 ms) | 76.1 ms | 51.4 ms | 4.01 ms | 0.66× |

W3–W6 are from the 15 November run. Before commit `9ffb413` they took 1.66–4.35 s on DataVo LSM and allocated 1.8–4.0 GB per call.

**Why.** DataVo's typed single-table scan (`Select.TypedScan.cs`, `TryCollectSidePredicates`) takes a `WHERE` built from `AND`, `OR`, `LIKE`/`NOT LIKE` and `column op literal` comparisons with `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS NULL` and `IS NOT NULL`. It decodes only the filtered columns and builds a result row only for matches. An `OR` becomes one predicate whose branches are AND lists. The parser pushes `NOT` down into the comparisons, and `WHERE` keeps only rows that are true, so treating a branch that compared a NULL as false gives SQL's three-valued result. `LIKE` is matched by `SqlLikePattern`, which tests the prefix (`'S0042%'`), suffix and substring (`'%042-%'`) shapes with ordinal span searches and anything else with a wildcard matcher. Until 15 November an `OR` or a `LIKE` sent the whole query to the general path, which built a dictionary for each of the million rows. That was the 33–69× gap.

SQLite is still 1.5 to 1.8 times faster. On the LSM engine W3–W6 allocate 36–46 MB per call, about one string per row read: the size matches the `Status` or `Sku` value decoded from the row bytes into a .NET string before it is compared. The InMemory engine holds those strings already and allocates 16 KB to 3.3 MB per call, yet is no faster, so the allocation is not the whole difference. I did not profile the rest of the per-row cost.

On the typed path the gap is small. W2 reads every row on both engines (SQLite: `SCAN Orders`) and SQLite is 1.2 times faster. W1 uses the index on both, and DataVo is 2.7 times faster: its secondary index returns all matching row keys in one lookup (`IndexManager.FilterUsingIndex`), and the typed scan then reads those rows by key. I did not profile where SQLite spends its 820 µs. The W1 p99 of 1.10 ms comes from garbage collection: 555 KB allocated per call, 67 gen-0 and 24 gen-1 collections over 1,000 calls. SQLite allocates 49 KB per call.

### Primary-key ranges

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| R1 `Id BETWEEN @lo AND @hi`, 100 rows | 12.8 µs (22.0 µs) | 21.7 µs (28.1 µs) | 18.4 µs (26.7 µs) | 171 µs | **1.44×** |
| R2 `Id >= @lo AND Id < @hi`, 100 rows | 12.5 µs (16.6 µs) | 22.0 µs (30.8 µs) | 18.5 µs (24.4 µs) | 171 µs | **1.48×** |
| R3 `BETWEEN`, 10,000 rows | 1.07 ms (12.4 ms) | 2.06 ms (14.9 ms) | 1.72 ms (1.91 ms) | 591 µs | **1.62×** |
| R4 `>= AND <`, 10,000 rows | 1.04 ms (12.1 ms) | 1.99 ms (15.9 ms) | 1.72 ms (1.86 ms) | 596 µs | **1.66×** |

**Why.** On the LSM engine the typed scan turns a range on the clustered integer key into a seek (`KeyRangeOf` → `LsmEngine.ScanStoredRows`), the same access pattern as SQLite's `SEARCH Orders USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)`. It reads two columns of each row from the memory-mapped SSTable, and is 1.4 to 1.7 times faster at both widths. `BETWEEN` and `>= AND <` take the same path on every engine.

The 10,000-row p99 of DataVo (12.0–12.3 ms, against a 1.05 ms median) is garbage collection: 3.0 MB allocated per call, with 4 gen-2 collections over 100 calls. SQLite's p99 stays within 1.2 times of its median.

**The InMemory engine reads key ranges from its primary-key lane.** Its rows are kept in insertion order, and its integer primary key is a hash map from key to row, with no order. Until commit `b952574` a range visited every row and dropped those outside it: about 70 ms for 100 of the million rows. The key map now answers ranges from a sorted copy of its keys, made by the first range query (once, about 100 ms for a million keys; 8 bytes per key) and kept current by logging the keys inserted since; a removed key is skipped by looking it up. The rows in the range are read by key, in the order a scan visits them. A 100-row range takes 22 µs, 1.2 times SQLite's time, and a 10,000-row range 2.0 ms. A range covering more than a quarter of the table is still scanned. The LSM engine remains faster: its rows are stored in key order, so a range is a seek and a sequential read.

### GROUP BY cardinality

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| G1 `GROUP BY Status` (4 groups) | 81.7 ms (82.6 ms) | 94.7 ms | 238 ms | 1.11 ms | **2.91×** |
| G2 `GROUP BY Bucket` (1,000 groups) | 66.3 ms (69.7 ms) | 80.5 ms | 210 ms | 0.73 ms | **3.17×** |
| G3 `GROUP BY CustomerId HAVING COUNT(*) >= @k ORDER BY N DESC, CustomerId LIMIT 100` (~125,000 groups) | 67.4 ms (73.1 ms) | 104 ms | 29.8 ms | 4.48 ms | 0.44× |

| Memory during the timed calls | DataVo (LSM), 14 November | DataVo (LSM), now | SQLite (managed only) |
| --- | ---: | ---: | ---: |
| G1, per call | 64.9 MB | 34.4 MB; 43 gen-0, 0 gen-2 over 10 calls | 720 B |
| G2, per call | 56.0 MB | 2.1 MB; 2 gen-0 | 70.7 KB |
| G3, per call | 260 MB; 17 gen-2 | 15.9 MB; 8 gen-0, 3 gen-1, 0 gen-2 | 5.2 KB |

**Why.** DataVo's typed scan aggregates into a hash table while it streams the rows. SQLite sorts into a temporary B-tree (`SCAN Orders | USE TEMP B-TREE FOR GROUP BY`), and with few groups the hash table is 2.9 to 3.2 times faster. In G3 SQLite reads the orders already in `CustomerId` order from the covering index (`SCAN Orders USING COVERING INDEX ix_orders_customer`), so it groups as it goes, with no table of groups and no scan of the wider rows. DataVo does not use a secondary index to drive a `GROUP BY`; it scans and hashes, and is 2.3 times slower.

Until commit `534edbc` it was 13 times slower (391 ms, 260 MB per call). Each row built a boxed `object[]` key, each of the 125,000 groups was an object with five arrays, and each group became a row with two dictionaries so that the general `HAVING` and `ORDER BY` could read it, before `LIMIT` kept 100. Now a one-column key is looked up by its value, the groups' aggregates live in arrays indexed by group number, and `HAVING`, `ORDER BY` and `LIMIT` are applied to the groups first, with the general evaluator on one reused row (so they mean exactly what they meant). Only the 100 groups returned become rows. G2 gained from the same change (97 to 66 ms). G1 still allocates 34 MB per call on LSM, one decoded `Status` string per row; the InMemory engine, which holds the strings, allocates 24 KB.

### Columnar index

DataVo's columnar index (`CREATE INDEX cx_orders ON Orders (Amount, Qty, Bucket, Discount, ProductTag) USING COLUMNAR`) was created after the queries above, and the queries were timed again. SQLite and DuckDB are shown once. "Pushed" means the index answered the query; the runner reads the index's scan counter around each query to tell.

| Query | DataVo (LSM) | DataVo (LSM) + index | DataVo (InMemory) + index | SQLite | DuckDB | SQLite / DataVo + index |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| C1 `COUNT(*) WHERE Amount >= @lo AND Amount < @hi` | 65.8 ms | 3.15 ms, pushed | 3.33 ms, pushed | 43.6 ms | 1.09 ms | **13.9×** |
| C2 `SUM(Qty) WHERE Bucket < @b` | 77.5 ms | 3.44 ms, pushed | 3.70 ms, pushed | 37.0 ms | 0.43 ms | **10.8×** |
| C3 `SUM(Amount) WHERE Qty >= @q` (DOUBLE sum) | 76.9 ms | 77.6 ms, row scan | 3.12 ms, pushed | 39.8 ms | 0.50 ms | 0.51× |
| C4 `COUNT(*), SUM(Qty), MIN(Amount), MAX(Amount) WHERE Bucket < @b AND Discount IS NOT NULL` | 99.2 ms | 11.5 ms, pushed | 12.0 ms, pushed | 57.7 ms | 1.39 ms | **5.02×** |
| C5 `COUNT(*) WHERE Status = @s AND Amount >= @lo` (string column) | 67.8 ms | 67.8 ms, row scan | 74.0 ms, row scan | 42.6 ms | 0.91 ms | 0.63× |
| C6 `COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= @lo` (15 November) | 70.1 ms | 4.62 ms, pushed | 0.49 ms, pushed | 35.0 ms | 1.62 ms | **7.58×** |
| C7 `GROUP BY Bucket`: `COUNT(*)`, `SUM(Qty)` | 95.9 ms | 95.9 ms, row scan | 104 ms, row scan | 200 ms | 0.73 ms | 2.09× |
| C8 `ORDER BY Amount DESC, Id LIMIT 10` | 65.3 ms | 65.8 ms, row scan | 82.0 ms, row scan | 40.6 ms | 1.30 ms | 0.62× |

Pushed queries allocate 14–20 KB per call, against 4.6–25 MB per call for the same query as a row scan.

**Which SQL shapes the index answers** (`Select.Columnar.cs`): a single-table `COUNT`, `SUM`, `AVG`, `MIN` or `MAX` without `GROUP BY` and without `DISTINCT`, whose `WHERE` is built from `AND` and `OR` over comparisons, `IS NULL` or `IS NOT NULL` between an indexed numeric column and a value of a compatible type, and whose aggregated columns are all in the index. An `OR` is answered since commit `9ffb413` (C6; before it, C6 took 2.25 s as a general-path row scan). Not answered: anything with `GROUP BY` (C7), `ORDER BY` (C8), `LIKE`, a column outside the index such as a string column (C5), or a statement inside an explicit transaction.

**Floating-point sums on the LSM engine (C3).** On the LSM engine the in-memory part of the columnar index is the tail of rows not yet flushed, and an index in that state reports itself as not in key order (`ColumnarIndex.InKeyOrder`). A `SUM` or `AVG` of a `FLOAT` or `DOUBLE` column is then left to the row scan, because floating-point addition in a different order can give different bits than the row scan's. This is deliberate: the index only answers when it can return exactly the row scan's result. On the InMemory engine the same query is pushed. Integer sums, counts, minimums and maximums are pushed on both.

**DuckDB is still faster.** Using all 12 cores and its own column storage, DuckDB answers C1 and C2 3 to 8 times faster than DataVo's columnar index. The [BenchmarkDotNet comparison](./benchmarks.md#with-a-columnar-index), where DataVo leads DuckDB, holds DuckDB to one thread and measures DataVo's compiled LINQ queries, not SQL text.

### Hybrid vector and SQL

`Docs (Id INT PRIMARY KEY, Category INT, Price DOUBLE, AuthorId INT, Emb VECTOR(384))`: 50,000 documents drawn around 128 random cluster centres and normalised. Ten categories, prices from 0 to 99.75, 2,000 authors in ten countries. The relational attributes are independent of the clusters. Each shape runs 200 held-out query vectors (not in the table) after 10 warm-up queries. Recall@10 compares the ten returned ids with the exact top-10 by cosine distance **among the rows that satisfy the `WHERE` clause**, computed by brute force in the runner. "Short" counts queries that returned fewer than 10 rows; "failing rows" counts returned rows that do not satisfy the `WHERE` clause.

DataVo runs `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE ... ORDER BY dist LIMIT 10`, prepared, on the LSM engine with a `FLAT` or `HNSW` index at default settings. SQLite uses sqlite-vec's documented filtered-KNN form: a `vec0(id INTEGER PRIMARY KEY, emb float[384] distance_metric=cosine, category INTEGER, price FLOAT, author_id INTEGER)` table whose filter columns are metadata columns, queried as `WHERE emb MATCH @q AND k = 10 AND category = @c AND price < @p`. The join shapes run that KNN in a `MATERIALIZED` CTE and join `Authors` to its ten rows. V4 constrains the KNN with `author_id IN (SELECT Id FROM Authors WHERE Country = @k)`, which sqlite-vec v0.1.9 accepts on a metadata column. sqlite-vec's `vec0` is an exact scan, with no approximate index.

| Shape | DataVo Flat | DataVo HNSW | SQLite + sqlite-vec | sqlite-vec / Flat |
| --- | --- | --- | --- | ---: |
| Load + index build | 331 ms | 1,600 ms | 644 ms | |
| V0 top-10, no filter | 2.13 ms (17.3 ms), recall 100% | 0.25 ms (0.53 ms), recall 99.9% (min 90%) | 18.5 ms (19.2 ms), recall 100% | **8.7×** |
| V1 `Category = @c AND Price < 50` (~5%) | 3.61 ms (8.73 ms), 100% | 6.71 ms (19.3 ms), 100% | 9.88 ms (10.5 ms), 100% | **2.7×** |
| V2 `Price < 50` (~50%) | 2.11 ms (4.25 ms), 100% | 0.74 ms (2.00 ms), 100% (min 90%) | 14.5 ms (15.2 ms), 100% | **6.9×** |
| V3 V1 + `JOIN Authors` | 4.76 ms (13.1 ms), 100% | 7.77 ms (22.5 ms), 100% | 9.86 ms (10.5 ms), 100% | **2.1×** |
| V4 `JOIN Authors WHERE a.Country = @k` (~10%) | 7.23 ms (17.7 ms), 100% | 6.15 ms (14.8 ms), 100% | 13.2 ms (13.7 ms), 100% | **1.8×** |

The same scenario with 20,000 documents of 1,536 dimensions (`hybrid-vector-1536.md`):

| Shape | DataVo Flat | DataVo HNSW | SQLite + sqlite-vec | sqlite-vec / Flat |
| --- | --- | --- | --- | ---: |
| Load + index build | 472 ms | 2,000 ms | 4,009 ms | |
| V0 | 4.53 ms (20.9 ms), 100% | 0.61 ms (0.99 ms), 100% | 32.2 ms (33.2 ms), 100% | **7.1×** |
| V1 (~5%) | 7.07 ms (16.8 ms), 100% | 10.7 ms (27.0 ms), 100% | 15.5 ms (16.5 ms), 100% | **2.2×** |
| V2 (~50%) | 4.30 ms (8.55 ms), 100% | 1.62 ms (3.98 ms), 100% | 23.5 ms (24.6 ms), 100% | **5.5×** |
| V3 | 7.33 ms (19.6 ms), 100% | 10.9 ms (28.8 ms), 100% | 15.5 ms (16.6 ms), 100% | **2.1×** |
| V4 | 14.4 ms (19.4 ms), 100% | 9.89 ms (19.8 ms), 100% | 17.5 ms (18.5 ms), 100% | **1.2×** |

The 15 November run, before commit `3939f1d`, measured V0 at 2.71 ms (Flat) and 0.66 ms (HNSW) at 384 dimensions and 6.24 ms and 2.40 ms at 1,536; see Allocation below for what changed.

No DataVo shape returned a row that fails its filter, and no shape, Flat or HNSW, returned fewer than 10 rows. Before the fixes, V3 returned recall 5% with every query short, and V4 returned about 1,800 failing rows per 200 queries (see `pre-fix/`).

**DataVo's planner routes,** from its diagnostics (`RuntimeQueryStats` and the hybrid-routing counters), recorded in a separate untimed pass over the same 200 queries at 384 dimensions (the 1,536-dimension run routed the same way, with no fallback either):

| Shape | Route | Candidates fetched from the vector index (Flat) | Expansion passes (mean) | Fallback to the general path |
| --- | --- | --- | ---: | ---: |
| V0 | vector index, baseline | 10 | 0 | 0 of 200 |
| V1 | vector index, adaptive initial top-k, filter applied to candidates | median 286, max 572 | 0.10 (Flat and HNSW) | 0 |
| V2 | vector index, adaptive | median 29, max 58 | 0.05 (Flat and HNSW) | 0 |
| V3 | vector index, adaptive; candidates pre-filtered, joined, filtered by the whole `WHERE` | median 286, max 1,073 | 0.10 (Flat and HNSW) | 0 |
| V4 | vector index, baseline; grows with the share of candidates that survive the join filter | median 150, max 600 | 1.49 (Flat and HNSW) | 0 |

The join shapes read the `Authors` table for every query (the runner's "full table scan" counter reports 200 of 200).

**Why DataVo Flat is faster than sqlite-vec on V0–V2.** Both compute exact distances. DataVo's Flat index runs its vectorised distance kernels (`SimdDistanceKernels`, NEON on this machine) and is 7.1 to 8.7 times faster than `vec0` without a filter. I did not profile sqlite-vec, so I cannot say how much of the gap is the kernel and how much the scan around it. With a filter the margin shrinks to 2.2–2.7 times, from both sides. sqlite-vec applies metadata filters inside its scan and computes no distance for a row that fails: its V1 is 1.9 times faster than its V0. DataVo instead asks the index for the top 286 to 572 candidates of the whole table, loads those rows, and filters them (5.0 MB allocated per query at 384 dimensions). When fewer than 10 candidates pass, it fetches twice as many, up to two more times. With Flat the candidates are exact, so the filtered top-10 is exact whenever enough candidates pass, which it was in all 200 queries.

**Vector search joined to another table (V3, V4).** DataVo pre-filters the candidates with the predicates on `Docs`, joins them to `Authors`, and filters the joined rows with the whole `WHERE`. If fewer than 10 rows survive, the next fetch is sized from the share that survived, with 50% headroom: for V4, where one candidate in ten survives, 10 candidates become about 150. If two expansions are not enough, the query runs on the general path, which reads every document and is exact but slow (about 350 ms here). In this run that never happened, with Flat or HNSW. DataVo Flat is 1.8–2.1 times faster than sqlite-vec on V3 and V4 at 384 dimensions, 2.1 times on V3 and 1.2 times on V4 at 1,536 dimensions, where the joined candidates' 6 KB vectors are the main cost. sqlite-vec pushes the author filter into its scan as an `IN` list on a metadata column, so it never computes a distance for a document of another country.

**HNSW on this data.** HNSW is the fastest engine on V0, V2 and V4, at recall@10 of 100% on every shape and both sizes except V0 at 384 dimensions, 99.9%; the lowest single query was 90% (V0 and V2, 384 dimensions). HNSW's level sampling is not seeded, so each build's graph differs slightly; the 15 November build had 100% on V0. It is slower than Flat on V1 and V3, where the filter keeps about 5% of the rows and the 286 or more candidates it must return cost more to find in a graph than to scan. HNSW is approximate: these figures hold for this data, and a different distribution can give lower recall.

Before 15 November HNSW's recall on this data was 81.6–93.7%, and 12 to 21 of the 200 queries found none of their true top-10 at any `EfSearch` up to 512. The index linked every node only to its nearest neighbours. The data has 128 well-separated clusters (cluster noise is 0.46 of the centre's length), so almost no edge left a cluster: 4–6% of the edges on levels 0 to 3 of the 384-dimension graph. When the descent through the upper levels entered the wrong cluster, the search could not get out, and a wider search only explored that cluster further. Two fixes followed:

- **Diversity heuristic on by default (`8e472ab`).** A candidate that is closer to an already chosen neighbour than to the node is skipped in favour of one in another direction, as in the HNSW paper. On levels 2 and 3 of the same graph, the share of cross-cluster edges rose from 4–5% to 23% and 47%, so the descent now reaches the right cluster. An index loaded from a snapshot keeps the setting it was built with. To rebuild one made before this change, drop the index and create it again. SQL does not expose the setting; in C#, `HNSWIndex.EnableDiversityHeuristic = false` still opts out.
- **Connectivity repair (`52b78cf`).** After each parallel batch, a repair pass links nodes no search can reach. To make room in a full neighbour list it dropped any edge whose target had two incoming edges. In a nearest-neighbour graph, nodes of a cluster point at each other, so the single edge into a cluster qualified, and builds without the heuristic ended with 4 to 817 unreachable nodes. The pass now never drops an edge of the spanning tree it walked, and the same builds end with none.

An ablation that built the index directly from the same data, one configuration per process (`hnsw-ablation.txt`), shows that the heuristic is the fix. A wider construction search alone does not fix it:

| Construction setting | Dimensions | Build | Recall@10, default width | Queries at 0%: default width / `EfSearch` 512 | Median query |
| --- | ---: | ---: | ---: | ---: | ---: |
| Nearest neighbours only (old default) | 384 | 1,690 ms | 83.6% | 20 / 19 | 0.158 ms |
| Diversity heuristic (new default) | 384 | 1,592 ms | 100.0% | 0 / 0 | 0.166 ms |
| Nearest only, `EfConstruction` uncapped | 384 | 5,520 ms | 89.8% | 17 / 13 | 0.164 ms |
| Nearest only, `EfConstruction` 200 | 384 | 2,777 ms | 86.8% | 15 / 13 | 0.162 ms |
| Diversity, `EfConstruction` uncapped | 384 | 7,063 ms | 100.0% | 0 / 0 | 0.252 ms |
| Nearest neighbours only (old default) | 1,536 | 1,800 ms | 87.5% | 15 / 12 | 0.284 ms |
| Diversity heuristic (new default) | 1,536 | 1,900 ms | 100.0% | 0 / 0 | 0.421 ms |

The heuristic does not change build time beyond noise or the memory the index holds (the neighbour lists have a fixed size). What it costs is query time at 1,536 dimensions, 0.28 to 0.42 ms median, because the search now visits more of the graph. The July measurement on the [results page](./benchmarks.md#vector-search), where the heuristic made the build four times slower, predates the incremental pruning and the parallel build. On uniform random vectors it now costs 8–11% more build time at the same recall (10,000 × 1,536 and 50,000 × 384, measured with the same probe).

**Allocation.** sqlite-vec allocates 2–7 KB of managed memory per query. DataVo allocated 1.2 MB per unfiltered query at 384 dimensions, growing with the dimensions, not the rows. An allocation profile showed about 1 MB of strings: a `float[]` bound as a query parameter was formatted to vector text, and the distance column (`Emb <=> @q AS dist`) parsed that text again for every row it was computed on and copied every stored vector it compared. Since commit `3939f1d` a literal's vector is parsed once per query, a parameter bound from a `float[]` carries the floats so it is not parsed at all (its text, which `INSERT` and `UPDATE` read, is unchanged), and stored vectors are read in place. V0 now allocates 135 KB per query at 384 dimensions and 384 KB at 1,536, and HNSW, which computes few distances, became 2.6 to 3.9 times faster on V0. The filtered shapes still allocate 4.6–5.0 MB per query at 384 dimensions and 8–17 MB at 1,536: the 150 to 1,073 candidate rows they load, decode and filter. DataVo Flat's p99 is 1.3–8 times its median, against at most 1.1 times for sqlite-vec. Garbage collection is the likely cause, but I did not measure it.

## What this does not show

- One machine, one run per scenario, Apple Silicon only. The [results page](./benchmarks.md#part-iii-platforms) shows that DataVo's lead over SQLite on scans is 2 to 2.8 times smaller on x64; expect the ratios here to move as well.
- Data was in the operating system's page cache. A cold read from disk was not measured.
- Only read queries were timed. Load times are shown, but write-heavy workloads are covered on the [results page](./benchmarks.md), not here.
- SQLite and DuckDB are driven through ADO.NET, whose cost is part of their times; DataVo's own API cost is part of its time. Neither SQLite nor DuckDB was tuned beyond `ANALYZE`; DataVo had no columnar index except in the columnar section.
- The vector attributes are independent of the clusters. A filter correlated with distance (for example, a category that matches the query's cluster) changes how many candidates DataVo needs; that was not measured.
- `COUNT(DISTINCT ...)` is correct now but was not timed.
- DataVo is a 0.2 release candidate; SQLite and DuckDB are mature engines with general query optimisers. DataVo's remaining losses here are full scans that decode a string per row (W3–W6, C5), a planner that does not use a secondary index to drive a `GROUP BY` (G3) or an outer join (J5), and the candidate rows a filtered vector query loads.
