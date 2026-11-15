# DataVo vs SQLite: When Each Engine Wins

This page compares DataVo with SQLite on 28 relational query shapes over 1,000,000 orders and on five filtered nearest-neighbour queries, with DuckDB as an analytical reference. It records where each engine is faster and why. Measured on 14 November 2026 on an Apple M4 Pro. The first run that day found five queries on which DataVo returned a wrong answer. All five are fixed, and every number on this page comes from a second run against the fixed engine. The raw output of both runs is in [`docs/public/benchmarks/2026-11-14/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-14); the first run is in its `pre-fix/` folder. On 15 November `OR` and `LIKE` moved onto DataVo's typed scan (commit `9ffb413`) and the SQL matrix was run again; the filter rows W3–W6 and C6 come from that run, whose output is in [`docs/public/benchmarks/2026-11-15/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-15). Every other row was within run-to-run noise of the 14 November numbers, which are kept.

**The short version.** DataVo is faster than SQLite on primary-key and secondary-index lookups, on full-table `GROUP BY` with up to about a thousand groups, on a two-table join with `GROUP BY`, on aggregates its columnar index can answer, and on vector search, including a vector search joined to another table. SQLite is faster, often by one to two orders of magnitude, on anything that leaves DataVo's typed execution paths: joins of three tables, `LEFT JOIN`, joins that return rows, and `GROUP BY` over a key that has an index. On full scans filtered with `OR` or `LIKE` SQLite is now 1.5 to 2 times faster, down from 33 to 69 times. Every engine now returns the exact answer on every relational query. See [Correctness findings](#correctness-findings) for what was wrong and what the fixes cost.

## Decision guide

Ratios compare medians, DataVo's LSM engine against SQLite (WAL, `synchronous=NORMAL`), one thread each, 1,000,000 orders unless stated.

| Your workload | Faster engine | By how much | Evidence |
| --- | --- | ---: | --- |
| Primary-key range read, 100 or 10,000 rows | DataVo LSM | 1.4–1.7× | [R1–R4](#primary-key-ranges) |
| Primary-key range read on the **InMemory** engine | SQLite | about 3,900× | [R1–R4](#primary-key-ranges): InMemory scans the whole table |
| Equality on an indexed column, about 1,000 rows | DataVo | 2.7× | [W1](#filters) |
| Equality on a column without an index | SQLite | 1.2× | [W2](#filters) |
| `OR` in `WHERE`, full scan | SQLite | 1.7–2.0× | [W3, W4, C6](#filters) |
| `OR` of numeric comparisons, columnar index on those columns, `COUNT`/`SUM`/... | DataVo | 7.6× | [C6](#columnar-index) |
| `LIKE`, prefix or substring, no index | SQLite | 1.5–1.8× | [W5, W6](#filters) |
| `GROUP BY` over the whole table, 4 to 1,000 groups | DataVo | 2.1–2.7× | [G1, G2, C7](#group-by-cardinality) |
| `GROUP BY` over about 125,000 groups, on an indexed key, `HAVING` + top-100 | SQLite | 13× | [G3](#group-by-cardinality) |
| Two-table inner join + `GROUP BY` (fact × dimension) | DataVo | 3.4× | [J6](#joins) |
| Small dimension filtered, joined to a large table through an index | SQLite | 2.6× | [J7](#joins) |
| Three-table join | SQLite | 7–27× | [J1–J3](#joins) |
| `LEFT JOIN` | SQLite | 19×; 109× for an anti-join | [J4, J5](#joins) |
| `COUNT`/`SUM`/`MIN`/`MAX` with an AND of numeric ranges, columnar index on those columns | DataVo | 5.0–13.9× | [C1, C2, C4](#columnar-index) |
| The same, without a columnar index | SQLite | 1.5–2.1× | [C1–C4](#columnar-index) |
| `SUM`/`AVG` of a `DOUBLE`/`FLOAT` column on the LSM engine, columnar index | SQLite | 2.0× | [C3](#columnar-index): the LSM engine does not push floating-point sums down |
| `ORDER BY ... LIMIT 10` over the whole table | SQLite | 1.6× | [C8](#columnar-index) |
| Top-10 nearest neighbours, one table, with or without a filter, exact | DataVo Flat | 1.6–7.2× | [V0–V2](#hybrid-vector-and-sql) |
| Top-10 nearest neighbours **joined to another table**, exact | DataVo Flat | 1.0–1.9× | [V3, V4](#hybrid-vector-and-sql) |
| Analytical scans, joins and aggregates that read the whole table | DuckDB | 3–2,600× over both | every table below; DuckDB is the slowest of the three on key ranges and index lookups |

Two rules follow from the reasons behind these numbers, explained in each section:

1. **DataVo is fast when a query stays on one of its typed paths:** one table with a `WHERE` built from `AND`, `OR`, `LIKE` and `column op value` comparisons; or exactly two tables in an inner integer-key join with an aggregate; or an index lookup. Anything else is evaluated row by row with a dictionary per row, at 2–4 KB of allocation per row read, and is many times slower than SQLite.
2. **SQLite wins wherever an index can replace a scan or a hash table,** because its planner uses indexes for joins, grouping and ordering. DataVo's planner does not use a secondary index to drive a join or a `GROUP BY`.

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

Apple M4 Pro, 12 cores, 48 GiB, macOS 26.3 (Darwin 25.3.0), .NET 10.0.3, Arm64 RyuJIT. Engine commit `6743521` (`9625ac1` plus the four fix commits); benchmark code `638324d`, unchanged between the two runs. One run per scenario, no CPU pinning, background processes not controlled. The SQL matrix took 21 minutes and peaked at 5.6 GB resident; the vector scenarios took 47 and 88 seconds. As on the [results page](./benchmarks.md), treat differences under about 20% as not established.

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
| J1 Three-table inner join, `COUNT`, `SUM`, no filter | 5.03 s (5.49 s) | 5.17 s | 725 ms (728 ms) | 2.09 ms | 0.14× |
| J2 J1 + `WHERE c.Region = @r AND p.CategoryId = @cat` (~0.2%) | 2.34 s (2.44 s) | 2.60 s | 85.3 ms | 1.50 ms | 0.04× |
| J3 J2 returning rows, `ORDER BY o.Amount DESC, o.Id LIMIT 50` | 2.20 s (2.36 s) | 2.52 s | 85.6 ms | 1.95 ms | 0.04× |
| J4 `LEFT JOIN`: `COUNT(*)`, `COUNT(c.Id)`, `SUM` | 4.52 s (4.87 s) | 4.34 s | 239 ms | 3.48 ms | 0.05× |
| J5 `LEFT JOIN ... WHERE c.Id IS NULL` | 4.00 s (4.84 s) | 4.14 s | 36.8 ms | 3.25 ms | 0.01× |
| J6 Orders × Customers, `GROUP BY c.Region` | 223 ms (227 ms) | 258 ms | 765 ms | 3.35 ms | **3.44×** |
| J7 Products (one category, 2%) × Orders, `COUNT`, `SUM` | 40.6 ms (41.0 ms) | 75.7 ms | 15.4 ms | 1.07 ms | 0.38× |

**Why.** DataVo has one fast join path (`Select.TypedJoin.cs`): exactly two tables, an inner join on one integer column, an aggregate, and per-table AND filters. It hashes the smaller table and streams the larger one, decoding only the columns it needs. J6 and J7 take it. J6 is DataVo's win: SQLite's plan (`SCAN c | SEARCH o USING INDEX ix_orders_customer | USE TEMP B-TREE FOR GROUP BY`) probes the order index 100,000 times and sorts into a temporary B-tree. In J7, SQLite's plan (`SCAN p | SEARCH o USING INDEX ix_orders_product`) reads only the 20,000 orders of the 20 matching products through the index, while DataVo's hash join streams all 1,000,000 orders.

Every other join leaves that path: three tables (J1–J3), no aggregate (J3), or `LEFT JOIN` (J4, J5). The general path materializes each joined row as dictionaries, about 3.7 KB of allocation per order in J1 (3.7 GB per call, 33 gen-2 collections over 10 calls). For each `WHERE` comparison it runs a complete join starting from the table that comparison names, then intersects the results (`StatementEvaluator.And`), so J2 joins the million orders twice. A comparison on the null-supplied side of a `LEFT JOIN` (J5) is applied to the rows of one full join. SQLite runs J1–J3 as index nested-loop joins (`SCAN c | SEARCH o USING INDEX ix_orders_customer | SEARCH p USING INTEGER PRIMARY KEY`) and J5 as a scan of the orders with an index probe into customers.

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
| R1 `Id BETWEEN @lo AND @hi`, 100 rows | 12.8 µs (17.2 µs) | 70.7 ms (73.5 ms) | 18.3 µs (21.2 µs) | 170 µs | **1.43×** |
| R2 `Id >= @lo AND Id < @hi`, 100 rows | 12.5 µs (16.5 µs) | 69.1 ms | 18.4 µs (25.0 µs) | 173 µs | **1.47×** |
| R3 `BETWEEN`, 10,000 rows | 1.05 ms (12.3 ms) | 69.8 ms | 1.74 ms (1.93 ms) | 601 µs | **1.66×** |
| R4 `>= AND <`, 10,000 rows | 1.05 ms (12.0 ms) | 70.8 ms | 1.73 ms (1.93 ms) | 608 µs | **1.64×** |

**Why.** On the LSM engine the typed scan turns a range on the clustered integer key into a seek (`KeyRangeOf` → `LsmEngine.ScanStoredRows`), the same access pattern as SQLite's `SEARCH Orders USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)`. It reads two columns of each row from the memory-mapped SSTable, and is 1.4 to 1.7 times faster at both widths. `BETWEEN` and `>= AND <` take the same path on every engine.

The 10,000-row p99 of DataVo (12.0–12.3 ms, against a 1.05 ms median) is garbage collection: 3.0 MB allocated per call, with 4 gen-2 collections over 100 calls. SQLite's p99 stays within 1.2 times of its median.

**The InMemory engine has no ordered access to its rows.** `StorageContext.ScanTableRows` hands the in-memory backend the key bounds, but it visits every row and drops those outside the range (`VisitTypedRowInRange`). A 100-row range therefore costs a pass over the million rows, about 70 ms. This is not a problem with the measurement: use the LSM engine, or a point lookup, for key ranges.

### GROUP BY cardinality

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| G1 `GROUP BY Status` (4 groups) | 89.7 ms (91.3 ms) | 96.4 ms | 238 ms | 1.12 ms | **2.65×** |
| G2 `GROUP BY Bucket` (1,000 groups) | 96.9 ms (98.3 ms) | 106 ms | 209 ms | 0.73 ms | **2.16×** |
| G3 `GROUP BY CustomerId HAVING COUNT(*) >= @k ORDER BY N DESC, CustomerId LIMIT 100` (~125,000 groups) | 391 ms (454 ms) | 423 ms | 29.7 ms | 4.40 ms | 0.08× |

| Memory during the timed calls | DataVo (LSM) | SQLite (managed only) |
| --- | ---: | ---: |
| G1, per call | 64.9 MB; 81 gen-0, 0 gen-2 over 10 calls | 720 B |
| G2, per call | 56.0 MB; 70 gen-0, 0 gen-2 | 70.7 KB |
| G3, per call | 260 MB; 328 gen-0, 149 gen-1, 17 gen-2 | 5.2 KB |

**Why.** DataVo's typed scan aggregates into a hash table while it streams the rows. SQLite sorts into a temporary B-tree (`SCAN Orders | USE TEMP B-TREE FOR GROUP BY`), and with few groups the hash table is 2.2 to 2.7 times faster. In G3 SQLite reads the orders already in `CustomerId` order from the covering index (`SCAN Orders USING COVERING INDEX ix_orders_customer`), so it groups as it goes, with no table of groups and no scan of the wider rows. DataVo does not use a secondary index to drive a `GROUP BY`. It builds 125,000 groups whose keys are boxed `object[]` arrays, allocating 260 MB per call, and is 13 times slower.

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
| Load + index build | 345 ms | 1,935 ms | 643 ms | |
| V0 top-10, no filter | 2.59 ms (16.1 ms), recall 100% | 0.66 ms (1.03 ms), recall 81.6% (min 0%) | 18.6 ms (19.9 ms), recall 100% | **7.2×** |
| V1 `Category = @c AND Price < 50` (~5%) | 4.24 ms (9.72 ms), 100% | 7.10 ms (19.7 ms), 92.0%, 4 short | 9.79 ms (10.3 ms), 100% | **2.3×** |
| V2 `Price < 50` (~50%) | 2.75 ms (5.36 ms), 100% | 1.31 ms (2.69 ms), 83.0% | 14.6 ms (15.3 ms), 100% | **5.3×** |
| V3 V1 + `JOIN Authors` | 5.31 ms (14.3 ms), 100% | 8.40 ms (273 ms), 93.7% | 9.83 ms (10.1 ms), 100% | **1.9×** |
| V4 `JOIN Authors WHERE a.Country = @k` (~10%) | 8.19 ms (16.5 ms), 100% | 6.71 ms (361 ms), 91.4% | 13.2 ms (13.5 ms), 100% | **1.6×** |

The same scenario with 20,000 documents of 1,536 dimensions (`hybrid-vector-1536.md`):

| Shape | DataVo Flat | DataVo HNSW | SQLite + sqlite-vec | sqlite-vec / Flat |
| --- | --- | --- | --- | ---: |
| Load + index build | 472 ms | 1,778 ms | 3,971 ms | |
| V0 | 6.06 ms (28.6 ms), 100% | 2.22 ms (2.75 ms), 85.6% | 32.1 ms (32.9 ms), 100% | **5.3×** |
| V1 (~5%) | 9.76 ms (21.0 ms), 100% | 11.9 ms (29.7 ms), 87.2%, 24 short | 15.4 ms (18.6 ms), 100% | **1.6×** |
| V2 (~50%) | 6.86 ms (12.0 ms), 100% | 3.60 ms (5.63 ms), 86.6% | 23.4 ms (24.0 ms), 100% | **3.4×** |
| V3 | 10.3 ms (22.3 ms), 100% | 12.7 ms (334 ms), 93.1% | 15.3 ms (16.2 ms), 100% | **1.5×** |
| V4 | 17.3 ms (23.8 ms), 100% | 10.1 ms (452 ms), 91.1% | 17.3 ms (18.5 ms), 100% | 1.0× |

No DataVo shape returned a row that fails its filter, and no Flat shape returned fewer than 10 rows. Before the fixes, V3 returned recall 5% with every query short, and V4 returned about 1,800 failing rows per 200 queries (see `pre-fix/`).

**DataVo's planner routes,** from its diagnostics (`RuntimeQueryStats` and the hybrid-routing counters), recorded in a separate untimed pass over the same 200 queries at 384 dimensions:

| Shape | Route | Candidates fetched from the vector index (Flat) | Expansion passes (mean) | Fallback to the general path |
| --- | --- | --- | ---: | ---: |
| V0 | vector index, baseline | 10 | 0 | 0 of 200 |
| V1 | vector index, adaptive initial top-k, filter applied to candidates | median 286, max 572 | 0.10 (Flat), 0.14 (HNSW) | 0 |
| V2 | vector index, adaptive | median 29, max 58 | 0.05 (Flat), 0.02 (HNSW) | 0 |
| V3 | vector index, adaptive; candidates pre-filtered, joined, filtered by the whole `WHERE` | median 286, max 1,073 | 0.10 (Flat), 0.08 (HNSW) | 0 (Flat), 4 (HNSW) |
| V4 | vector index, baseline; grows with the share of candidates that survive the join filter | median 150, max 600 | 1.49 (Flat), 1.47 (HNSW) | 0 (Flat), 4 (HNSW) |

The join shapes read the `Authors` table for every query (the runner's "full table scan" counter reports 200 of 200).

**Why DataVo Flat is faster than sqlite-vec on V0–V2.** Both compute exact distances. DataVo's Flat index runs its vectorised distance kernels (`SimdDistanceKernels`, NEON on this machine) and is 5.3 to 7.2 times faster than `vec0` without a filter. I did not profile sqlite-vec, so I cannot say how much of the gap is the kernel and how much the scan around it. With a filter the margin shrinks to 1.6–2.3 times, from both sides. sqlite-vec applies metadata filters inside its scan and computes no distance for a row that fails: its V1 is 1.9 times faster than its V0. DataVo instead asks the index for the top 286 to 572 candidates of the whole table, loads those rows, and filters them (6.8 MB allocated per query at 384 dimensions). When fewer than 10 candidates pass, it fetches twice as many, up to two more times. With Flat the candidates are exact, so the filtered top-10 is exact whenever enough candidates pass, which it was in all 200 queries.

**Vector search joined to another table (V3, V4).** DataVo pre-filters the candidates with the predicates on `Docs`, joins them to `Authors`, and filters the joined rows with the whole `WHERE`. If fewer than 10 rows survive, the next fetch is sized from the share that survived, with 50% headroom: for V4, where one candidate in ten survives, 10 candidates become about 150. If two expansions are not enough, the query runs on the general path, which reads every document and is exact but slow (about 350 ms here). With Flat that never happened. DataVo Flat is 1.5–1.9 times faster than sqlite-vec on V3 and level with it on V4 at 1,536 dimensions, where the joined candidates' 6 KB vectors are the main cost. sqlite-vec pushes the author filter into its scan as an `IN` list on a metadata column, so it never computes a distance for a document of another country.

**HNSW on this data.** HNSW is the fastest on V0, V2 and V4, but its recall is 81.6–93.7% across the shapes and both sizes, with a minimum of 0%. In a separate probe (`EfSearch` 64, 128, 256, 512), 15 of the 200 queries had 0% recall at every `EfSearch`. Their results had a mean distance of 0.74 where the true top-10 had 0.15. The search ended in a different cluster, and widening the search did not bring it back. This data has well-separated clusters (cluster noise is 0.46 of the centre's length), which is hard for a graph index, and the [earlier recall experiment](./benchmarks.md#vector-search) measured 90–96% on clustered data from a different generator. With a selective filter, HNSW's approximate candidates can run out. V1 returned fewer than 10 rows for 4 queries at 384 dimensions and 24 at 1,536 (0 and 18 in the first run, whose graph was built in a different order). On the join shapes, the same shortage sends 3–23 queries to the exact general path, which is HNSW's p99 of 273–452 ms. Use Flat up to at least 50,000 vectors when the result must be complete, and measure HNSW recall on your own embeddings before relying on it.

**Allocation.** sqlite-vec allocates 2–7 KB of managed memory per query. DataVo allocates 1.2 MB per unfiltered query and up to 24 MB per filtered query at 1,536 dimensions. One cause is visible in the API: a `float[]` bound as a query parameter is turned into vector text (`DataVoQueryParameter.FromObject` → `FormatVectorString`) and parsed back for every execution. The rest is the candidate rows loaded for post-filtering. DataVo Flat's p99 is 1.4–6.2 times its median, against at most 1.2 times for sqlite-vec. Garbage collection is the likely cause, but I did not measure it.

## What this does not show

- One machine, one run per scenario, Apple Silicon only. The [results page](./benchmarks.md#part-iii-platforms) shows that DataVo's lead over SQLite on scans is 2 to 2.8 times smaller on x64; expect the ratios here to move as well.
- Data was in the operating system's page cache. A cold read from disk was not measured.
- Only read queries were timed. Load times are shown, but write-heavy workloads are covered on the [results page](./benchmarks.md), not here.
- SQLite and DuckDB are driven through ADO.NET, whose cost is part of their times; DataVo's own API cost is part of its time. Neither SQLite nor DuckDB was tuned beyond `ANALYZE`; DataVo had no columnar index except in the columnar section.
- The vector attributes are independent of the clusters. A filter correlated with distance (for example, a category that matches the query's cluster) changes how many candidates DataVo needs; that was not measured.
- `COUNT(DISTINCT ...)` is correct now but was not timed.
- DataVo is a 0.2 release candidate; SQLite and DuckDB are mature engines with general query optimisers. Most of DataVo's remaining losses here trace to one missing capability: index-driven joins and grouping, together with a typed path for joins beyond two tables.
