# DataVo vs SQLite: When Each Engine Wins

This page compares DataVo with SQLite on 28 relational query shapes over 1,000,000 orders and on five filtered nearest-neighbour queries, with DuckDB as an analytical reference. It records where each engine is faster and why, and it records five queries on which DataVo returned a wrong answer. Measured on 14 November 2026 on an Apple M4 Pro. The raw output of every run is in [`docs/public/benchmarks/2026-11-14/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-14).

**The short version.** DataVo is faster than SQLite on primary-key and secondary-index lookups, on full-table `GROUP BY` with up to about a thousand groups, on a two-table join with `GROUP BY`, on aggregates its columnar index can answer, and on single-table vector search. SQLite is faster, often by one to two orders of magnitude, on anything that leaves DataVo's typed execution paths: `OR`, `LIKE`, joins of three tables, `LEFT JOIN`, joins that return rows, and `GROUP BY` over a key that has an index. DataVo also returned wrong results for an anti-join, `COUNT(DISTINCT ...)`, some inner joins with a filter on the joined table, and both vector-plus-join queries. Read [Correctness findings](#correctness-findings) before choosing DataVo for a query that joins tables.

## Decision guide

Ratios compare medians, DataVo's LSM engine against SQLite (WAL, `synchronous=NORMAL`), one thread each, 1,000,000 orders unless stated.

| Your workload | Faster engine | By how much | Evidence |
| --- | --- | ---: | --- |
| Primary-key range read, 100 or 10,000 rows | DataVo LSM | 1.4–1.6× | [R1–R4](#primary-key-ranges) |
| Primary-key range read on the **InMemory** engine | SQLite | about 3,900× | [R1–R4](#primary-key-ranges): InMemory scans the whole table |
| Equality on an indexed column, about 1,000 rows | DataVo | 2.4× | [W1](#filters) |
| Equality on a column without an index | SQLite | 1.2× | [W2](#filters) |
| Any `OR` in `WHERE` | SQLite | 54–68× | [W3, W4, C6](#filters) |
| `LIKE`, prefix or substring | SQLite | 35–38× | [W5, W6](#filters) |
| `GROUP BY` over the whole table, 4 to 1,000 groups | DataVo | 2.1–2.6× | [G1, G2, C7](#group-by-cardinality) |
| `GROUP BY` over about 125,000 groups, on an indexed key, `HAVING` + top-100 | SQLite | 13× | [G3](#group-by-cardinality) |
| Two-table inner join + `GROUP BY` (fact × dimension) | DataVo | 2.9× | [J6](#joins) |
| Small dimension filtered, joined to a large table through an index | SQLite | 2.6× | [J7](#joins) |
| Three-table join | SQLite | 7.4× | [J1](#joins); J2/J3 also gave wrong answers in DataVo |
| `LEFT JOIN` | SQLite | 18× | [J4](#joins); J5 gave a wrong answer in DataVo |
| `COUNT`/`SUM`/`MIN`/`MAX` with an AND of numeric ranges, columnar index on those columns | DataVo | 4.8–12.8× | [C1, C2, C4](#columnar-index) |
| The same, without a columnar index | SQLite | 1.5–2.1× | [C1–C4](#columnar-index) |
| `SUM`/`AVG` of a `DOUBLE`/`FLOAT` column on the LSM engine, columnar index | SQLite | 2.0× | [C3](#columnar-index): the LSM engine does not push floating-point sums down |
| `ORDER BY ... LIMIT 10` over the whole table | SQLite | 1.7× | [C8](#columnar-index) |
| Top-10 nearest neighbours, one table, with or without a filter, exact | DataVo Flat | 1.6–5.4× | [V0–V2](#hybrid-vector-and-sql) |
| Top-10 nearest neighbours **joined to another table** | SQLite + sqlite-vec | DataVo's answer is wrong | [V3, V4](#hybrid-vector-and-sql) |
| Analytical scans, joins and aggregates that read the whole table | DuckDB | 3–2,600× over both | every table below; DuckDB is the slowest of the three on key ranges and index lookups |

Two rules follow from the reasons behind these numbers, explained in each section:

1. **DataVo is fast when a query stays on one of its typed paths:** one table with an AND of `column op value` comparisons; or exactly two tables in an inner integer-key join with an aggregate; or an index lookup. Anything else is evaluated row by row with a dictionary per row, at 2–4 KB of allocation per row read, and is many times slower than SQLite.
2. **SQLite wins wherever an index can replace a scan or a hash table,** because its planner uses indexes for joins, grouping and ordering. DataVo's planner does not use a secondary index to drive a join or a `GROUP BY`.

## Correctness findings

Each query was checked against an exact answer computed from the data generator before it was timed. SQLite and DuckDB returned that answer on every query and every parameter set. DataVo did not on the queries below, on both its LSM and InMemory engines. None of these has been fixed. The benchmark ran against the unmodified engine (commit `9625ac1`), so the timings above describe the engine as released.

| # | Query | DataVo returns | Correct | Where it goes wrong |
| --- | --- | --- | --- | --- |
| B1 | `SELECT COUNT(*) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id WHERE c.Id IS NULL` (orders without a customer) | 0 | 250,395 | The general join path evaluates each `WHERE` comparison against the rows of the table it names and then joins outward from them (`Parser/Statements/Mechanism/StatementEvaluator.cs`, `HandleNonIndexableStatement` → `GetJoinedTableContent`). For a column of the null-supplying side of an outer join this filters before the join instead of after it: no customer row has a NULL `Id`, so nothing survives. |
| B2 | `SELECT COUNT(DISTINCT CustomerId) FROM Orders` | 2,000 (the row count) | 175 | `DISTINCT` inside an aggregate's parentheses is not modelled; only `SELECT DISTINCT` is (`Parser.cs`, `ParseSingleSelectStatement`). The query runs as `COUNT(CustomerId)` without an error. Found by the pre-run probe on 2,000 rows; not in the timed matrix. |
| B3 | Top-10 by `d.Emb <=> @q` with `JOIN Authors a` and `WHERE d.Category = @c AND d.Price < @p` | 0–2 rows, recall@10 5% | 10 rows | When the statement uses table aliases, the hybrid planner cannot extract the documents' own filter on the join path (`ExpressionExtractor.TryExtractTableSpecificPredicates` called from `ResolveSeedPredicateForExpansion`), so it fetches only 10 candidates and filters them after the join. The same statement written with table names instead of aliases fetches 286 candidates and returns the right 10 rows. |
| B4 | `... FROM Orders o JOIN Customers c ON o.CustomerId = c.Id ... WHERE c.Region = @r ...` when some orders reference a customer that does not exist (J2, J3) | 0 rows | 1,827 rows (J2) | Triggered by an **equality** on a non-key column of the joined table together with at least one dangling foreign key. `c.Region <> 'x'`, `c.Id > 0` and filters on the orders themselves give correct results in the same data. The typed join path declines the query (a diagnostics probe shows rows being scanned by the general path), and the general path's equality branch (`HandleIndexableStatement` → `EvaluateUsingFullScan` → `Join.Evaluate` starting from `Customers`) produces no rows. Affects two- and three-table joins, aggregated or not. |
| B5 | Top-10 by `d.Emb <=> @q` with `JOIN Authors a` and `WHERE a.Country = @k` | 10 rows, of which 9 on average fail the filter | 10 rows from that country | `Select.TryEvaluateNearestNeighborUsingVectorIndex`: when `WHERE` references the joined table, the join is evaluated with only the part of the filter that names the embedding table, which is empty here. The `a.Country` predicate is dropped. |

**What to do until they are fixed.** Avoid `LEFT JOIN ... WHERE <right>.col IS NULL` anti-joins and `COUNT(DISTINCT ...)`. Make sure foreign keys reference existing rows before filtering a join on a dimension column, or filter with a subquery. Run vector top-k queries on one table and join the ten results in a second query.

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

- 25% of orders reference a customer id between 100,001 and 125,000, which has no row, so outer joins have unmatched rows. This is also what exposed B4.
- `ProductTag` always equals `ProductId` but has no index, so W1 and W2 differ only in the index.
- `Discount` is NULL in about 10% of rows. `Bucket` has 1,000 values; `Status` four upper-case values; `Sku` looks like `S0042-PAID`.
- Every `DOUBLE` is a multiple of 0.25 below 1,000, so every sum is exact in any order of addition, and answers are compared exactly, bit for bit, never with a tolerance.
- DataVo's `FLOAT` is a 32-bit type, so the DataVo schema uses `DOUBLE` to match SQLite's `REAL` and DuckDB's `DOUBLE`.

### Answer checks and timing

Before timing a query, the runner executes it with each of its parameter sets and compares a normalised digest of the rows (integers as integers whatever their CLR type, other numbers in round-trip form, rows sorted unless the SQL fixes the order) with the oracle's. A mismatch is reported and the cell is marked `✗`; it is not a result. Then come unmeasured warm-up calls and the timed calls: 10 for full-table queries, 100 for 10,000-row ranges, 1,000 for selective queries, cycling through the parameter sets, with a full garbage collection before the timed block. Medians and p99 come from those calls; with 10 calls the p99 is the slowest call. Managed allocation is `GC.GetAllocatedBytesForCurrentThread` over the timed calls; SQLite's and DuckDB's native memory is not visible to it.

### Machine

Apple M4 Pro, 12 cores, 48 GiB, macOS 26.3 (Darwin 25.3.0), .NET 10.0.3, Arm64 RyuJIT. Engine commit `9625ac1`; benchmark code `638324d`. One run per scenario, no CPU pinning, background processes not controlled. The SQL matrix took 20 minutes and peaked at 5.0 GB resident; the vector scenario took 33 seconds. As on the [results page](./benchmarks.md), treat differences under about 20% as not established.

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
| Load 1,101,000 rows (ms) | 3,660 | 559 | 4,935 (incl. `ANALYZE`) | 1,023 |
| Reopen (ms) | 500 | – | 2.2 | 239 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.0 |
| Create the columnar index (ms) | 318 | 186 | – | – |

### Joins

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| J1 Three-table inner join, `COUNT`, `SUM`, no filter | 5.36 s (5.71 s) | 5.38 s | 724 ms (725 ms) | 2.08 ms | 0.14× |
| J2 J1 + `WHERE c.Region = @r AND p.CategoryId = @cat` (~0.2%) | ✗ 2.35 s | ✗ 2.46 s | 84.6 ms | 1.50 ms | wrong answer |
| J3 J2 returning rows, `ORDER BY o.Amount DESC, o.Id LIMIT 50` | ✗ 2.17 s | ✗ 2.39 s | 84.7 ms | 1.98 ms | wrong answer |
| J4 `LEFT JOIN`: `COUNT(*)`, `COUNT(c.Id)`, `SUM` | 4.36 s (4.75 s) | 4.44 s | 238 ms | 3.44 ms | 0.05× |
| J5 `LEFT JOIN ... WHERE c.Id IS NULL` | ✗ 2.00 s | ✗ 2.01 s | 36.2 ms | 3.35 ms | wrong answer |
| J6 Orders × Customers, `GROUP BY c.Region` | 262 ms (360 ms) | 257 ms | 763 ms | 3.49 ms | **2.92×** |
| J7 Products (one category, 2%) × Orders, `COUNT`, `SUM` | 40.1 ms (40.5 ms) | 76.6 ms | 15.1 ms | 1.08 ms | 0.38× |

**Why.** DataVo has one fast join path (`Select.TypedJoin.cs`): exactly two tables, an inner join on one integer column, an aggregate, and per-table AND filters. It hashes the smaller table and streams the larger one, decoding only the columns it needs. J6 and J7 take it. J6 is DataVo's win: SQLite's plan (`SCAN c | SEARCH o USING INDEX ix_orders_customer | USE TEMP B-TREE FOR GROUP BY`) probes the order index 100,000 times and sorts into a temporary B-tree. In J7, SQLite's plan (`SCAN p | SEARCH o USING INDEX ix_orders_product`) reads only the 20,000 orders of the 20 matching products through the index, while DataVo's hash join streams all 1,000,000 orders.

Every other join leaves that path: three tables (J1–J3), no aggregate (J3), or `LEFT JOIN` (J4, J5). The general path materializes each joined row as dictionaries, about 3.9 KB of allocation per order in J1 (3.7 GB per call, 35 gen-2 collections over 10 calls). For each `WHERE` comparison it runs a complete join starting from the table that comparison names, then intersects the results (`StatementEvaluator.And`), so J2 joins the million orders twice. SQLite runs J1–J3 as index nested-loop joins (`SCAN c | SEARCH o USING INDEX ix_orders_customer | SEARCH p USING INTEGER PRIMARY KEY`).

### Filters

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| W1 `WHERE ProductId = @p` (indexed, ~1,000 rows) | 334 µs (1.09 ms) | 188 µs (452 µs) | 817 µs (923 µs) | 1.13 ms | **2.45×** |
| W2 `WHERE ProductTag = @p` (no index, ~1,000 rows) | 35.6 ms (39.6 ms) | 68.7 ms | 28.8 ms | 0.57 ms | 0.81× |
| W3 `(Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= @lo AND Amount < @hi` | 3.31 s (4.64 s) | 3.39 s | 61.1 ms | 2.09 ms | 0.02× |
| W4 W3 `AND Discount IS NOT NULL AND Discount < @d` | 4.32 s (5.00 s) | 4.21 s | 63.7 ms | 2.08 ms | 0.01× |
| W5 `Sku LIKE 'S0042%'` (no index) | 1.64 s (1.83 s) | 1.88 s | 42.7 ms | 2.82 ms | 0.03× |
| W6 `Sku LIKE '%042-%'` (no index) | 1.81 s (2.07 s) | 1.98 s | 51.8 ms | 4.11 ms | 0.03× |

**Why.** DataVo's typed single-table scan (`Select.TypedScan.cs`, `TryCollectSidePredicates`) accepts only an AND of `column op literal` comparisons with `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS NULL` and `IS NOT NULL`. It decodes only the filtered columns and builds a result row only for matches. An `OR` or a `LIKE` anywhere in `WHERE` sends the whole query to the general path, which builds a dictionary for each of the million rows (3.2–4.0 GB allocated per call in W3/W4, 1.8 GB in W5/W6). That is the 35–68× gap; SQLite evaluates these predicates on its row format without allocating.

On the typed path the gap is small. W2 reads every row on both engines (SQLite: `SCAN Orders`) and SQLite is 1.2 times faster. W1 uses the index on both, and DataVo is 2.4 times faster: its secondary index returns all matching row keys in one lookup (`IndexManager.FilterUsingIndex`), and the typed scan then reads those rows by key. I did not profile where SQLite spends its 817 µs. The W1 p99 of 1.09 ms comes from garbage collection: 555 KB allocated per call, 67 gen-0 and 24 gen-1 collections over 1,000 calls. SQLite allocates 49 KB per call.

### Primary-key ranges

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| R1 `Id BETWEEN @lo AND @hi`, 100 rows | 13.2 µs (18.5 µs) | 70.9 ms (74.1 ms) | 18.0 µs (19.3 µs) | 172 µs | **1.36×** |
| R2 `Id >= @lo AND Id < @hi`, 100 rows | 12.5 µs (15.9 µs) | 69.6 ms | 18.0 µs (23.9 µs) | 172 µs | **1.43×** |
| R3 `BETWEEN`, 10,000 rows | 1.11 ms (12.8 ms) | 70.1 ms | 1.70 ms (1.79 ms) | 590 µs | **1.54×** |
| R4 `>= AND <`, 10,000 rows | 1.10 ms (11.5 ms) | 70.8 ms | 1.71 ms (1.98 ms) | 595 µs | **1.55×** |

**Why.** On the LSM engine the typed scan turns a range on the clustered integer key into a seek (`KeyRangeOf` → `LsmEngine.ScanStoredRows`), the same access pattern as SQLite's `SEARCH Orders USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)`. It reads two columns of each row from the memory-mapped SSTable, and is 1.4 to 1.6 times faster at both widths. `BETWEEN` and `>= AND <` take the same path on every engine.

The 10,000-row p99 of DataVo (11.5–12.8 ms, against a 1.1 ms median) is garbage collection: 3.0 MB allocated per call, with 4 gen-2 collections over 100 calls. SQLite's p99 stays within 1.2 times of its median.

**The InMemory engine has no ordered access to its rows.** `StorageContext.ScanTableRows` hands the in-memory backend the key bounds, but it visits every row and drops those outside the range (`VisitTypedRowInRange`). A 100-row range therefore costs a pass over the million rows, about 70 ms. This is not a problem with the measurement: use the LSM engine, or a point lookup, for key ranges.

### GROUP BY cardinality

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| G1 `GROUP BY Status` (4 groups) | 90.5 ms (92.6 ms) | 96.8 ms | 237 ms | 1.10 ms | **2.62×** |
| G2 `GROUP BY Bucket` (1,000 groups) | 98.6 ms (105 ms) | 110 ms | 209 ms | 0.73 ms | **2.12×** |
| G3 `GROUP BY CustomerId HAVING COUNT(*) >= @k ORDER BY N DESC, CustomerId LIMIT 100` (~125,000 groups) | 391 ms (457 ms) | 418 ms | 29.6 ms | 4.36 ms | 0.08× |

| Memory during the timed calls | DataVo (LSM) | SQLite (managed only) |
| --- | ---: | ---: |
| G1, per call | 64.9 MB; 81 gen-0, 0 gen-2 over 10 calls | 720 B |
| G2, per call | 56.0 MB; 70 gen-0, 0 gen-2 | 70.7 KB |
| G3, per call | 260 MB; 327 gen-0, 146 gen-1, 16 gen-2 | 5.2 KB |

**Why.** DataVo's typed scan aggregates into a hash table while it streams the rows. SQLite sorts into a temporary B-tree (`SCAN Orders | USE TEMP B-TREE FOR GROUP BY`), and with few groups the hash table is 2.1 to 2.6 times faster. In G3 SQLite reads the orders already in `CustomerId` order from the covering index (`SCAN Orders USING COVERING INDEX ix_orders_customer`), so it groups as it goes, with no table of groups and no scan of the wider rows. DataVo does not use a secondary index to drive a `GROUP BY`. It builds 125,000 groups whose keys are boxed `object[]` arrays, allocating 260 MB per call, and is 13 times slower.

### Columnar index

DataVo's columnar index (`CREATE INDEX cx_orders ON Orders (Amount, Qty, Bucket, Discount, ProductTag) USING COLUMNAR`) was created after the queries above, and the queries were timed again. SQLite and DuckDB are shown once. "Pushed" means the index answered the query; the runner reads the index's scan counter around each query to tell.

| Query | DataVo (LSM) | DataVo (LSM) + index | DataVo (InMemory) + index | SQLite | DuckDB | SQLite / DataVo + index |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| C1 `COUNT(*) WHERE Amount >= @lo AND Amount < @hi` | 67.2 ms | 3.40 ms, pushed | 3.35 ms, pushed | 43.5 ms | 1.18 ms | **12.8×** |
| C2 `SUM(Qty) WHERE Bucket < @b` | 77.7 ms | 3.55 ms, pushed | 3.67 ms, pushed | 36.9 ms | 0.43 ms | **10.4×** |
| C3 `SUM(Amount) WHERE Qty >= @q` (DOUBLE sum) | 77.9 ms | 78.6 ms, row scan | 3.10 ms, pushed | 40.1 ms | 0.47 ms | 0.51× |
| C4 `COUNT(*), SUM(Qty), MIN(Amount), MAX(Amount) WHERE Bucket < @b AND Discount IS NOT NULL` | 103 ms | 12.1 ms, pushed | 11.9 ms, pushed | 58.2 ms | 1.42 ms | **4.80×** |
| C5 `COUNT(*) WHERE Status = @s AND Amount >= @lo` (string column) | 69.3 ms | 70.4 ms, row scan | 74.3 ms, row scan | 42.6 ms | 0.97 ms | 0.61× |
| C6 `COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= @lo` | 2.17 s | 2.56 s, row scan | 2.34 s, row scan | 35.9 ms | 1.61 ms | 0.01× |
| C7 `GROUP BY Bucket`: `COUNT(*)`, `SUM(Qty)` | 96.8 ms | 97.2 ms, row scan | 110 ms, row scan | 201 ms | 0.74 ms | 2.06× |
| C8 `ORDER BY Amount DESC, Id LIMIT 10` | 69.3 ms | 67.7 ms, row scan | 84.7 ms, row scan | 40.3 ms | 1.28 ms | 0.60× |

Pushed queries allocate 14–20 KB per call, against 4.6–25 MB per call for the same query as a row scan.

**Which SQL shapes the index answers** (`Select.Columnar.cs`): a single-table `COUNT`, `SUM`, `AVG`, `MIN` or `MAX` without `GROUP BY`, whose `WHERE` is an AND of comparisons, `IS NULL` or `IS NOT NULL` between an indexed numeric column and a value of a compatible type, and whose aggregated columns are all in the index. Not answered: anything with `GROUP BY` (C7), `ORDER BY` (C8), `OR` (C6), a column outside the index such as a string column (C5), or a statement inside an explicit transaction.

**Floating-point sums on the LSM engine (C3).** On the LSM engine the in-memory part of the columnar index is the tail of rows not yet flushed, and an index in that state reports itself as not in key order (`ColumnarIndex.InKeyOrder`). A `SUM` or `AVG` of a `FLOAT` or `DOUBLE` column is then left to the row scan, because floating-point addition in a different order can give different bits than the row scan's. This is deliberate: the index only answers when it can return exactly the row scan's result. On the InMemory engine the same query is pushed. Integer sums, counts, minimums and maximums are pushed on both.

**DuckDB is still faster.** Using all 12 cores and its own column storage, DuckDB answers C1 and C2 3 to 8 times faster than DataVo's columnar index. The [BenchmarkDotNet comparison](./benchmarks.md#with-a-columnar-index), where DataVo leads DuckDB, holds DuckDB to one thread and measures DataVo's compiled LINQ queries, not SQL text.

### Hybrid vector and SQL

`Docs (Id INT PRIMARY KEY, Category INT, Price DOUBLE, AuthorId INT, Emb VECTOR(384))`: 50,000 documents drawn around 128 random cluster centres and normalised. Ten categories, prices from 0 to 99.75, 2,000 authors in ten countries. The relational attributes are independent of the clusters. Each shape runs 200 held-out query vectors (not in the table) after 10 warm-up queries. Recall@10 compares the ten returned ids with the exact top-10 by cosine distance **among the rows that satisfy the `WHERE` clause**, computed by brute force in the runner.

DataVo runs `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE ... ORDER BY dist LIMIT 10`, prepared, on the LSM engine with a `FLAT` or `HNSW` index at default settings. SQLite uses sqlite-vec's documented filtered-KNN form: a `vec0(id INTEGER PRIMARY KEY, emb float[384] distance_metric=cosine, category INTEGER, price FLOAT, author_id INTEGER)` table whose filter columns are metadata columns, queried as `WHERE emb MATCH @q AND k = 10 AND category = @c AND price < @p`. The join shapes run that KNN in a `MATERIALIZED` CTE and join `Authors` to its ten rows. V4 constrains the KNN with `author_id IN (SELECT Id FROM Authors WHERE Country = @k)`, which sqlite-vec v0.1.9 accepts on a metadata column. sqlite-vec's `vec0` is an exact scan, with no approximate index.

| Shape | DataVo Flat | DataVo HNSW | SQLite + sqlite-vec | sqlite-vec / Flat |
| --- | --- | --- | --- | ---: |
| Load + index build | 390 ms | 2,047 ms | 640 ms | |
| V0 top-10, no filter | 3.46 ms (15.8 ms), recall 100% | 0.66 ms (1.02 ms), recall 81.5% (min 0%) | 18.7 ms (19.2 ms), recall 100% | **5.4×** |
| V1 `Category = @c AND Price < 50` (~5%) | 4.22 ms (10.1 ms), 100% | 7.13 ms (20.0 ms), 90.3% | 9.92 ms (10.5 ms), 100% | **2.4×** |
| V2 `Price < 50` (~50%) | 2.73 ms (5.37 ms), 100% | 1.32 ms (2.70 ms), 83.8% | 14.6 ms (15.2 ms), 100% | **5.4×** |
| V3 V1 + `JOIN Authors` | ✗ 2.06 ms, recall 5.3%, 200 of 200 queries short | ✗ 0.31 ms, recall 4.7%, 200 short | 9.95 ms (10.7 ms), 100% | wrong answer (B3) |
| V4 `JOIN Authors WHERE a.Country = @k` (~10%) | ✗ 2.89 ms, recall 9.5%, 1,809 rows that fail the filter | ✗ 1.13 ms, 8.7%, 1,804 failing rows | 13.3 ms (13.5 ms), 100% | wrong answer (B5) |

The same scenario with 20,000 documents of 1,536 dimensions (`hybrid-vector-1536.md`):

| Shape | DataVo Flat | DataVo HNSW | SQLite + sqlite-vec | sqlite-vec / Flat |
| --- | --- | --- | --- | ---: |
| Load + index build | 481 ms | 1,862 ms | 4,011 ms | |
| V0 | 6.06 ms (27.2 ms), 100% | 2.22 ms (2.58 ms), 86.8% | 32.7 ms (35.5 ms), 100% | **5.4×** |
| V1 (~5%) | 9.70 ms (20.8 ms), 100% | 11.9 ms (29.3 ms), 89.9%, 18 queries short | 16.1 ms (17.8 ms), 100% | **1.7×** |
| V2 (~50%) | 6.83 ms (12.0 ms), 100% | 3.64 ms (6.21 ms), 88.4% | 24.2 ms (25.4 ms), 100% | **3.5×** |
| V3 | ✗ recall 5.5%, 200 short | ✗ 5.2%, 200 short | 16.1 ms, 100% | wrong answer |
| V4 | ✗ 11.1%, 1,779 failing rows | ✗ 10.0%, 1,786 failing rows | 18.2 ms, 100% | wrong answer |

**DataVo's planner routes,** from its diagnostics (`RuntimeQueryStats` and the hybrid-routing counters), recorded in a separate untimed pass over the same 200 queries:

| Shape | Route | Candidates fetched from the vector index | Expansion passes (mean) | Full table scan |
| --- | --- | --- | ---: | ---: |
| V0 | vector index, baseline | 10 | 0 | 0 of 200 |
| V1 | vector index, adaptive initial top-k, filter applied to candidates | median 286, max 572 | 0.10 (Flat), 0.08 (HNSW) | 0 |
| V2 | vector index, adaptive | median 29, max 58 | 0.05 (Flat), 0.03 (HNSW) | 0 |
| V3 | vector index, **baseline** (filter not extracted, B3) | 10 | 0 | 84–86 of 200 |
| V4 | vector index, baseline; `Authors` scanned | 10 | 0 | 200 of 200 |

**Why DataVo Flat is faster than sqlite-vec on V0–V2.** Both compute exact distances. DataVo's Flat index runs its vectorised distance kernels (`SimdDistanceKernels`, NEON on this machine) and is 5.4 times faster than `vec0` without a filter. I did not profile sqlite-vec, so I cannot say how much of the gap is the kernel and how much the scan around it. With a filter the margin shrinks to 1.7–2.4 times, from both sides. sqlite-vec applies metadata filters inside its scan and computes no distance for a row that fails: its V1 is 1.9 times faster than its V0. DataVo instead asks the index for the top 286 to 572 candidates of the whole table, loads those rows, and filters them (6.8 MB allocated per query at 384 dimensions). When fewer than 10 candidates pass, it fetches twice as many, up to two more times. With Flat the candidates are exact, so the filtered top-10 is exact whenever enough candidates pass, which it was in all 200 queries.

**HNSW on this data.** HNSW is the fastest on V0 and V2, but its recall is 81.5–90.3% across the shapes and both sizes, with a minimum of 0%. In a separate probe (`EfSearch` 64, 128, 256, 512), 15 of the 200 queries had 0% recall at every `EfSearch`. Their results had a mean distance of 0.74 where the true top-10 had 0.15. The search ended in a different cluster, and widening the search did not bring it back. This data has well-separated clusters (cluster noise is 0.46 of the centre's length), which is hard for a graph index, and the [earlier recall experiment](./benchmarks.md#vector-search) measured 90–96% on clustered data from a different generator. With a selective filter (V1) HNSW is slower than Flat and at 1,536 dimensions returned fewer than 10 rows for 18 queries: the 572 approximate candidates were not enough and the expansions ran out. Use Flat up to at least 50,000 vectors when the result must be complete, and measure HNSW recall on your own embeddings before relying on it.

**Allocation.** sqlite-vec allocates 2–7 KB of managed memory per query. DataVo allocates 1.2 MB per unfiltered query and up to 24 MB per filtered query at 1,536 dimensions. One cause is visible in the API: a `float[]` bound as a query parameter is turned into vector text (`DataVoQueryParameter.FromObject` → `FormatVectorString`) and parsed back for every execution. The rest is the candidate rows loaded for post-filtering. DataVo Flat's p99 is 1.8–4.6 times its median, against at most 1.1 times for sqlite-vec. Garbage collection is the likely cause, but I did not measure it.

## What this does not show

- One machine, one run per scenario, Apple Silicon only. The [results page](./benchmarks.md#part-iii-platforms) shows that DataVo's lead over SQLite on scans is 2 to 2.8 times smaller on x64; expect the ratios here to move as well.
- Data was in the operating system's page cache. A cold read from disk was not measured.
- Only read queries were timed. Load times are shown, but write-heavy workloads are covered on the [results page](./benchmarks.md), not here.
- SQLite and DuckDB are driven through ADO.NET, whose cost is part of their times; DataVo's own API cost is part of its time. Neither SQLite nor DuckDB was tuned beyond `ANALYZE`; DataVo had no columnar index except in the columnar section.
- The vector attributes are independent of the clusters. A filter correlated with distance (for example, a category that matches the query's cluster) changes how many candidates DataVo needs; that was not measured.
- DataVo is a 0.2 release candidate; SQLite and DuckDB are mature engines with general query optimisers. Most of DataVo's losses here trace to two missing capabilities, index-driven joins and grouping, and typed evaluation of `OR` and `LIKE`, and to the five correctness defects above.
