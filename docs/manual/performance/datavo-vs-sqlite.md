# DataVo vs SQLite: When Each Engine Wins

This page compares DataVo with SQLite on 28 relational query shapes over 1,000,000 orders and on five filtered nearest-neighbour queries, with DuckDB as an analytical reference. For each shape it gives the measured time and the mechanism behind it, on both sides where I know it. Every number comes from the final v0.4 runs, 17 November 2026: the SQL matrix against engine commit `216205d`, and the vector scenarios against `5cbcd14`, which differs only in how the vector distance kernels are compiled (see note 1 under [Hybrid vector and SQL](#hybrid-vector-and-sql)). The raw output is in [`docs/public/benchmarks/2026-11-16-v04-final/`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-16-v04-final). Earlier runs are kept in sibling folders and are referred to where a number moved.

Before any query is timed, its answer from every engine is compared with an exact answer the benchmark computes from its own data generator. In the final runs every engine returned that answer on every relational query and parameter set, and no vector query returned too few rows or a row that fails its filter. The first run of this comparison, on 14 November, found five queries on which DataVo returned a wrong answer; [Correctness findings](#correctness-findings) describes them and their fixes.

**The short version.** DataVo is faster than SQLite on primary-key ranges (1.5–1.8×) and secondary-index lookups (2.4×), on full-table `GROUP BY` with up to about a thousand groups (2.8–3.0×), on inner and `LEFT` joins of two or three tables on integer keys (2.3–9.7×), on aggregates its columnar index can answer (5–14×), and on filtered vector search (Flat 3.0–7.0×, HNSW 3.6–71× over sqlite-vec's exact scan, at 384 dimensions). SQLite is faster on a `GROUP BY` over about 125,000 groups of an indexed key (2.4×), which it reads in index order; on full scans filtered by an unindexed equality, `OR` or `LIKE` (1.1–1.3×); on an unindexed `ORDER BY ... LIMIT 10` (1.4×); on a `LEFT JOIN` anti-join (1.2×); and on unindexed aggregates (1.5–2.1×) when DataVo has no columnar index. DuckDB, a parallel column store, is faster than both on every query that reads the whole table (3–8 times faster than DataVo's columnar index, and up to 340 times faster than SQLite), and slower than both on key lookups and ranges.

## Decision guide

Ratios compare medians, DataVo's LSM engine against SQLite (WAL, `synchronous=NORMAL`), one thread issuing queries, 1,000,000 orders unless stated.

| Your workload | Faster engine | By how much | Evidence |
| --- | --- | ---: | --- |
| Primary-key range read, 100 or 10,000 rows | DataVo LSM | 1.5–1.8× | [R1–R4](#primary-key-ranges) |
| Primary-key range read on the **InMemory** engine | SQLite | 1.1–1.2× | [R1–R4](#primary-key-ranges) |
| Equality on an indexed column, about 1,000 rows | DataVo | 2.4× | [W1](#filters) |
| Equality on a column without an index | SQLite | 1.2× | [W2](#filters) |
| `OR` or `LIKE` in `WHERE`, full scan, no index | SQLite | 1.1–1.3× | [W3–W6](#filters) |
| `COUNT`/`SUM`/`MIN`/`MAX` with numeric `AND`/`OR` ranges, columnar index on those columns | DataVo | 5.1–14.1× | [C1, C2, C4, C6](#columnar-index) |
| The same, without a columnar index | SQLite | 1.5–2.1× | [C1–C4, C6](#columnar-index) |
| `SUM`/`AVG` of a `DOUBLE`/`FLOAT` column on the LSM engine, columnar index | SQLite | 2.0× | [C3](#columnar-index): the LSM engine does not push floating-point sums down |
| `GROUP BY` over the whole table, 4 to 1,000 groups | DataVo | 2.8–3.0× | [G1, G2, C7](#group-by) |
| `GROUP BY` over about 125,000 groups of an indexed key, `HAVING` + top-100 | SQLite | 2.4× | [G3](#group-by) |
| `ORDER BY ... LIMIT 10` over the whole table, no index | SQLite | 1.4× | [C8](#columnar-index) |
| Three-table inner join, aggregate over all rows | DataVo | 9.7× | [J1](#joins) |
| Three-table inner join filtered on both dimensions, aggregate or `ORDER BY ... LIMIT 50` | DataVo | 3.9–4.2× | [J2, J3](#joins) |
| `LEFT JOIN` aggregate (`COUNT(*)`, `COUNT(c.Id)`, `SUM`) | DataVo | 3.2× | [J4](#joins) |
| `LEFT JOIN ... WHERE c.Id IS NULL` (anti-join) | SQLite | 1.2× | [J5](#joins) |
| Two-table inner join + `GROUP BY` (fact × dimension) | DataVo | 5.2× | [J6](#joins) |
| Small dimension filtered, joined to a large table through an index | DataVo | 2.3× | [J7](#joins) |
| Top-10 nearest neighbours with a filter, exact | DataVo Flat | 3.0–7.0× (384 dims), 1.4–5.6× (1,536) | [V1–V4](#hybrid-vector-and-sql) |
| Top-10 nearest neighbours, approximate (HNSW), with or without a filter | DataVo HNSW | 2.9–71× over sqlite-vec's exact scan; recall 100% on this data | [V0–V4](#hybrid-vector-and-sql) |
| Analytical scans, joins and aggregates over the whole table | DuckDB | 3–340× | every table below |

Three rules follow from the mechanisms explained in each section:

1. **DataVo is fast when a query stays on a typed path:** one table with a `WHERE` built from `AND`, `OR`, `NOT`, `LIKE` and column-versus-value comparisons; `INNER` and `LEFT` joins of any number of tables on integer keys, aggregated or returning rows; an index lookup or a key range; a columnar aggregate. Anything else (an `OR` across two joined tables, `RIGHT`/`FULL` joins, joins on text or `GUID` keys, `DISTINCT`, computed expressions, a read inside an explicit transaction) runs on the general evaluator, which allocates 2–4 KB per row read and is many times slower than SQLite. [Query Planner And Fast Paths](/manual/storage-engine/query-planner-fast-paths) lists the paths.
2. **On a full scan, SQLite's row decoder is still a little faster than DataVo's.** Where both engines must touch every row and neither has a structure that avoids work, SQLite wins by 1.1–2.1×. DataVo wins full scans only when it does less work: hashing instead of sorting for `GROUP BY`, or reading a columnar index instead of rows.
3. **SQLite wins wherever an index can replace a hash table or a scan that DataVo's planner does not replace,** because its planner uses indexes for grouping and per-row probes. DataVo uses its primary key for ranges, a secondary index for equality and to drive a selective inner join, but not to drive a `GROUP BY`.

## Correctness findings

The first run (14 November, against the unmodified engine `9625ac1`) found five queries on which DataVo, on both its LSM and InMemory engines, returned a wrong answer. Each fix has regression tests on both engines.

| # | Query | Before the fix | Correct | Cause | Fix |
| --- | --- | --- | --- | --- | --- |
| B1 | `SELECT COUNT(*) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id WHERE c.Id IS NULL` (J5) | 0 | 250,395 | The general join path filtered the table each `WHERE` comparison names and joined outward from the survivors. On the NULL-supplying side of an outer join that filter runs before the NULL padding exists. Starting from that table also flipped which side the join kept, so `LEFT JOIN ... WHERE c.Region = 'R1'` returned customers without orders. | `bfb1390`: a comparison filters before the join only when that is equivalent (all joins inner, or a comparison on the FROM table with joins that keep it). Otherwise the full join is built once and its rows are filtered. |
| B2 | `SELECT COUNT(DISTINCT CustomerId) FROM Orders` | 2,000 (the row count) | 175 | `DISTINCT` inside an aggregate's parentheses was dropped by the parser. Found by a probe on 2,000 rows; not in the timed matrix. | `6743521`: `COUNT/SUM/AVG/MIN/MAX(DISTINCT x)` are parsed and computed on the general path; the fast paths decline them, and reactive subscriptions reject them with an error. |
| B3 | Top-10 by `d.Emb <=> @q` with `JOIN Authors a` and `WHERE d.Category = @c AND d.Price < @p` (V3) | 0–2 rows, recall@10 5% | 10 rows | The planner extracts the documents' own filter to size the candidate fetch. It compared the column qualifier `d` with the table name `Docs`, so with aliases nothing was extracted and only 10 candidates were fetched. | `8ce4295`: qualifiers are resolved through the table service. |
| B4 | `... JOIN Customers c ... JOIN Products p ... WHERE c.Region = @r AND p.CategoryId = @cat` (J2, J3) | 0 rows | 1,827 rows (J2) | Each comparison was evaluated from its own table and the partial results intersected on joined row keys. Those keys listed per-table row ids in an order that depended on the starting table, so `AND` across two joined tables matched almost nothing and `OR` gave wrong counts. | `fd81cf8`: keys are rewritten into one canonical table order after every join. |
| B5 | Top-10 by `d.Emb <=> @q` with `JOIN Authors a` and `WHERE a.Country = @k` (V4) | 10 rows, 9 of them failing the filter on average | 10 rows from that country | The vector join path applied only the part of `WHERE` that names the vector table; `a.Country` was dropped. An inner join also returned fewer than 10 rows when a candidate had no join partner. | `8ce4295`: candidates are pre-filtered, joined and filtered by the whole `WHERE`, growing the candidate count with the observed survival rate. |

Further defects fixed in the v0.4 closeout are not in the timed matrix. Two concern the queries on this page; the [Roadmap](/manual/preface/roadmap) lists the others (`BIT` filters, and Disk index lookups under comma-decimal cultures):

- **Short filtered vector results** (`50afc36`). When every allowed candidate expansion still found fewer passing rows than the `LIMIT`, the query returned what it had although more rows matched: with 10 matching rows among 1,500, 9 came back. It now finds the passing rows with a table scan and ranks only those by the index's stored vectors, which is exact.
- **`WHERE ... HAVING` without `GROUP BY`** (`09b958c`) failed with "Invalid expression tree node type": the parser ended the `WHERE` condition at `GROUP BY`, `ORDER BY`, `LIMIT` and other keywords but not at `HAVING`, and parsed `HAVING` as part of `WHERE`.

A correction to the first report of B4: it was first described as an equality filter combined with dangling foreign keys. That was wrong; the probe that suggested it happened to have data in which 0 was the right answer. The trigger is an `AND` or `OR` across two different joined tables.

## How the comparison was made

### Engines and settings

| Engine | Configuration |
| --- | --- |
| DataVo (LSM) | `StorageMode.Lsm`, `LsmStrictFsync = false` (Relaxed: the commit log is not flushed per commit). Loaded with `InsertTypedBatch`, then closed and reopened so every query reads from SSTables (the page cache is warm). |
| DataVo (InMemory) | `StorageMode.InMemory`, loaded with `InsertTypedBatch`. |
| SQLite | `Microsoft.Data.Sqlite` 10.0.0 with the bundled SQLite 3.50.4. File database, `journal_mode=WAL`, `synchronous=NORMAL`, no connection pooling. Loaded in one transaction, then `ANALYZE`, then reopened. |
| DuckDB | `DuckDB.NET.Data.Full` 1.5.3, file database, default thread count (12). Loaded with the appender, then reopened. A column store, shown as the analytical reference, not as a like-for-like competitor. |
| SQLite + sqlite-vec | `sqlite-vec` v0.1.9 (the `vec0.dylib` in the repository root), loaded into the same SQLite build, WAL, `synchronous=NORMAL`. |

Neither DataVo Relaxed nor SQLite `NORMAL` makes a commit durable against power loss; they are the comparable pair. Every query is a statement prepared once per engine and executed with bound parameters. Every engine reads every cell of every result row, so materializing results is part of every time. One thread issues the queries. DuckDB may use its own threads inside a query; DataVo and SQLite do not.

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
- Every `DOUBLE` is a multiple of 0.25 below 1,000, so every sum is exact in any order of addition, and answers are compared bit for bit, never with a tolerance.
- DataVo's `FLOAT` is a 32-bit type, so the DataVo schema uses `DOUBLE` to match SQLite's `REAL` and DuckDB's `DOUBLE`.

### Answer checks and timing

Before timing a query, the runner executes it with each of its parameter sets and compares a normalized digest of the rows (integers as integers whatever their CLR type, other numbers in round-trip form, rows sorted unless the SQL fixes the order) with the oracle's. A mismatch marks the cell `✗`; it is not a result. Then come unmeasured warm-up calls and the timed calls: 10 for full-table queries, 100 for 10,000-row ranges, 1,000 for selective queries, cycling through the parameter sets, after a full garbage collection. With 10 calls the p99 is the slowest call. Managed allocation is `GC.GetAllocatedBytesForCurrentThread` over the timed calls; SQLite's and DuckDB's native memory is not visible to it.

### Machine and run

Apple M4 Pro, 12 cores, 48 GiB, macOS 26.3 (Darwin 25.3.0), .NET 10.0.3, Arm64 RyuJIT. Engine `216205d` for the SQL matrix and `5cbcd14` for the vector scenarios, benchmark code `563c400`. One run per scenario, one process each, no CPU pinning, no build or test process running. Spotlight indexing kept one core busy during the runs; each started when the 1-minute load average was below 2.5 (`load.txt`). The SQL matrix took 115 seconds at a 1.1 GB peak footprint; the vector scenarios took 30 and 49 seconds at 0.8–1.0 GB. As on the [results page](./benchmarks.md), treat differences under about 20% as not established.

### Reproducing

```bash
dotnet build demos/Research.Benchmark/src/Research.Benchmark.Host -c Release
DLL=demos/Research.Benchmark/src/Research.Benchmark.Host/bin/Release/net10.0/Research.Benchmark.Host.dll

dotnet "$DLL" --scenario sql-matrix > sql-matrix.md 2> sql-matrix.log    # 1,000,000 orders, about 2 minutes
SQLITE_VEC_PATH=$PWD/vec0.dylib dotnet "$DLL" --scenario hybrid-vector > hybrid-vector.md 2> hybrid-vector.log
SQLITE_VEC_PATH=$PWD/vec0.dylib dotnet "$DLL" --scenario hybrid-vector --vectors 20000 --dimensions 1536 \
  > hybrid-vector-1536.md 2> hybrid-vector-1536.log
```

Options: `--records`, `--customers`, `--products`, `--engine datavo,datavo-mem,sqlite,duckdb`, `--queries J5,C8` (a subset of shapes) for `sql-matrix`; `--vectors`, `--dimensions`, `--queries`, `--engine datavo-flat,datavo-hnsw,sqlite` for `hybrid-vector`.

## Results

Median latency of one call, p99 in brackets. The last column is SQLite's median divided by DataVo LSM's: above 1, DataVo is faster.

### Setup

| | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB |
| --- | ---: | ---: | ---: | ---: |
| Load 1,101,000 rows (ms) | 3,498 | 525 | 4,595 (incl. `ANALYZE`) | 999 |
| Reopen (ms) | 468 | – | 2.4 | 231 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.0 |
| Create the columnar index (ms) | 317 | 180 | – | – |

SQLite reopens by reading a file header and its schema page. I have not profiled where DataVo's 468 ms reopen goes; it is a one-time cost per process, not part of any query time below.

### Joins

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| J1 Three-table inner join, `COUNT`, `SUM`, no filter | 70.7 ms (77.4 ms) | 88.7 ms | 682 ms (690 ms) | 2.00 ms | **9.65×** |
| J2 J1 + `WHERE c.Region = @r AND p.CategoryId = @cat` (~0.2%) | 20.4 ms (23.7 ms) | 10.6 ms | 80.5 ms | 1.49 ms | **3.94×** |
| J3 J2 returning rows, `ORDER BY o.Amount DESC, o.Id LIMIT 50` | 19.2 ms (28.9 ms) | 11.7 ms | 80.6 ms | 1.87 ms | **4.20×** |
| J4 `LEFT JOIN`: `COUNT(*)`, `COUNT(c.Id)`, `SUM` | 70.7 ms (73.4 ms) | 90.9 ms | 226 ms | 3.35 ms | **3.20×** |
| J5 `LEFT JOIN ... WHERE c.Id IS NULL` | 42.3 ms (42.7 ms) | 79.3 ms | 36.0 ms | 3.10 ms | 0.85× |
| J6 Orders × Customers, `GROUP BY c.Region` | 139 ms (158 ms) | 190 ms | 727 ms | 3.30 ms | **5.22×** |
| J7 Products (one category, 2%) × Orders, `COUNT`, `SUM` | 6.45 ms (8.64 ms) | 5.95 ms | 15.0 ms | 1.04 ms | **2.33×** |

| Allocated per call, DataVo LSM | 15 November (`269f226`) | 16 November (`b952574`) | Final (`216205d`) |
| --- | ---: | ---: | ---: |
| J1 | 6.2 MB | 3.0 MB | 39.7 KB |
| J2 / J3 | 9.9 / 16.6 MB | 5.0 / 11.6 MB | 4.1 / 11.5 MB |
| J4, J5 | 2.9 MB each | 2.9 MB each | 33.7 KB, 30.5 KB |
| J6 | 47.3 MB | 24.4 MB | 24.4 MB |
| J7 | 6.2 MB | 30.2 KB | 28.5 KB |

**How the typed join works.** All seven joins run on DataVo's typed join (`Select.TypedJoin.cs`). One table is streamed: the largest when every join is INNER, the FROM table when a `LEFT JOIN` must keep its rows. Every other table is hashed on its join key, keeping only the columns read after the join. Each streamed row is extended table by table; a `LEFT` step pads an unmatched row with NULLs. A condition on an INNER-joined table filters it while it is hashed; a condition on a `LEFT`-joined table is tested after the join, which is SQL's meaning (finding B1). When every join is INNER and a hashed table has few keys, the streamed table may instead be read through a single-column index on its join column, when a 32-key sample estimates that the index names at most an eighth of its rows (`Select.TypedJoinIndex.cs`). J7 reads the 20,000 orders of the 20 matching products that way, as SQLite does (`SCAN p | SEARCH o USING INDEX ix_orders_product`); J2 and J3 likewise.

**Why DataVo wins J1, J4 and J6 by 3–10×.** SQLite runs them as index nested-loop joins (`SCAN c | SEARCH o USING INDEX ix_orders_customer | SEARCH p USING INTEGER PRIMARY KEY`, and for J6 a temporary B-tree for `GROUP BY`): one B-tree descent per joined row, a million times. DataVo probes an in-memory structure per row instead.

**Key-only tables as bit sets (`7967a45`).** In J1, J4 and J5 the customers table contributes nothing but its key: J5 asks only whether a matching customer exists, J4 counts matches, J1 needs the match to exist. Until the final run DataVo hashed those 100,000 keys into a `Dictionary<long,int>` of counts (2.9 MB) and paid a hash lookup per order. Now the keys are gathered in a pooled buffer while the table is scanned. If they are distinct and span at most 64 values per key, which a dense integer primary key does, they become one bit per value of their range (`DenseInt64Set`): 12.5 KB for 100,000 consecutive keys, and a probe is a bounds check and a bit test. Repeated or scattered keys fall back to the hash with counts, so multiplicity is preserved. J5 went from 56.4 to 42.3 ms and from 2.9 MB to 31 KB per call; J1 from 84.5 to 70.7 ms; J4 from 83.5 to 70.7 ms.

**Why SQLite still wins J5.** SQLite scans the orders and probes the customers' integer primary key for each (`SCAN o | SEARCH c USING INTEGER PRIMARY KEY`); it does that at 36 ns per order. DataVo's probe is now a bit test, cheaper than a B-tree descent, but it pays for decoding each order's `CustomerId` from its row and for building the bit set from the 100,000 customers on every call: 42 ns per order in total. The remaining gap is the per-row decode cost that also appears in the filters below.

**Where J2 and J3 still allocate.** Hashing the filtered customers decodes their region and name strings from row bytes (4.1 and 11.5 MB on LSM, 1.0 and 4.7 MB on InMemory). J6 allocates 24 MB for the same reason: each customer's region is decoded to a string to become a group key. The UTF-8 matching described under [Filters](#filters) covers filters, not group keys or hashed columns.

**Joins that return rows.** Before `65d6434`, a join returning rows without an `ORDER BY ... LIMIT` ran on the general join path, which builds a dictionary per row of every table. Those shapes are not in the matrix; [`join-rows.txt`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-15-left-join/join-rows.txt) times four of them (for example, 93,684 joined rows: 4.59 s and 3.36 GB before, 457 ms and 303 MB on the typed join). What remains is building the returned rows, a dictionary each, which the API returns.

### Filters

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| W1 `WHERE ProductId = @p` (indexed, ~1,000 rows) | 334 µs (622 µs) | 174 µs (427 µs) | 797 µs (913 µs) | 1.07 ms | **2.39×** |
| W2 `WHERE ProductTag = @p` (no index, ~1,000 rows) | 34.3 ms (37.0 ms) | 67.5 ms | 27.9 ms | 0.51 ms | 0.81× |
| W3 `(Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= @lo AND Amount < @hi` | 67.0 ms (67.7 ms) | 102 ms | 59.3 ms | 2.09 ms | 0.88× |
| W4 W3 `AND Discount IS NOT NULL AND Discount < @d` | 77.0 ms (77.8 ms) | 105 ms | 61.9 ms | 2.14 ms | 0.80× |
| W5 `Sku LIKE 'S0042%'` (no index) | 51.1 ms (51.6 ms) | 66.6 ms | 41.4 ms | 2.91 ms | 0.81× |
| W6 `Sku LIKE '%042-%'` (no index) | 65.3 ms (65.8 ms) | 76.0 ms | 50.5 ms | 4.19 ms | 0.77× |

| DataVo LSM, per call | Before `9ffb413` (14 Nov) | 16 November | Final |
| --- | ---: | ---: | ---: |
| W3 | 3.40 s, 3.15 GB | 94.4 ms, 37.6 MB | 67.0 ms, 22 KB |
| W4 | 4.35 s, 4.01 GB | 104.9 ms, 35.5 MB | 77.0 ms, 23 KB |
| W5 | 1.66 s, 1.85 GB | 72.1 ms, 45.8 MB | 51.1 ms, 16 KB |
| W6 | 1.66 s, 1.86 GB | 76.4 ms, 45.8 MB | 65.3 ms, 16 KB |

**How the typed scan evaluates these.** DataVo's typed single-table scan (`Select.TypedScan.cs`) takes a `WHERE` built from `AND`, `OR`, `LIKE`/`NOT LIKE` and column-versus-value comparisons, decodes only the filtered columns, and builds a result row only for matches. An `OR` becomes one predicate whose branches are AND lists. The parser pushes `NOT` down into the comparisons, and `WHERE` keeps only rows that are true, so treating a comparison with a NULL as false gives SQL's three-valued result. Before 15 November an `OR` or a `LIKE` sent the whole query to the general path, a dictionary per row; the first column of the table above shows what that cost.

**Text compared as UTF-8 bytes (`e3caf04`).** On the LSM engine rows are stored serialized, and until the final run the scan decoded a .NET string from the row bytes for every text column a condition named, on every row, compared it, and dropped it: one string per row, 36–46 MB per query. Now the row walk records where the column's bytes lie, and `=`, `<>`, `LIKE`, `NOT LIKE` and `IS [NOT] NULL` compare the stored UTF-8 directly. This is exact, not approximate: two valid UTF-8 strings are ordinally equal exactly when their bytes are equal, and a `LIKE` pattern's literal runs, encoded to UTF-8, can only match at character boundaries. The one case bytes cannot decide is `_` against non-ASCII text, since `_` matches one UTF-16 character, which may be two to four bytes; that value is decoded and matched the old way. A test runs 40,000 random patterns and requires the byte matcher to agree with the character matcher whenever it answers. Allocation fell from 36–46 MB to 16–23 KB and time by 15–29%.

**Why SQLite is still 1.1–1.3 times faster.** With allocation gone, what remains is the cost of walking a million serialized rows: locating the filtered columns in each row and evaluating the predicate tree. SQLite's record format puts a header of column types and sizes at the start of each row, and its virtual machine reads the needed columns with little per-row overhead. I have not profiled where DataVo's remaining 20–30 ns per row goes. The InMemory engine holds rows as decoded cells and is *slower* than LSM here (102 against 67 ms on W3), which suggests the cost lies in the row visit and predicate evaluation rather than decoding.

**Indexed lookups (W1).** DataVo is 2.4 times faster: its secondary index returns all matching row keys in one lookup and the typed scan reads those rows by key. The W1 p99 of 622 µs is garbage collection: 329 KB allocated per call (the 1,000 result rows), 40 gen-0 and 14 gen-1 collections over 1,000 calls.

### Primary-key ranges

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| R1 `Id BETWEEN @lo AND @hi`, 100 rows | 12.4 µs (20.3 µs) | 21.8 µs (32.5 µs) | 18.2 µs (20.9 µs) | 180 µs | **1.47×** |
| R2 `Id >= @lo AND Id < @hi`, 100 rows | 11.9 µs (19.4 µs) | 21.5 µs (28.6 µs) | 18.3 µs (24.3 µs) | 181 µs | **1.54×** |
| R3 `BETWEEN`, 10,000 rows | 987 µs (11.6 ms) | 1.97 ms (14.4 ms) | 1.73 ms (2.29 ms) | 556 µs | **1.75×** |
| R4 `>= AND <`, 10,000 rows | 1.01 ms (11.4 ms) | 1.86 ms (13.4 ms) | 1.75 ms (1.90 ms) | 566 µs | **1.73×** |

**LSM: a seek and a sequential read.** The orders table is clustered on its integer key, so rows are stored in key order in the SSTables. The typed scan turns a range on that key into a seek (`KeyRangeOf` → `LsmEngine.ScanStoredRows`) and reads two columns of each row from the memory-mapped files: the same access pattern as SQLite's `SEARCH Orders USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)`, and 1.5 to 1.8 times faster.

**InMemory: a sorted copy of a hash map's keys.** The InMemory engine's integer primary key is a hash map from key to row slot, which has no order. The first range query builds a sorted copy of its keys (8 bytes per key, about 100 ms for a million) and later inserts are logged and merged; a removed key is skipped by looking it up. A range then binary-searches the copy and reads the rows by key. That costs two random accesses per row where LSM reads sequentially, so InMemory is 1.1–1.2 times slower than SQLite here. A range covering more than a quarter of the table is scanned instead.

**The 10,000-row p99** (11–14 ms against a 1–2 ms median) is garbage collection: 3.0–3.4 MB of result rows per call and 2–4 gen-2 collections over 100 calls. SQLite's p99 stays within 1.3 times its median.

### GROUP BY

| Query | DataVo (LSM) | DataVo (InMemory) | SQLite | DuckDB | SQLite / DataVo |
| --- | ---: | ---: | ---: | ---: | ---: |
| G1 `GROUP BY Status` (4 groups) | 83.2 ms (83.7 ms) | 95.3 ms | 232 ms | 1.12 ms | **2.79×** |
| G2 `GROUP BY Bucket` (1,000 groups) | 68.1 ms (68.9 ms) | 82.6 ms | 205 ms | 0.72 ms | **3.01×** |
| G3 `GROUP BY CustomerId HAVING COUNT(*) >= @k ORDER BY N DESC, CustomerId LIMIT 100` (~125,000 groups) | 69.1 ms (73.5 ms) | 102 ms | 29.2 ms | 4.32 ms | 0.42× |

| Allocated per call, DataVo LSM | 14 November | Final | SQLite (managed only) |
| --- | ---: | ---: | ---: |
| G1 | 64.9 MB | 34.4 MB; 43 gen-0 over 10 calls | 720 B |
| G2 | 56.0 MB | 2.1 MB | 70.7 KB |
| G3 | 260 MB; 17 gen-2 | 15.9 MB; 0 gen-2 | 5.2 KB |

**Hashing beats sorting when groups are few.** DataVo aggregates into a hash table while it streams the rows. A one-column key is looked up by its value without boxing, and each aggregate lives in an array indexed by group number. SQLite sorts the rows into a temporary B-tree (`SCAN Orders | USE TEMP B-TREE FOR GROUP BY`), which costs `O(n log g)` comparisons and B-tree page work, and is 2.8–3.0 times slower with 4 to 1,000 groups.

**G3: why SQLite's covering index wins, and why this is a trade-off rather than a bug.** SQLite reads the orders from its index on `CustomerId` (`SCAN Orders USING COVERING INDEX ix_orders_customer`). The index stores the keys in order and holds every column the query reads, so SQLite never touches the table rows, and because equal keys are adjacent it can finish each group as soon as the key changes: no table of 125,000 groups, 5 KB of allocation. DataVo does not use a secondary index to drive a `GROUP BY`. It scans the wider table rows and maintains 125,000 hash groups (16 MB), then applies `HAVING`, `ORDER BY` and `LIMIT` to the groups before materializing the 100 it returns. It is 2.4 times slower. The general fix is index-ordered streaming aggregation: read a single-column integer index in key order and aggregate runs of equal keys. DataVo's LSM index tables are already stored in key order, so the access path exists, but the planner does not choose it. Until it does, a high-cardinality grouping over an indexed key is a case where SQLite is the better engine. Before `534edbc` DataVo was 13 times slower on G3 (391 ms, 260 MB): each row boxed an `object[]` key and each group became a row with two dictionaries before `LIMIT` kept 100.

**G1's 34 MB.** On LSM each row's `Status` is decoded to a string to be the group key. The InMemory engine, which holds the strings, allocates 24 KB. Grouping by stored UTF-8 bytes would remove this, as it did for filters.

### Columnar index

DataVo's columnar index (`CREATE INDEX cx_orders ON Orders (Amount, Qty, Bucket, Discount, ProductTag) USING COLUMNAR`) was created after the queries above, and the queries were timed again. "Pushed" means the index answered; the runner reads the index's scan counter around each query to tell.

| Query | DataVo (LSM) | DataVo (LSM) + index | DataVo (InMemory) + index | SQLite | DuckDB | SQLite / DataVo + index |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| C1 `COUNT(*) WHERE Amount >= @lo AND Amount < @hi` | 66.0 ms | 3.05 ms, pushed | 3.22 ms, pushed | 43.1 ms | 1.14 ms | **14.1×** |
| C2 `SUM(Qty) WHERE Bucket < @b` | 76.0 ms | 3.32 ms, pushed | 3.55 ms, pushed | 35.6 ms | 0.43 ms | **10.7×** |
| C3 `SUM(Amount) WHERE Qty >= @q` (DOUBLE sum) | 74.2 ms | 75.5 ms, row scan | 2.91 ms, pushed | 37.9 ms | 0.48 ms | 0.50× |
| C4 `COUNT(*), SUM(Qty), MIN(Amount), MAX(Amount) WHERE Bucket < @b AND Discount IS NOT NULL` | 93.6 ms | 11.0 ms, pushed | 11.6 ms, pushed | 55.8 ms | 1.40 ms | **5.08×** |
| C5 `COUNT(*) WHERE Status = @s AND Amount >= @lo` (string column) | 45.1 ms | 45.3 ms, row scan | 73.5 ms, row scan | 41.2 ms | 0.92 ms | 0.91× |
| C6 `COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= @lo` | 70.7 ms | 4.62 ms, pushed | 0.47 ms, pushed | 34.7 ms | 1.57 ms | **7.51×** |
| C7 `GROUP BY Bucket`: `COUNT(*)`, `SUM(Qty)` | 67.7 ms | 68.0 ms, row scan | 82.6 ms, row scan | 194 ms | 0.67 ms | **2.85×** |
| C8 `ORDER BY Amount DESC, Id LIMIT 10` | 56.6 ms | 56.8 ms, row scan | 77.8 ms, row scan | 39.8 ms | 1.26 ms | 0.70× |

Every one of these queries now allocates 16–48 KB per call on DataVo LSM, with or without the index (16 November: 4.6–84 MB without it).

**What the index answers** (`Select.Columnar.cs`): a single-table `COUNT`, `SUM`, `AVG`, `MIN` or `MAX` without `GROUP BY` or `DISTINCT`, whose `WHERE` is `AND` and `OR` over comparisons, `IS NULL` or `IS NOT NULL` between an indexed numeric column and a value, and whose aggregated columns are all in the index. Not answered: `GROUP BY` (C7), `ORDER BY` (C8), `LIKE`, a column outside the index such as a string (C5), or a statement inside an explicit transaction. On LSM, flushed rows are read from columnar SSTable segments in 4,096-row chunks, and a chunk whose min/max zone map excludes the predicate is skipped without being read.

**Floating-point sums on LSM (C3) are deliberately not pushed.** On LSM the in-memory part of the columnar index is the unflushed tail, and an index in that state reports itself as not in key order (`ColumnarIndex.InKeyOrder`). Floating-point addition is not associative: summing the same values in segment order and then the tail can give different low-order bits than the row scan's key order. The index answers only when it can return exactly the row scan's bits, so a `SUM` or `AVG` of `FLOAT` or `DOUBLE` on LSM goes to the row scan and SQLite is twice as fast. On InMemory, where the index is in key order, the same query is pushed (2.91 ms). Integer sums, counts, minimums and maximums are exact in any order and are pushed on both. (This data uses multiples of 0.25, which sum exactly in any order; real data generally does not, which is why the rule exists.)

**Top-k without an index (C8, `216205d`).** `ORDER BY Amount DESC, Id LIMIT 10` keeps the 10 best rows in a bounded list. Until the final run every scanned row first boxed its two sort keys into a new `object[]` to compare it with the kept rows: 84 B per row, 84 MB per call. Now a row's sort-key cells are compared with the kept rows in place (NULL first, numbers by value, a `FLOAT` column at single precision, anything else boxed and compared as before, so the order is the general path's), and keys are copied only for a row that enters the list. C8 went from 66.2 ms and 84.0 MB to 56.6 ms and 48 KB; `MIN`/`MAX` on the row path (C4 without the index) from 24.8 MB to 24 KB. SQLite is still 1.4 times faster, for the full-scan reason given under [Filters](#filters).

**Aggregates without the index.** `29277aa` stopped an aggregate without `GROUP BY` from building an empty key array and looking up its single group for every matching row; C1–C3 went from 4.6–9.2 MB to 16 KB per call. Their time did not change materially: SQLite remains 1.5–2.1 times faster on unindexed aggregates.

**DuckDB is still faster than the columnar index.** Using all 12 cores and its own column storage, DuckDB answers C1 and C2 3–8 times faster than DataVo's index. The [BenchmarkDotNet comparison](./benchmarks.md#with-a-columnar-index), where DataVo leads DuckDB, holds DuckDB to one thread and measures DataVo's compiled LINQ, not SQL text.

### Hybrid vector and SQL

`Docs (Id INT PRIMARY KEY, Category INT, Price DOUBLE, AuthorId INT, Emb VECTOR(384))`: 50,000 documents drawn around 128 random cluster centres and normalized; ten categories, prices from 0 to 99.75, 2,000 authors in ten countries. The relational attributes are independent of the clusters. Each shape runs 200 held-out query vectors after 10 warm-up queries. Recall@10 compares the returned ids with the exact top-10 by cosine distance **among the rows that satisfy the `WHERE` clause**, computed by brute force in the runner. "Short" counts queries that returned fewer than 10 rows; "failing" counts returned rows that do not satisfy the `WHERE`.

DataVo runs `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE ... ORDER BY dist LIMIT 10`, prepared, on the LSM engine with a `FLAT` or `HNSW` index at default settings. SQLite uses sqlite-vec's documented filtered-KNN form: a `vec0` table whose filter columns are metadata columns, queried as `WHERE emb MATCH @q AND k = 10 AND category = @c AND price < @p`. The join shapes run that KNN in a `MATERIALIZED` CTE and join `Authors` to its ten rows; V4 constrains the KNN with `author_id IN (SELECT Id FROM Authors WHERE Country = @k)`. sqlite-vec's `vec0` is an exact scan; it has no approximate index.

| Shape (384 dimensions, 50,000 rows) | DataVo Flat | DataVo HNSW | SQLite + sqlite-vec | sqlite-vec / Flat | sqlite-vec / HNSW |
| --- | --- | --- | --- | ---: | ---: |
| Load + index build | 442 ms | 1,628 ms | 653 ms | | |
| V0 top-10, no filter | 2.19 ms (3.96 ms)¹ | 0.26 ms (0.54 ms) | 18.2 ms (18.5 ms) | **8.3×** | **70×** |
| V1 `Category = @c AND Price < 50` (~5%) | 2.64 ms (6.87 ms) | 2.48 ms (6.61 ms) | 9.78 ms (10.4 ms) | **3.7×** | **3.9×** |
| V2 `Price < 50` (~50%) | 2.04 ms (4.15 ms) | 0.67 ms (1.92 ms) | 14.4 ms (15.0 ms) | **7.0×** | **22×** |
| V3 V1 + `JOIN Authors` | 2.56 ms (6.16 ms) | 2.42 ms (5.63 ms) | 9.76 ms (10.5 ms) | **3.8×** | **4.0×** |
| V4 `JOIN Authors WHERE a.Country = @k` (~10%) | 4.34 ms (8.13 ms) | 3.62 ms (6.10 ms) | 13.1 ms (16.1 ms) | **3.0×** | **3.6×** |

| Shape (1,536 dimensions, 20,000 rows) | DataVo Flat | DataVo HNSW | SQLite + sqlite-vec | sqlite-vec / Flat | sqlite-vec / HNSW |
| --- | --- | --- | --- | ---: | ---: |
| Load + index build | 485 ms | 2,039 ms | 3,952 ms | | |
| V0 | 4.57 ms (5.71 ms) | 0.60 ms (0.95 ms) | 32.0 ms (33.0 ms) | **7.0×** | **53×** |
| V1 (~5%) | 4.69 ms (10.6 ms) | 4.66 ms (9.49 ms) | 15.4 ms (16.6 ms) | **3.3×** | **3.3×** |
| V2 (~50%) | 4.16 ms (8.25 ms) | 1.40 ms (3.26 ms) | 23.5 ms (24.5 ms) | **5.6×** | **17×** |
| V3 | 4.61 ms (10.4 ms) | 4.52 ms (9.58 ms) | 15.4 ms (18.7 ms) | **3.3×** | **3.4×** |
| V4 | 12.2 ms (14.6 ms) | 6.10 ms (10.1 ms) | 17.4 ms (18.5 ms) | **1.4×** | **2.9×** |

Recall@10 was 100% on every shape, engine and size, for every single query. No query was short and no returned row failed its filter.

¹ **Flat V0 and the JIT.** The first run of this scenario, against `216205d`, measured Flat V0 at 11.3 ms (p99 13.5 ms) where the 16 November run measured 2.13 ms. Reruns showed two speeds, about 2.1 ms and 11–14 ms, on that build and on `master` alike, and every run had a p99 near 14 ms. JIT compilation logs of a slow and a fast run contained the same final code; what differed was *when* the distance kernel `SimdDistanceKernels.Dot` and its NEON worker were promoted from tier-0 code. Tiered compilation promotes a method only after a quiet period in JIT activity, and every search before that computed 50,000 distances with unoptimized `Vector128` code. If the promotion came late, the run's median landed in the slow mode. Commit `5cbcd14` compiles the kernels' entry points fully optimized on first use (`AggressiveOptimization`) and inlines their per-ISA workers into them, and does the same for the Flat scan loop. Fifteen single-process reruns then gave medians of 2.21–2.28 ms and p99s of 3.0–4.2 ms, and the vector tables above come from a rerun against that commit. At 1,536 dimensions the Flat V0 p99 fell from 20.0 to 5.7 ms. The evidence is in [`flat-v0-jit-tiering.txt`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-16-v04-final/flat-v0-jit-tiering.txt).

| DataVo LSM, per query, 384 dimensions | 16 November: p50, allocation | Final: p50, allocation |
| --- | ---: | ---: |
| V1 Flat / HNSW | 3.61 / 6.71 ms, 5.0 MB | 2.64 / 2.48 ms, 210 KB |
| V2 Flat / HNSW | 2.11 / 0.74 ms, 552 KB | 2.04 / 0.67 ms, 197 KB |
| V3 Flat / HNSW | 4.76 / 7.77 ms, 4.9 MB | 2.56 / 2.42 ms, 183 KB |
| V4 Flat / HNSW | 7.23 / 6.15 ms, 4.6 MB | 4.34 / 3.62 ms, 232 KB |

At 1,536 dimensions the filtered shapes went from 8.4–17.0 MB to 0.48–0.58 MB per query, and HNSW V1 and V3 from 10.7 and 10.9 ms to 4.7 and 4.5 ms.

**How a filtered vector query is planned.** DataVo does not push the filter into the vector index. It asks the index for more candidates than the `LIMIT`, about `LIMIT / selectivity` (286 for V1, where about 5% of rows pass), filters them, and returns the best 10 that pass. If too few pass, it asks for twice as many, up to two more times. The planner's counters for this run:

| Shape | Candidates requested | Expansion passes (mean, 384 / 1,536) | Rows read by full table scan |
| --- | --- | ---: | ---: |
| V0 | 10 | 0 / 0 | 0 of 200 |
| V1 | median 286, max 572 | 0.10 / 0.12 | 0 |
| V2 | median 29, max 58 | 0.05 / 0.02 | 0 |
| V3 | median 286, max 1,073 | 0.10 / 0.12 | 0 |
| V4 | median 150, max 600 | 1.49 / 1.53 | 0 |

Three changes in the v0.4 closeout account for the gains:

- **Filtering candidates on stored rows (`50afc36`).** Each candidate used to be loaded as a dictionary, with its stored vector copied, to count how many pass; then loaded again to be filtered; and a join also scanned every row of the joined table (`Authors`: "full table scan 200 of 200" on 16 November). Now the `WHERE` conditions on the vector table run as typed predicates against the stored candidate rows, decoding only the columns they name, and only passing rows are built. Over a join, candidates go through the typed join in stored form, and a joined table keyed by its clustered integer primary key is read by key for just the candidates that refer to it. Allocation fell 20–35 times.
- **Cost-routing HNSW to an exact scan (`a8b3bca`).** An HNSW search widens its beam with the number of candidates requested, about `log2(n) × k × 1.5`: 286 candidates over 50,000 vectors search with a beam of about 6,700 and walk much of the graph. Measured on this data, a graph search costs about 550 ns per beam slot plus 2.4 distance computations, and an exact scan one distance computation per vector. The index now compares the two estimates and scans when that is cheaper, which happens once the beam exceeds about 9% of the vectors at 384 dimensions and 20% at 1,536. On V1 and V3 HNSW used to be 1.5–1.9 times *slower* than Flat for this reason; now the two take the same path and the same time. The scan is exact, so the routing can only raise recall.
- **Exact fallback instead of short results.** When two expansions are not enough, the query now scans the table for the passing rows and ranks only those, instead of returning fewer than `LIMIT` rows. It did not trigger in this run.

**Why DataVo Flat is faster than sqlite-vec.** Both compute exact distances. DataVo's Flat index runs vectorized distance kernels (`SimdDistanceKernels`, NEON on this machine) over vectors stored contiguously, and without a filter is 7–8 times faster than `vec0`. I did not profile sqlite-vec, so I cannot say how much of that is the kernel and how much the scan around it. With a filter the margin narrows to 1.5–3.7 times, for a reason on each side: sqlite-vec applies metadata filters inside its scan and computes no distance for a row that fails (its V1 is 1.8 times faster than its V0), while DataVo computes all 50,000 distances and filters the candidates afterwards.

**Why V4 at 1,536 dimensions is DataVo's narrowest win.** One candidate in ten survives the author filter, so V4 needs about 150 candidates, often after an expansion, each with a 6 KB vector to rank. sqlite-vec pushes the author filter into its scan as an `IN` list on a metadata column and never computes a distance for a document by an author from another country. Flat is 1.4 times faster, HNSW 2.9 times.

**HNSW's recall on clustered data.** HNSW finds 100% of the true top-10 on average on every shape here. Before 15 November it found 82–94%, and 12 to 21 of 200 queries found none of their true neighbours at any `ef_search` up to 512. The data has 128 well-separated clusters, and an index that links each node only to its nearest neighbours had almost no edges between clusters (4–6% on levels 0 to 3), so a search that descended into the wrong cluster could not leave it. Two fixes:

- **Diversity heuristic on by default (`8e472ab`).** When choosing a node's neighbours, a candidate closer to an already chosen neighbour than to the node is skipped in favour of one in another direction, as in the HNSW paper. Cross-cluster edges on levels 2 and 3 rose from 4–5% to 23% and 47%. In SQL it is `enable_diversity_heuristic` in `CREATE INDEX ... USING HNSW WITH (...)`; an index loaded from a snapshot keeps the setting it was built with.
- **Connectivity repair (`52b78cf`).** After each parallel insert batch, a repair pass links unreachable nodes. To make room it dropped any edge whose target had two incoming edges, which in a clustered graph includes the single edge into a cluster; builds without the heuristic ended with 4 to 817 unreachable nodes. The pass now never drops an edge of the spanning tree it walked.

An ablation building the index directly from the same data, one configuration per process ([`hnsw-ablation.txt`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/docs/public/benchmarks/2026-11-15-hnsw/hnsw-ablation.txt)), shows that the heuristic is the fix and that a wider construction search alone is not:

| Construction setting | Dimensions | Build | Recall@10 | Queries at 0%: default width / `ef_search` 512 | Median query |
| --- | ---: | ---: | ---: | ---: | ---: |
| Nearest neighbours only (old default) | 384 | 1,690 ms | 83.6% | 20 / 19 | 0.158 ms |
| Diversity heuristic (new default) | 384 | 1,592 ms | 100.0% | 0 / 0 | 0.166 ms |
| Nearest only, `ef_construction` uncapped | 384 | 5,520 ms | 89.8% | 17 / 13 | 0.164 ms |
| Diversity, `ef_construction` uncapped | 384 | 7,063 ms | 100.0% | 0 / 0 | 0.252 ms |
| Nearest neighbours only (old default) | 1,536 | 1,800 ms | 87.5% | 15 / 12 | 0.284 ms |
| Diversity heuristic (new default) | 1,536 | 1,900 ms | 100.0% | 0 / 0 | 0.421 ms |

The heuristic costs query time at 1,536 dimensions (0.28 to 0.42 ms median) because the search visits more of the graph. HNSW is approximate: these recall figures hold for this data, and a different distribution can give lower recall. Measure on your own embeddings.

**Allocation.** sqlite-vec allocates 2–7 KB of managed memory per query. DataVo allocates 134 KB per unfiltered query at 384 dimensions and 383 KB at 1,536 (a bound `float[]` query vector is used in place, never formatted to text and parsed back, since `3939f1d`), and 183–232 KB per filtered query at 384 dimensions. DataVo's p99 is up to 2.9 times its median on the filtered shapes, against at most 1.3 times for sqlite-vec; garbage collection is a plausible cause, which I did not measure.

## What this does not show

- One machine, one run per scenario, Apple Silicon only. The [results page](./benchmarks.md#part-iii-platforms) shows that DataVo's lead over SQLite on scans is 2 to 2.8 times smaller on x64; expect the ratios here to move as well.
- Data was in the operating system's page cache. A cold read from disk was not measured.
- Only read queries were timed. Write-heavy workloads are on the [results page](./benchmarks.md).
- SQLite and DuckDB are driven through ADO.NET, whose cost is part of their times. Neither was tuned beyond `ANALYZE`.
- The vector attributes are independent of the clusters. A filter correlated with distance changes how many candidates DataVo needs; that was not measured.
- `COUNT(DISTINCT ...)` is correct but was not timed.
- DataVo's remaining losses here have specific causes: the per-row cost of walking serialized rows on a full scan (W2–W6, C1–C3, C5, C8), a planner that does not use a secondary index to drive a `GROUP BY` (G3), the per-row decode and per-call bit-set build of the anti-join (J5), and floating-point sums that are kept in key order on LSM (C3).
