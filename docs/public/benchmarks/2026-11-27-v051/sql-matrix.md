## SQL capability matrix: 1,000,000 orders, 100,000 customers, 1,000 products

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-27 17:07 +02:00.

Cells are the median latency of one call (p99 in brackets). `✗` marks an answer that differs from the oracle; such a cell is not a result. The last column is SQLite's median divided by DataVo LSM's (above 1: DataVo LSM is faster).

| Setup | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| Load (ms) | 3,737 | 535 | 5,243 | 1,096 |
| Close and reopen after the load (ms) | 387.7 | 0.0 | 2.9 | 258.4 |
| Close and reopen again, nothing to write (ms) | 7.7 | 0.0 | 0.6 | 3.8 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.0 |
| Create columnar index (ms) | 328 | 182 | - | - |

### Joins

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 71.76 ms (87.21 ms) | 93.40 ms (100.04 ms) | 765.09 ms (804.46 ms) | 2.12 ms (2.42 ms) | 10.66x |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 22.04 ms (24.10 ms) | 8.93 ms (9.17 ms) | 90.67 ms (107.16 ms) | 1.50 ms (1.70 ms) | 4.11x |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 14.73 ms (19.84 ms) | 10.73 ms (17.82 ms) | 89.03 ms (89.65 ms) | 1.99 ms (2.27 ms) | 6.04x |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 68.99 ms (82.01 ms) | 92.47 ms (93.83 ms) | 240.16 ms (263.33 ms) | 3.45 ms (3.63 ms) | 3.48x |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 40.10 ms (42.90 ms) | 83.79 ms (92.13 ms) | 36.81 ms (38.17 ms) | 3.31 ms (3.60 ms) | 0.92x |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 157.97 ms (182.17 ms) | 231.48 ms (266.77 ms) | 795.12 ms (847.54 ms) | 3.41 ms (3.64 ms) | 5.03x |
| J7 Small x large: Products (1 category, 2%) x Orders | 7.10 ms (7.55 ms) | 4.98 ms (6.09 ms) | 16.08 ms (16.35 ms) | 1.10 ms (1.39 ms) | 2.26x |

### Filters

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 340.0 µs (779.3 µs) | 154.8 µs (427.2 µs) | 894.5 µs (1.19 ms) | 1.14 ms (1.44 ms) | 2.63x |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 35.13 ms (39.38 ms) | 66.06 ms (69.27 ms) | 28.27 ms (31.52 ms) | 535.5 µs (748.2 µs) | 0.80x |
| W3 (Status = A OR Status = B) AND Amount range | 70.53 ms (78.54 ms) | 83.76 ms (87.82 ms) | 61.36 ms (72.20 ms) | 2.04 ms (2.13 ms) | 0.87x |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 84.68 ms (85.55 ms) | 91.70 ms (102.28 ms) | 65.26 ms (74.31 ms) | 2.11 ms (2.69 ms) | 0.77x |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 49.19 ms (50.85 ms) | 33.84 ms (34.52 ms) | 42.54 ms (43.53 ms) | 2.97 ms (3.26 ms) | 0.86x |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 66.73 ms (77.00 ms) | 43.24 ms (43.51 ms) | 51.91 ms (52.59 ms) | 4.21 ms (4.43 ms) | 0.78x |

### Primary-key ranges

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| R1 PK range, 100 rows, BETWEEN | 14.0 µs (22.8 µs) | 20.6 µs (30.0 µs) | 18.3 µs (27.5 µs) | 171.7 µs (1.86 ms) | 1.31x |
| R2 PK range, 100 rows, >= AND < | 13.5 µs (20.8 µs) | 20.3 µs (34.1 µs) | 18.5 µs (33.8 µs) | 172.1 µs (1.54 ms) | 1.37x |
| R3 PK range, 10,000 rows, BETWEEN | 1.05 ms (12.01 ms) | 1.74 ms (13.77 ms) | 1.76 ms (2.04 ms) | 608.6 µs (1.38 ms) | 1.68x |
| R4 PK range, 10,000 rows, >= AND < | 1.03 ms (11.54 ms) | 1.55 ms (11.99 ms) | 1.79 ms (2.20 ms) | 596.8 µs (1.21 ms) | 1.73x |

### GROUP BY cardinality

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| G1 GROUP BY Status (4 groups) | 51.31 ms (62.99 ms) | 68.15 ms (68.79 ms) | 241.31 ms (256.03 ms) | 1.17 ms (1.30 ms) | 4.70x |
| G2 GROUP BY Bucket (1,000 groups) | 63.71 ms (67.74 ms) | 52.12 ms (55.27 ms) | 213.67 ms (226.74 ms) | 787.6 µs (1.04 ms) | 3.35x |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 31.18 ms (34.58 ms) | 19.81 ms (21.65 ms) | 30.68 ms (31.05 ms) | 4.81 ms (6.80 ms) | 0.98x |

### Columnar index

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| C1 COUNT(*) WHERE Amount range | 65.69 ms (68.07 ms) | 56.12 ms (60.12 ms) | 43.46 ms (45.57 ms) | 1.37 ms (2.08 ms) | 0.66x |
| C1 + columnar index | 3.15 ms (3.29 ms) pushed | 3.32 ms (3.44 ms) pushed |  |  | 13.80x |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 71.60 ms (73.24 ms) | 49.96 ms (51.59 ms) | 36.71 ms (37.57 ms) | 488.2 µs (675.2 µs) | 0.51x |
| C2 + columnar index | 3.42 ms (3.67 ms) pushed | 3.70 ms (3.74 ms) pushed |  |  | 10.74x |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 74.23 ms (86.60 ms) | 57.36 ms (59.41 ms) | 40.15 ms (42.24 ms) | 532.0 µs (644.8 µs) | 0.54x |
| C3 + columnar index | 74.33 ms (76.54 ms) row scan | 3.13 ms (3.51 ms) pushed |  |  | 0.54x |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 89.60 ms (96.09 ms) | 73.40 ms (74.88 ms) | 60.14 ms (89.39 ms) | 1.46 ms (1.66 ms) | 0.67x |
| C4 + columnar index | 11.93 ms (12.24 ms) pushed | 12.36 ms (12.44 ms) pushed |  |  | 5.04x |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 42.92 ms (43.81 ms) | 48.73 ms (49.84 ms) | 44.32 ms (45.96 ms) | 1.02 ms (1.26 ms) | 1.03x |
| C5 + columnar index | 42.15 ms (42.91 ms) row scan | 48.04 ms (49.69 ms) row scan |  |  | 1.05x |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 66.52 ms (66.98 ms) | 59.79 ms (62.57 ms) | 35.99 ms (36.51 ms) | 1.68 ms (1.90 ms) | 0.54x |
| C6 + columnar index | 4.69 ms (5.00 ms) pushed | 561.5 µs (791.7 µs) pushed |  |  | 7.67x |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 63.15 ms (66.89 ms) | 51.56 ms (56.32 ms) | 206.29 ms (213.82 ms) | 739.3 µs (880.0 µs) | 3.27x |
| C7 + columnar index | 64.10 ms (66.60 ms) row scan | 51.27 ms (53.25 ms) row scan |  |  | 3.22x |
| C8 ORDER BY Amount DESC LIMIT 10 | 52.11 ms (54.53 ms) | 46.65 ms (47.31 ms) | 40.56 ms (41.05 ms) | 1.45 ms (1.65 ms) | 0.78x |
| C8 + columnar index | 52.52 ms (53.06 ms) row scan | 47.25 ms (48.10 ms) row scan |  |  | 0.77x |

### Managed allocation per call and GC collections during the timed calls

Bytes allocated on the calling thread per call (`GC.GetAllocatedBytesForCurrentThread`), with gen0/gen1/gen2 collections over all timed calls. SQLite's and DuckDB's native memory is not visible to this counter; their figures are only what the ADO.NET layer allocates.

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 40.3 KB (0/0/0) | 36.2 KB (0/0/0) | 432 B (0/0/0) | 1.3 KB (0/0/0) |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 1.0 MB (0/0/0) | 1.0 MB (0/0/0) | 696 B (0/0/0) | 1.7 KB (0/0/0) |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 5.2 MB (4/2/0) | 4.7 MB (3/1/0) | 6.5 KB (0/0/0) | 10.3 KB (0/0/0) |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 34.6 KB (0/0/0) | 33.5 KB (0/0/0) | 456 B (0/0/0) | 1.5 KB (0/0/0) |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 31.3 KB (0/0/0) | 30.1 KB (0/0/0) | 408 B (0/0/0) | 1.1 KB (0/0/0) |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 21.3 MB (25/14/3) | 21.3 MB (23/12/1) | 1.0 KB (0/0/0) | 2.5 KB (0/0/0) |
| J7 Small x large: Products (1 category, 2%) x Orders | 27.9 KB (0/0/0) | 21.0 KB (0/0/0) | 560 B (0/0/0) | 1.5 KB (0/0/0) |
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 329.0 KB (40/14/0) | 312.6 KB (38/14/0) | 48.6 KB (5/0/0) | 97.5 KB (11/0/0) |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 318.2 KB (0/0/0) | 318.2 KB (0/0/0) | 48.7 KB (0/0/0) | 98.1 KB (0/0/0) |
| W3 (Status = A OR Status = B) AND Amount range | 22.8 KB (0/0/0) | 21.6 KB (0/0/0) | 688 B (0/0/0) | 1.7 KB (0/0/0) |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 23.8 KB (0/0/0) | 22.7 KB (0/0/0) | 816 B (0/0/0) | 1.9 KB (0/0/0) |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 16.5 KB (0/0/0) | 15.4 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 16.5 KB (0/0/0) | 15.5 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| R1 PK range, 100 rows, BETWEEN | 31.1 KB (3/0/0) | 34.9 KB (4/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R2 PK range, 100 rows, >= AND < | 31.1 KB (3/0/0) | 34.9 KB (4/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R3 PK range, 10,000 rows, BETWEEN | 3.0 MB (40/23/4) | 3.4 MB (39/21/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| R4 PK range, 10,000 rows, >= AND < | 3.0 MB (40/23/4) | 3.4 MB (39/21/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| G1 GROUP BY Status (4 groups) | 26.9 KB (0/0/0) | 25.9 KB (0/0/0) | 720 B (0/0/0) | 1.9 KB (0/0/0) |
| G2 GROUP BY Bucket (1,000 groups) | 2.1 MB (2/0/0) | 2.1 MB (2/0/0) | 70.7 KB (0/0/0) | 142.0 KB (0/0/0) |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 909.6 KB (1/0/0) | 921.3 KB (1/0/0) | 5.2 KB (0/0/0) | 10.8 KB (0/0/0) |
| C1 COUNT(*) WHERE Amount range | 16.6 KB (0/0/0) | 15.6 KB (0/0/0) | 664 B (0/0/0) | 1.5 KB (0/0/0) |
| C1 + columnar index | 16.4 KB (0/0/0) | 15.4 KB (0/0/0) | - | - |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 17.0 KB (0/0/0) | 16.0 KB (0/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C2 + columnar index | 16.7 KB (0/0/0) | 15.7 KB (0/0/0) | - | - |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 17.0 KB (0/0/0) | 16.0 KB (0/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C3 + columnar index | 17.0 KB (0/0/0) | 15.8 KB (0/0/0) | - | - |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 24.4 KB (0/0/0) | 23.4 KB (0/0/0) | 608 B (0/0/0) | 2.1 KB (0/0/0) |
| C4 + columnar index | 23.8 KB (0/0/0) | 22.8 KB (0/0/0) | - | - |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 17.7 KB (0/0/0) | 16.7 KB (0/0/0) | 672 B (0/0/0) | 1.5 KB (0/0/0) |
| C5 + columnar index | 17.7 KB (0/0/0) | 16.7 KB (0/0/0) | - | - |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 17.6 KB (0/0/0) | 16.6 KB (0/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C6 + columnar index | 17.3 KB (0/0/0) | 16.3 KB (0/0/0) | - | - |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 2.1 MB (2/1/0) | 2.1 MB (2/1/0) | 70.7 KB (0/0/0) | 126.4 KB (0/0/0) |
| C7 + columnar index | 2.1 MB (2/0/0) | 2.1 MB (2/1/0) | - | - |
| C8 ORDER BY Amount DESC LIMIT 10 | 49.6 KB (0/0/0) | 48.5 KB (0/0/0) | 864 B (0/0/0) | 2.1 KB (0/0/0) |
| C8 + columnar index | 49.5 KB (0/0/0) | 48.5 KB (0/0/0) | - | - |

### Answer check

- Every engine returned the oracle's answer for every query and parameter set.

### Columnar pushdown (DataVo)

- DataVo (LSM) [DataVo.Core net10.0], C1: answered by the columnar index
- DataVo (LSM) [DataVo.Core net10.0], C2: answered by the columnar index
- DataVo (LSM) [DataVo.Core net10.0], C3: row scan (index not used)
- DataVo (LSM) [DataVo.Core net10.0], C4: answered by the columnar index
- DataVo (LSM) [DataVo.Core net10.0], C5: row scan (index not used)
- DataVo (LSM) [DataVo.Core net10.0], C6: answered by the columnar index
- DataVo (LSM) [DataVo.Core net10.0], C7: row scan (index not used)
- DataVo (LSM) [DataVo.Core net10.0], C8: row scan (index not used)
- DataVo (InMemory) [DataVo.Core net10.0], C1: answered by the columnar index
- DataVo (InMemory) [DataVo.Core net10.0], C2: answered by the columnar index
- DataVo (InMemory) [DataVo.Core net10.0], C3: answered by the columnar index
- DataVo (InMemory) [DataVo.Core net10.0], C4: answered by the columnar index
- DataVo (InMemory) [DataVo.Core net10.0], C5: row scan (index not used)
- DataVo (InMemory) [DataVo.Core net10.0], C6: answered by the columnar index
- DataVo (InMemory) [DataVo.Core net10.0], C7: row scan (index not used)
- DataVo (InMemory) [DataVo.Core net10.0], C8: row scan (index not used)

### Engines

- **DataVo (LSM) [DataVo.Core net10.0]**: LSM, LsmStrictFsync=false (Relaxed: no flush per commit); bulk load via InsertTypedBatch, then close and reopen; prepared statements with bound parameters.
- **DataVo (InMemory) [DataVo.Core net10.0]**: InMemory engine; bulk load via InsertTypedBatch; prepared statements with bound parameters.
- **SQLite**: File, journal_mode=WAL, synchronous=NORMAL; load in one transaction then ANALYZE; prepared commands with parameters (Microsoft.Data.Sqlite).
- **DuckDB**: File database, default threads (12 cores); appender for bulk load, then close and reopen; prepared commands with named parameters (DuckDB.NET).

### Queries

Timed calls: 10 for scans, 100 for 10,000-row ranges, 1000 for selective queries, after unmeasured warm-up calls; parameter sets are cycled.

- **J1** (3-table inner join, no filter): `SELECT COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id` with no parameters
- **J2** (3-table inner join, filter on both dimensions (~0.2%)): `SELECT COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id WHERE c.Region = @region AND p.CategoryId = @cat` with @region=R3, @cat=7; @region=R5, @cat=12; @region=R0, @cat=33; @region=R6, @cat=48
- **J3** (3-table join, selective filter, ORDER BY ... LIMIT 50): `SELECT o.Id, o.Amount, c.Name AS CustomerName, p.Name AS ProductName FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id WHERE c.Region = @region AND p.CategoryId = @cat ORDER BY o.Amount DESC, o.Id ASC LIMIT 50` with @region=R3, @cat=7; @region=R5, @cat=12; @region=R0, @cat=33; @region=R6, @cat=48
- **J4** (LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM): `SELECT COUNT(*), COUNT(c.Id), SUM(o.Amount) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id` with no parameters
- **J5** (LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL)): `SELECT COUNT(*) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id WHERE c.Id IS NULL` with no parameters
- **J6** (Large x medium: Orders x Customers, GROUP BY Region): `SELECT c.Region, COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id GROUP BY c.Region` with no parameters
- **J7** (Small x large: Products (1 category, 2%) x Orders): `SELECT COUNT(*), SUM(o.Amount) FROM Products p JOIN Orders o ON o.ProductId = p.Id WHERE p.CategoryId = @cat` with @cat=3; @cat=17; @cat=29; @cat=41
- **W1** (Equality, 0.1% of rows, secondary index (ProductId)): `SELECT Id, Amount FROM Orders WHERE ProductId = @p` with @p=7; @p=333; @p=500; @p=997
- **W2** (Equality, 0.1% of rows, no index (ProductTag)): `SELECT Id, Amount FROM Orders WHERE ProductTag = @p` with @p=7; @p=333; @p=500; @p=997
- **W3** ((Status = A OR Status = B) AND Amount range): `SELECT COUNT(*), SUM(Amount) FROM Orders WHERE (Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= @lo AND Amount < @hi` with @lo=200, @hi=500; @lo=0, @hi=250; @lo=600, @hi=900
- **W4** (W3 + NULL-aware: Discount IS NOT NULL AND Discount < x): `SELECT COUNT(*), SUM(Amount) FROM Orders WHERE (Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= @lo AND Amount < @hi AND Discount IS NOT NULL AND Discount < @disc` with @lo=200, @hi=500, @disc=5; @lo=0, @hi=250, @disc=2.5
- **W5** (Prefix LIKE 'S0042%' (0.01%, no index)): `SELECT COUNT(*) FROM Orders WHERE Sku LIKE @pattern` with @pattern=S0042%; @pattern=S1234%; @pattern=S9001%
- **W6** (Substring LIKE '%042-%' (0.1%, no index)): `SELECT COUNT(*) FROM Orders WHERE Sku LIKE @pattern` with @pattern=%042-%; @pattern=%777-%; @pattern=%123-%
- **R1** (PK range, 100 rows, BETWEEN): `SELECT Id, Amount FROM Orders WHERE Id BETWEEN @lo AND @hi` with @lo=573458, @hi=573557; @lo=877952, @hi=878051; @lo=127708, @hi=127807; @lo=107237, @hi=107336; @lo=248771, @hi=248870; @lo=644366, @hi=644465; @lo=734256, @hi=734355; @lo=319601, @hi=319700
- **R2** (PK range, 100 rows, >= AND <): `SELECT Id, Amount FROM Orders WHERE Id >= @lo AND Id < @hi` with @lo=148328, @hi=148428; @lo=300629, @hi=300729; @lo=505758, @hi=505858; @lo=842462, @hi=842562; @lo=905413, @hi=905513; @lo=788997, @hi=789097; @lo=750280, @hi=750380; @lo=105541, @hi=105641
- **R3** (PK range, 10,000 rows, BETWEEN): `SELECT Id, Amount FROM Orders WHERE Id BETWEEN @lo AND @hi` with @lo=894434, @hi=904433; @lo=40310, @hi=50309; @lo=852389, @hi=862388; @lo=572542, @hi=582541; @lo=929676, @hi=939675; @lo=639063, @hi=649062; @lo=765558, @hi=775557; @lo=711894, @hi=721893
- **R4** (PK range, 10,000 rows, >= AND <): `SELECT Id, Amount FROM Orders WHERE Id >= @lo AND Id < @hi` with @lo=626976, @hi=636976; @lo=815920, @hi=825920; @lo=869764, @hi=879764; @lo=192402, @hi=202402; @lo=541593, @hi=551593; @lo=553843, @hi=563843; @lo=491710, @hi=501710; @lo=103530, @hi=113530
- **G1** (GROUP BY Status (4 groups)): `SELECT Status, COUNT(*), SUM(Amount) FROM Orders GROUP BY Status` with no parameters
- **G2** (GROUP BY Bucket (1,000 groups)): `SELECT Bucket, COUNT(*), SUM(Amount) FROM Orders GROUP BY Bucket` with no parameters
- **G3** (GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100): `SELECT CustomerId, COUNT(*) AS N FROM Orders GROUP BY CustomerId HAVING COUNT(*) >= @k ORDER BY N DESC, CustomerId ASC LIMIT 100` with @k=12; @k=16
- **C1** (COUNT(*) WHERE Amount range): `SELECT COUNT(*) FROM Orders WHERE Amount >= @lo AND Amount < @hi` with @lo=200, @hi=500; @lo=0, @hi=100
- **C2** (SUM(Qty) WHERE Bucket < b (INT sum)): `SELECT SUM(Qty) FROM Orders WHERE Bucket < @b` with @b=100; @b=500
- **C3** (SUM(Amount) WHERE Qty >= q (DOUBLE sum)): `SELECT SUM(Amount) FROM Orders WHERE Qty >= @q` with @q=5; @q=9
- **C4** (COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL): `SELECT COUNT(*), SUM(Qty), MIN(Amount), MAX(Amount) FROM Orders WHERE Bucket < @b AND Discount IS NOT NULL` with @b=100; @b=700
- **C5** (COUNT(*) WHERE Status = s AND Amount >= x (string column)): `SELECT COUNT(*) FROM Orders WHERE Status = @status AND Amount >= @lo` with @status=PAID, @lo=200; @status=NEW, @lo=900
- **C6** (COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x): `SELECT COUNT(*) FROM Orders WHERE (Qty = 1 OR Qty = 2) AND Amount >= @lo` with @lo=200; @lo=900
- **C7** (GROUP BY Bucket: COUNT, SUM(Qty)): `SELECT Bucket, COUNT(*), SUM(Qty) FROM Orders GROUP BY Bucket` with no parameters
- **C8** (ORDER BY Amount DESC LIMIT 10): `SELECT Id, Amount FROM Orders ORDER BY Amount DESC, Id ASC LIMIT 10` with no parameters
