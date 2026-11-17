## SQL capability matrix: 1,000,000 orders, 100,000 customers, 1,000 products

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-17 10:45 +02:00.

Cells are the median latency of one call (p99 in brackets). `✗` marks an answer that differs from the oracle; such a cell is not a result. The last column is SQLite's median divided by DataVo LSM's (above 1: DataVo LSM is faster).

| Setup | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| Load (ms) | 3,498 | 525 | 4,595 | 999 |
| Reopen (ms) | 468.2 | 0.0 | 2.4 | 231.0 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.0 |
| Create columnar index (ms) | 317 | 180 | - | - |

### Joins

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 70.65 ms (77.37 ms) | 88.65 ms (93.69 ms) | 681.95 ms (689.94 ms) | 2.00 ms (2.47 ms) | 9.65x |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 20.43 ms (23.67 ms) | 10.59 ms (10.86 ms) | 80.49 ms (82.51 ms) | 1.49 ms (1.53 ms) | 3.94x |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 19.20 ms (28.91 ms) | 11.73 ms (17.09 ms) | 80.63 ms (81.45 ms) | 1.87 ms (2.15 ms) | 4.20x |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 70.74 ms (73.42 ms) | 90.90 ms (98.71 ms) | 226.08 ms (228.01 ms) | 3.35 ms (3.39 ms) | 3.20x |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 42.27 ms (42.68 ms) | 79.25 ms (79.87 ms) | 36.00 ms (36.67 ms) | 3.10 ms (3.36 ms) | 0.85x |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 139.44 ms (157.52 ms) | 190.29 ms (213.65 ms) | 727.49 ms (748.48 ms) | 3.30 ms (3.61 ms) | 5.22x |
| J7 Small x large: Products (1 category, 2%) x Orders | 6.45 ms (8.64 ms) | 5.95 ms (7.04 ms) | 15.01 ms (18.12 ms) | 1.04 ms (1.07 ms) | 2.33x |

### Filters

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 333.7 µs (622.3 µs) | 174.0 µs (426.7 µs) | 796.5 µs (912.9 µs) | 1.07 ms (1.27 ms) | 2.39x |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 34.33 ms (36.96 ms) | 67.48 ms (69.69 ms) | 27.91 ms (30.80 ms) | 514.1 µs (570.1 µs) | 0.81x |
| W3 (Status = A OR Status = B) AND Amount range | 67.03 ms (67.65 ms) | 102.25 ms (103.64 ms) | 59.29 ms (60.30 ms) | 2.09 ms (2.15 ms) | 0.88x |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 77.04 ms (77.84 ms) | 105.44 ms (107.94 ms) | 61.92 ms (63.42 ms) | 2.14 ms (2.21 ms) | 0.80x |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 51.10 ms (51.62 ms) | 66.63 ms (68.65 ms) | 41.37 ms (41.83 ms) | 2.91 ms (3.56 ms) | 0.81x |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 65.30 ms (65.83 ms) | 76.01 ms (76.80 ms) | 50.49 ms (55.72 ms) | 4.19 ms (5.21 ms) | 0.77x |

### Primary-key ranges

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| R1 PK range, 100 rows, BETWEEN | 12.4 µs (20.3 µs) | 21.8 µs (32.5 µs) | 18.2 µs (20.9 µs) | 180.4 µs (243.9 µs) | 1.47x |
| R2 PK range, 100 rows, >= AND < | 11.9 µs (19.4 µs) | 21.5 µs (28.6 µs) | 18.3 µs (24.3 µs) | 180.5 µs (226.3 µs) | 1.54x |
| R3 PK range, 10,000 rows, BETWEEN | 987.4 µs (11.62 ms) | 1.97 ms (14.39 ms) | 1.73 ms (2.29 ms) | 556.0 µs (737.1 µs) | 1.75x |
| R4 PK range, 10,000 rows, >= AND < | 1.01 ms (11.42 ms) | 1.86 ms (13.38 ms) | 1.75 ms (1.90 ms) | 566.0 µs (726.7 µs) | 1.73x |

### GROUP BY cardinality

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| G1 GROUP BY Status (4 groups) | 83.18 ms (83.66 ms) | 95.31 ms (97.19 ms) | 232.37 ms (237.25 ms) | 1.12 ms (1.20 ms) | 2.79x |
| G2 GROUP BY Bucket (1,000 groups) | 68.06 ms (68.93 ms) | 82.64 ms (85.72 ms) | 204.66 ms (206.89 ms) | 715.3 µs (852.9 µs) | 3.01x |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 69.11 ms (73.52 ms) | 102.36 ms (107.11 ms) | 29.20 ms (30.37 ms) | 4.32 ms (4.89 ms) | 0.42x |

### Columnar index

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| C1 COUNT(*) WHERE Amount range | 66.02 ms (68.39 ms) | 87.78 ms (88.44 ms) | 43.11 ms (43.84 ms) | 1.14 ms (1.17 ms) | 0.65x |
| C1 + columnar index | 3.05 ms (3.09 ms) pushed | 3.22 ms (3.30 ms) pushed |  |  | 14.12x |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 76.01 ms (78.38 ms) | 81.35 ms (82.28 ms) | 35.56 ms (36.45 ms) | 430.5 µs (494.9 µs) | 0.47x |
| C2 + columnar index | 3.32 ms (3.36 ms) pushed | 3.55 ms (3.59 ms) pushed |  |  | 10.71x |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 74.23 ms (75.24 ms) | 86.06 ms (86.95 ms) | 37.88 ms (39.62 ms) | 477.8 µs (540.5 µs) | 0.51x |
| C3 + columnar index | 75.47 ms (79.02 ms) row scan | 2.91 ms (3.01 ms) pushed |  |  | 0.50x |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 93.64 ms (96.14 ms) | 96.41 ms (97.69 ms) | 55.80 ms (57.30 ms) | 1.40 ms (1.43 ms) | 0.60x |
| C4 + columnar index | 11.00 ms (11.09 ms) pushed | 11.62 ms (11.71 ms) pushed |  |  | 5.08x |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 45.10 ms (46.36 ms) | 73.56 ms (74.40 ms) | 41.19 ms (41.43 ms) | 924.3 µs (1.09 ms) | 0.91x |
| C5 + columnar index | 45.31 ms (46.51 ms) row scan | 73.45 ms (74.35 ms) row scan |  |  | 0.91x |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 70.74 ms (72.80 ms) | 88.91 ms (90.85 ms) | 34.71 ms (35.22 ms) | 1.57 ms (1.69 ms) | 0.49x |
| C6 + columnar index | 4.62 ms (4.74 ms) pushed | 473.6 µs (618.1 µs) pushed |  |  | 7.51x |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 67.69 ms (72.62 ms) | 82.81 ms (86.56 ms) | 194.04 ms (196.20 ms) | 672.8 µs (722.4 µs) | 2.87x |
| C7 + columnar index | 68.00 ms (72.29 ms) row scan | 82.61 ms (86.47 ms) row scan |  |  | 2.85x |
| C8 ORDER BY Amount DESC LIMIT 10 | 56.57 ms (58.04 ms) | 78.14 ms (80.68 ms) | 39.84 ms (40.41 ms) | 1.26 ms (1.44 ms) | 0.70x |
| C8 + columnar index | 56.77 ms (57.61 ms) row scan | 77.79 ms (78.02 ms) row scan |  |  | 0.70x |

### Managed allocation per call and GC collections during the timed calls

Bytes allocated on the calling thread per call (`GC.GetAllocatedBytesForCurrentThread`), with gen0/gen1/gen2 collections over all timed calls. SQLite's and DuckDB's native memory is not visible to this counter; their figures are only what the ADO.NET layer allocates.

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 39.7 KB (0/0/0) | 35.2 KB (0/0/0) | 432 B (0/0/0) | 1.3 KB (0/0/0) |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 4.1 MB (4/0/0) | 1.0 MB (0/0/0) | 696 B (0/0/0) | 1.7 KB (0/0/0) |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 11.5 MB (12/3/0) | 4.7 MB (3/1/0) | 6.5 KB (0/0/0) | 10.3 KB (0/0/0) |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 33.7 KB (0/0/0) | 32.8 KB (0/0/0) | 456 B (0/0/0) | 1.5 KB (0/0/0) |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 30.5 KB (0/0/0) | 29.4 KB (0/0/0) | 408 B (0/0/0) | 1.1 KB (0/0/0) |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 24.4 MB (28/16/3) | 21.3 MB (23/12/1) | 1.0 KB (0/0/0) | 2.5 KB (0/0/0) |
| J7 Small x large: Products (1 category, 2%) x Orders | 28.5 KB (0/0/0) | 20.2 KB (0/0/0) | 560 B (0/0/0) | 1.5 KB (0/0/0) |
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 329.1 KB (40/14/0) | 312.7 KB (38/13/0) | 48.6 KB (5/0/0) | 97.5 KB (11/0/0) |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 318.2 KB (0/0/0) | 318.3 KB (0/0/0) | 48.7 KB (0/0/0) | 98.1 KB (0/0/0) |
| W3 (Status = A OR Status = B) AND Amount range | 22.1 KB (0/0/0) | 21.1 KB (0/0/0) | 688 B (0/0/0) | 1.7 KB (0/0/0) |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 23.2 KB (0/0/0) | 22.2 KB (0/0/0) | 816 B (0/0/0) | 1.9 KB (0/0/0) |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 15.8 KB (0/0/0) | 14.9 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 15.9 KB (0/0/0) | 14.9 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| R1 PK range, 100 rows, BETWEEN | 31.1 KB (3/0/0) | 35.1 KB (4/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R2 PK range, 100 rows, >= AND < | 31.1 KB (3/0/0) | 35.1 KB (4/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R3 PK range, 10,000 rows, BETWEEN | 3.0 MB (40/23/4) | 3.4 MB (39/21/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| R4 PK range, 10,000 rows, >= AND < | 3.0 MB (40/23/4) | 3.4 MB (39/21/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| G1 GROUP BY Status (4 groups) | 34.4 MB (43/0/0) | 24.4 KB (0/0/0) | 720 B (0/0/0) | 1.9 KB (0/0/0) |
| G2 GROUP BY Bucket (1,000 groups) | 2.1 MB (2/0/0) | 2.1 MB (2/0/0) | 70.7 KB (0/0/0) | 142.0 KB (0/0/0) |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 15.9 MB (8/3/0) | 16.0 MB (8/3/0) | 5.2 KB (0/0/0) | 10.8 KB (0/0/0) |
| C1 COUNT(*) WHERE Amount range | 15.9 KB (0/0/0) | 15.1 KB (0/0/0) | 664 B (0/0/0) | 1.5 KB (0/0/0) |
| C1 + columnar index | 15.9 KB (0/0/0) | 14.9 KB (0/0/0) | - | - |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 16.3 KB (0/0/0) | 15.4 KB (0/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C2 + columnar index | 16.3 KB (0/0/0) | 15.3 KB (0/0/0) | - | - |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 16.3 KB (0/0/0) | 15.4 KB (0/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C3 + columnar index | 16.3 KB (0/0/0) | 15.3 KB (0/0/0) | - | - |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 23.8 KB (0/0/0) | 22.9 KB (0/0/0) | 608 B (0/0/0) | 2.1 KB (0/0/0) |
| C4 + columnar index | 23.4 KB (0/0/0) | 22.4 KB (0/0/0) | - | - |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 17.0 KB (0/0/0) | 16.1 KB (0/0/0) | 672 B (0/0/0) | 1.5 KB (0/0/0) |
| C5 + columnar index | 17.1 KB (0/0/0) | 16.2 KB (0/0/0) | - | - |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 16.9 KB (0/0/0) | 16.0 KB (0/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C6 + columnar index | 16.9 KB (0/0/0) | 15.9 KB (0/0/0) | - | - |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 2.1 MB (2/1/0) | 2.1 MB (2/1/0) | 70.7 KB (0/0/0) | 126.4 KB (0/0/0) |
| C7 + columnar index | 2.1 MB (2/0/0) | 2.1 MB (2/1/0) | - | - |
| C8 ORDER BY Amount DESC LIMIT 10 | 48.4 KB (0/0/0) | 47.5 KB (0/0/0) | 864 B (0/0/0) | 2.1 KB (0/0/0) |
| C8 + columnar index | 48.4 KB (0/0/0) | 47.5 KB (0/0/0) | - | - |

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
