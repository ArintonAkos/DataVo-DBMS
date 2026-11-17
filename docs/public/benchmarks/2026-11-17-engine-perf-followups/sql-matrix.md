## SQL capability matrix: 1,000,000 orders, 100,000 customers, 1,000 products

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-17 22:08 +02:00.

Cells are the median latency of one call (p99 in brackets). `✗` marks an answer that differs from the oracle; such a cell is not a result. The last column is SQLite's median divided by DataVo LSM's (above 1: DataVo LSM is faster).

| Setup | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| Load (ms) | 3,564 | 536 | 4,952 | 1,014 |
| Close and reopen after the load (ms) | 506.1 | 0.0 | 2.2 | 235.2 |
| Close and reopen again, nothing to write (ms) | 8.0 | 0.0 | 0.4 | 3.7 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.3 |
| Create columnar index (ms) | 317 | 188 | - | - |

### Joins

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 70.16 ms (82.86 ms) | 90.19 ms (96.15 ms) | 721.36 ms (739.95 ms) | 2.11 ms (2.45 ms) | 10.28x |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 21.92 ms (23.43 ms) | 10.84 ms (11.16 ms) | 84.38 ms (87.44 ms) | 1.51 ms (1.71 ms) | 3.85x |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 16.10 ms (19.98 ms) | 12.77 ms (15.86 ms) | 84.04 ms (85.24 ms) | 1.86 ms (2.07 ms) | 5.22x |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 68.29 ms (78.91 ms) | 93.08 ms (93.57 ms) | 237.89 ms (238.79 ms) | 3.37 ms (3.52 ms) | 3.48x |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 39.07 ms (39.34 ms) | 81.77 ms (89.76 ms) | 36.58 ms (36.87 ms) | 3.30 ms (3.41 ms) | 0.94x |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 130.33 ms (140.66 ms) | 195.14 ms (209.47 ms) | 764.43 ms (773.73 ms) | 3.40 ms (3.51 ms) | 5.87x |
| J7 Small x large: Products (1 category, 2%) x Orders | 6.70 ms (9.62 ms) | 6.20 ms (7.31 ms) | 15.31 ms (15.68 ms) | 1.05 ms (1.10 ms) | 2.28x |

### Filters

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 341.4 µs (658.0 µs) | 183.7 µs (479.4 µs) | 824.6 µs (920.1 µs) | 1.13 ms (1.36 ms) | 2.42x |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 30.46 ms (33.38 ms) | 66.51 ms (69.17 ms) | 28.65 ms (30.86 ms) | 553.1 µs (611.5 µs) | 0.94x |
| W3 (Status = A OR Status = B) AND Amount range | 64.01 ms (66.05 ms) | 76.26 ms (77.08 ms) | 61.11 ms (62.68 ms) | 2.11 ms (2.21 ms) | 0.95x |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 74.19 ms (78.13 ms) | 84.70 ms (89.01 ms) | 63.28 ms (65.32 ms) | 2.08 ms (2.16 ms) | 0.85x |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 47.58 ms (48.05 ms) | 33.22 ms (34.53 ms) | 42.23 ms (43.38 ms) | 2.85 ms (2.94 ms) | 0.89x |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 68.71 ms (70.30 ms) | 41.94 ms (42.27 ms) | 50.71 ms (51.37 ms) | 4.16 ms (4.62 ms) | 0.74x |

### Primary-key ranges

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| R1 PK range, 100 rows, BETWEEN | 13.6 µs (20.5 µs) | 22.9 µs (45.2 µs) | 18.5 µs (24.6 µs) | 177.5 µs (2.43 ms) | 1.35x |
| R2 PK range, 100 rows, >= AND < | 12.9 µs (19.3 µs) | 23.0 µs (31.6 µs) | 18.2 µs (23.5 µs) | 179.9 µs (280.8 µs) | 1.41x |
| R3 PK range, 10,000 rows, BETWEEN | 1.01 ms (12.00 ms) | 2.13 ms (15.21 ms) | 1.76 ms (1.95 ms) | 588.3 µs (760.4 µs) | 1.75x |
| R4 PK range, 10,000 rows, >= AND < | 990.9 µs (11.89 ms) | 1.99 ms (13.69 ms) | 1.74 ms (1.85 ms) | 587.7 µs (1.15 ms) | 1.76x |

### GROUP BY cardinality

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| G1 GROUP BY Status (4 groups) | 50.54 ms (50.74 ms) | 66.99 ms (67.87 ms) | 237.49 ms (239.25 ms) | 1.12 ms (1.23 ms) | 4.70x |
| G2 GROUP BY Bucket (1,000 groups) | 63.38 ms (65.94 ms) | 51.43 ms (54.68 ms) | 209.16 ms (224.66 ms) | 727.2 µs (795.5 µs) | 3.30x |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 44.17 ms (48.06 ms) | 16.70 ms (22.40 ms) | 30.54 ms (32.21 ms) | 4.40 ms (4.54 ms) | 0.69x |

### Columnar index

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| C1 COUNT(*) WHERE Amount range | 53.50 ms (56.15 ms) | 47.66 ms (48.39 ms) | 43.66 ms (44.12 ms) | 1.13 ms (1.24 ms) | 0.82x |
| C1 + columnar index | 3.18 ms (3.28 ms) pushed | 3.27 ms (3.43 ms) pushed |  |  | 13.73x |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 67.19 ms (68.07 ms) | 46.57 ms (48.40 ms) | 36.66 ms (37.10 ms) | 414.5 µs (456.3 µs) | 0.55x |
| C2 + columnar index | 3.44 ms (3.51 ms) pushed | 3.73 ms (3.84 ms) pushed |  |  | 10.65x |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 65.13 ms (68.86 ms) | 50.47 ms (51.69 ms) | 39.91 ms (40.72 ms) | 459.4 µs (543.9 µs) | 0.61x |
| C3 + columnar index | 65.89 ms (67.77 ms) row scan | 3.05 ms (3.23 ms) pushed |  |  | 0.61x |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 86.69 ms (89.75 ms) | 69.71 ms (71.25 ms) | 57.38 ms (58.92 ms) | 1.38 ms (1.45 ms) | 0.66x |
| C4 + columnar index | 11.91 ms (11.99 ms) pushed | 8.48 ms (12.04 ms) pushed |  |  | 4.82x |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 40.05 ms (40.89 ms) | 45.71 ms (47.12 ms) | 42.67 ms (43.72 ms) | 984.5 µs (1.12 ms) | 1.07x |
| C5 + columnar index | 39.94 ms (40.90 ms) row scan | 45.92 ms (47.62 ms) row scan |  |  | 1.07x |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 64.08 ms (64.88 ms) | 57.38 ms (58.83 ms) | 35.56 ms (35.99 ms) | 1.69 ms (1.90 ms) | 0.55x |
| C6 + columnar index | 4.77 ms (4.83 ms) pushed | 496.6 µs (666.2 µs) pushed |  |  | 7.46x |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 63.06 ms (67.21 ms) | 51.07 ms (54.45 ms) | 200.16 ms (202.23 ms) | 744.2 µs (935.4 µs) | 3.17x |
| C7 + columnar index | 63.23 ms (74.75 ms) row scan | 50.88 ms (52.01 ms) row scan |  |  | 3.17x |
| C8 ORDER BY Amount DESC LIMIT 10 | 52.98 ms (53.59 ms) | 46.90 ms (47.99 ms) | 41.57 ms (42.75 ms) | 1.31 ms (1.63 ms) | 0.78x |
| C8 + columnar index | 52.89 ms (54.45 ms) row scan | 46.71 ms (48.04 ms) row scan |  |  | 0.79x |

### Managed allocation per call and GC collections during the timed calls

Bytes allocated on the calling thread per call (`GC.GetAllocatedBytesForCurrentThread`), with gen0/gen1/gen2 collections over all timed calls. SQLite's and DuckDB's native memory is not visible to this counter; their figures are only what the ADO.NET layer allocates.

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 40.5 KB (0/0/0) | 35.7 KB (0/0/0) | 432 B (0/0/0) | 1.3 KB (0/0/0) |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 1.0 MB (0/0/0) | 1.0 MB (0/0/0) | 696 B (0/0/0) | 1.7 KB (0/0/0) |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 5.2 MB (4/2/0) | 4.7 MB (3/1/0) | 6.5 KB (0/0/0) | 10.3 KB (0/0/0) |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 34.1 KB (0/0/0) | 33.0 KB (0/0/0) | 456 B (0/0/0) | 1.5 KB (0/0/0) |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 30.9 KB (0/0/0) | 29.6 KB (0/0/0) | 408 B (0/0/0) | 1.1 KB (0/0/0) |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 21.3 MB (25/14/3) | 21.3 MB (23/12/1) | 1.0 KB (0/0/0) | 2.5 KB (0/0/0) |
| J7 Small x large: Products (1 category, 2%) x Orders | 28.9 KB (0/0/0) | 20.6 KB (0/0/0) | 560 B (0/0/0) | 1.5 KB (0/0/0) |
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 329.1 KB (40/14/0) | 312.7 KB (38/13/0) | 48.6 KB (5/0/0) | 97.5 KB (11/0/0) |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 318.2 KB (0/0/0) | 318.2 KB (0/0/0) | 48.7 KB (0/0/0) | 98.1 KB (0/0/0) |
| W3 (Status = A OR Status = B) AND Amount range | 22.7 KB (0/0/0) | 21.6 KB (0/0/0) | 688 B (0/0/0) | 1.7 KB (0/0/0) |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 23.7 KB (0/0/0) | 22.7 KB (0/0/0) | 816 B (0/0/0) | 1.9 KB (0/0/0) |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 16.4 KB (0/0/0) | 15.4 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 16.4 KB (0/0/0) | 15.4 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| R1 PK range, 100 rows, BETWEEN | 31.1 KB (3/0/0) | 35.1 KB (4/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R2 PK range, 100 rows, >= AND < | 31.1 KB (3/0/0) | 35.1 KB (4/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R3 PK range, 10,000 rows, BETWEEN | 3.0 MB (40/23/4) | 3.4 MB (39/21/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| R4 PK range, 10,000 rows, >= AND < | 3.0 MB (40/23/4) | 3.4 MB (39/21/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| G1 GROUP BY Status (4 groups) | 26.7 KB (0/0/0) | 25.7 KB (0/0/0) | 720 B (0/0/0) | 1.9 KB (0/0/0) |
| G2 GROUP BY Bucket (1,000 groups) | 2.1 MB (2/0/0) | 2.1 MB (2/0/0) | 70.7 KB (0/0/0) | 142.0 KB (0/0/0) |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 928.7 KB (1/0/0) | 920.2 KB (1/0/0) | 5.2 KB (0/0/0) | 10.8 KB (0/0/0) |
| C1 COUNT(*) WHERE Amount range | 16.5 KB (0/0/0) | 15.5 KB (0/0/0) | 664 B (0/0/0) | 1.5 KB (0/0/0) |
| C1 + columnar index | 16.3 KB (0/0/0) | 15.3 KB (0/0/0) | - | - |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 16.9 KB (0/0/0) | 15.9 KB (0/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C2 + columnar index | 16.6 KB (0/0/0) | 15.6 KB (0/0/0) | - | - |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 16.9 KB (0/0/0) | 15.9 KB (0/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C3 + columnar index | 16.9 KB (0/0/0) | 15.7 KB (0/0/0) | - | - |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 24.3 KB (0/0/0) | 23.3 KB (0/0/0) | 608 B (0/0/0) | 2.1 KB (0/0/0) |
| C4 + columnar index | 23.8 KB (0/0/0) | 22.8 KB (0/0/0) | - | - |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 17.6 KB (0/0/0) | 16.6 KB (0/0/0) | 672 B (0/0/0) | 1.5 KB (0/0/0) |
| C5 + columnar index | 17.6 KB (0/0/0) | 16.6 KB (0/0/0) | - | - |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 17.5 KB (0/0/0) | 16.5 KB (0/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C6 + columnar index | 17.2 KB (0/0/0) | 16.2 KB (0/0/0) | - | - |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 2.1 MB (2/0/0) | 2.1 MB (2/1/0) | 70.7 KB (0/0/0) | 126.4 KB (0/0/0) |
| C7 + columnar index | 2.1 MB (2/0/0) | 2.1 MB (2/1/0) | - | - |
| C8 ORDER BY Amount DESC LIMIT 10 | 48.6 KB (0/0/0) | 47.6 KB (0/0/0) | 864 B (0/0/0) | 2.1 KB (0/0/0) |
| C8 + columnar index | 48.6 KB (0/0/0) | 47.6 KB (0/0/0) | - | - |

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
