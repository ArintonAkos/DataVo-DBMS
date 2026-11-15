## SQL capability matrix: 1,000,000 orders, 100,000 customers, 1,000 products

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-15 16:07 +02:00.

Cells are the median latency of one call (p99 in brackets). `✗` marks an answer that differs from the oracle; such a cell is not a result. The last column is SQLite's median divided by DataVo LSM's (above 1: DataVo LSM is faster).

| Setup | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| Load (ms) | 3,716 | 527 | 4,713 | 1,014 |
| Reopen (ms) | 499.4 | 0.0 | 2.2 | 240.8 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.0 |
| Create columnar index (ms) | 313 | 186 | - | - |

### Joins

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 5.28 s (5.99 s) | 5.34 s (6.47 s) | 720.19 ms (756.77 ms) | 2.14 ms (2.24 ms) | 0.14x |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 2.43 s (2.64 s) | 2.79 s (2.97 s) | 84.40 ms (85.50 ms) | 1.48 ms (1.55 ms) | 0.03x |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 2.29 s (2.47 s) | 2.55 s (2.87 s) | 83.94 ms (84.38 ms) | 1.92 ms (1.97 ms) | 0.04x |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 4.83 s (4.90 s) | 4.79 s (5.13 s) | 237.82 ms (253.71 ms) | 3.41 ms (3.54 ms) | 0.05x |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 4.39 s (5.08 s) | 4.29 s (4.59 s) | 36.49 ms (37.65 ms) | 3.24 ms (3.32 ms) | 0.01x |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 215.24 ms (236.44 ms) | 263.13 ms (300.97 ms) | 763.65 ms (779.20 ms) | 3.35 ms (3.53 ms) | 3.55x |
| J7 Small x large: Products (1 category, 2%) x Orders | 40.24 ms (40.35 ms) | 77.47 ms (77.72 ms) | 15.12 ms (15.54 ms) | 1.02 ms (1.09 ms) | 0.38x |

### Filters

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 315.8 µs (1.15 ms) | 189.4 µs (448.8 µs) | 817.2 µs (933.3 µs) | 1.20 ms (1.59 ms) | 2.59x |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 34.53 ms (36.81 ms) | 69.11 ms (71.60 ms) | 28.23 ms (30.49 ms) | 583.0 µs (792.9 µs) | 0.82x |
| W3 (Status = A OR Status = B) AND Amount range | 100.04 ms (103.76 ms) | 107.51 ms (110.50 ms) | 60.37 ms (61.26 ms) | 2.21 ms (2.46 ms) | 0.60x |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 112.14 ms (142.93 ms) | 110.28 ms (112.11 ms) | 62.83 ms (78.35 ms) | 2.23 ms (2.56 ms) | 0.56x |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 73.73 ms (74.64 ms) | 67.18 ms (67.75 ms) | 42.18 ms (42.53 ms) | 3.04 ms (3.52 ms) | 0.57x |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 77.67 ms (78.16 ms) | 76.06 ms (77.78 ms) | 51.39 ms (52.01 ms) | 4.01 ms (4.39 ms) | 0.66x |

### Primary-key ranges

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| R1 PK range, 100 rows, BETWEEN | 12.8 µs (17.5 µs) | 72.53 ms (84.24 ms) | 18.2 µs (19.6 µs) | 173.4 µs (333.4 µs) | 1.42x |
| R2 PK range, 100 rows, >= AND < | 12.5 µs (18.8 µs) | 70.88 ms (83.96 ms) | 18.1 µs (19.1 µs) | 179.5 µs (300.0 µs) | 1.45x |
| R3 PK range, 10,000 rows, BETWEEN | 1.09 ms (14.54 ms) | 72.51 ms (92.10 ms) | 1.75 ms (1.86 ms) | 651.2 µs (968.2 µs) | 1.61x |
| R4 PK range, 10,000 rows, >= AND < | 1.10 ms (14.30 ms) | 73.60 ms (86.63 ms) | 1.73 ms (1.95 ms) | 645.5 µs (951.7 µs) | 1.58x |

### GROUP BY cardinality

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| G1 GROUP BY Status (4 groups) | 91.52 ms (94.02 ms) | 102.99 ms (107.87 ms) | 248.37 ms (255.07 ms) | 1.17 ms (1.33 ms) | 2.71x |
| G2 GROUP BY Bucket (1,000 groups) | 101.38 ms (101.95 ms) | 112.54 ms (134.07 ms) | 212.66 ms (218.34 ms) | 788.6 µs (958.2 µs) | 2.10x |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 400.27 ms (434.01 ms) | 459.33 ms (530.56 ms) | 29.28 ms (29.87 ms) | 4.88 ms (5.65 ms) | 0.07x |

### Columnar index

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| C1 COUNT(*) WHERE Amount range | 66.70 ms (68.11 ms) | 90.13 ms (90.79 ms) | 42.90 ms (43.73 ms) | 1.30 ms (1.64 ms) | 0.64x |
| C1 + columnar index | 3.12 ms (3.21 ms) pushed | 3.19 ms (3.41 ms) pushed |  |  | 13.75x |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 77.55 ms (78.40 ms) | 80.73 ms (82.21 ms) | 36.23 ms (36.36 ms) | 579.2 µs (700.4 µs) | 0.47x |
| C2 + columnar index | 3.41 ms (3.54 ms) pushed | 3.62 ms (3.78 ms) pushed |  |  | 10.62x |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 76.97 ms (91.41 ms) | 85.58 ms (87.51 ms) | 39.06 ms (39.44 ms) | 582.0 µs (642.8 µs) | 0.51x |
| C3 + columnar index | 77.18 ms (78.11 ms) row scan | 3.01 ms (3.20 ms) pushed |  |  | 0.51x |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 97.69 ms (101.34 ms) | 95.01 ms (98.02 ms) | 57.65 ms (58.17 ms) | 1.34 ms (1.62 ms) | 0.59x |
| C4 + columnar index | 11.98 ms (12.36 ms) pushed | 11.79 ms (12.07 ms) pushed |  |  | 4.81x |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 68.42 ms (69.43 ms) | 74.91 ms (76.17 ms) | 42.13 ms (42.89 ms) | 1.01 ms (1.78 ms) | 0.62x |
| C5 + columnar index | 68.28 ms (69.61 ms) row scan | 75.44 ms (76.51 ms) row scan |  |  | 0.62x |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 70.07 ms (71.07 ms) | 85.85 ms (86.79 ms) | 35.04 ms (36.01 ms) | 1.62 ms (3.01 ms) | 0.50x |
| C6 + columnar index | 4.62 ms (4.99 ms) pushed | 485.0 µs (690.2 µs) pushed |  |  | 7.58x |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 100.28 ms (105.72 ms) | 105.41 ms (112.13 ms) | 203.98 ms (225.85 ms) | 702.3 µs (825.5 µs) | 2.03x |
| C7 + columnar index | 102.07 ms (117.57 ms) row scan | 105.70 ms (106.57 ms) row scan |  |  | 2.00x |
| C8 ORDER BY Amount DESC LIMIT 10 | 76.36 ms (80.09 ms) | 83.45 ms (84.13 ms) | 40.20 ms (40.39 ms) | 1.43 ms (1.50 ms) | 0.53x |
| C8 + columnar index | 79.64 ms (82.14 ms) row scan | 84.16 ms (84.47 ms) row scan |  |  | 0.50x |

### Managed allocation per call and GC collections during the timed calls

Bytes allocated on the calling thread per call (`GC.GetAllocatedBytesForCurrentThread`), with gen0/gen1/gen2 collections over all timed calls. SQLite's and DuckDB's native memory is not visible to this counter; their figures are only what the ADO.NET layer allocates.

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 3,686.9 MB (4054/1642/30) | 3,718.3 MB (4083/1531/30) | 432 B (0/0/0) | 1.3 KB (0/0/0) |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 2,144.3 MB (2371/882/28) | 2,175.7 MB (2394/960/24) | 696 B (0/0/0) | 1.7 KB (0/0/0) |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 2,147.2 MB (2379/893/31) | 2,178.9 MB (2398/1046/24) | 6.5 KB (0/0/0) | 10.3 KB (0/0/0) |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 3,543.8 MB (3967/1377/34) | 3,575.3 MB (3989/1435/28) | 456 B (0/0/0) | 1.5 KB (0/0/0) |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 3,283.4 MB (3636/1347/28) | 3,314.9 MB (3664/1439/28) | 408 B (0/0/0) | 1.1 KB (0/0/0) |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 66.8 MB (79/35/6) | 60.0 MB (67/29/2) | 1.0 KB (0/0/0) | 2.5 KB (0/0/0) |
| J7 Small x large: Products (1 category, 2%) x Orders | 495.9 KB (0/0/0) | 494.4 KB (0/0/0) | 560 B (0/0/0) | 1.5 KB (0/0/0) |
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 554.9 KB (67/24/0) | 312.6 KB (38/13/0) | 48.6 KB (5/0/0) | 97.5 KB (11/0/0) |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 318.2 KB (0/0/0) | 318.3 KB (0/0/0) | 48.7 KB (0/0/0) | 98.1 KB (0/0/0) |
| W3 (Status = A OR Status = B) AND Amount range | 37.6 MB (47/0/0) | 3.3 MB (4/0/0) | 688 B (0/0/0) | 1.7 KB (0/0/0) |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 35.5 MB (44/0/0) | 1.1 MB (1/0/0) | 816 B (0/0/0) | 1.9 KB (0/0/0) |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 45.8 MB (57/0/0) | 16.1 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 45.8 MB (57/0/0) | 37.3 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| R1 PK range, 100 rows, BETWEEN | 31.1 KB (3/0/0) | 31.2 KB (3/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R2 PK range, 100 rows, >= AND < | 31.1 KB (3/0/0) | 31.2 KB (3/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R3 PK range, 10,000 rows, BETWEEN | 3.0 MB (41/24/5) | 3.0 MB (38/20/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| R4 PK range, 10,000 rows, >= AND < | 3.0 MB (40/23/4) | 3.0 MB (38/20/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| G1 GROUP BY Status (4 groups) | 64.9 MB (81/0/0) | 30.5 MB (38/0/0) | 720 B (0/0/0) | 1.9 KB (0/0/0) |
| G2 GROUP BY Bucket (1,000 groups) | 56.0 MB (70/8/0) | 56.0 MB (70/7/0) | 70.7 KB (0/0/0) | 142.0 KB (0/0/0) |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 260.2 MB (327/148/16) | 261.3 MB (322/143/9) | 5.2 KB (0/0/0) | 10.8 KB (0/0/0) |
| C1 COUNT(*) WHERE Amount range | 4.6 MB (5/0/0) | 4.6 MB (5/0/0) | 664 B (0/0/0) | 1.5 KB (0/0/0) |
| C1 + columnar index | 15.1 KB (0/0/0) | 14.1 KB (0/0/0) | - | - |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 6.9 MB (8/0/0) | 6.9 MB (8/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C2 + columnar index | 15.0 KB (0/0/0) | 14.0 KB (0/0/0) | - | - |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 9.2 MB (11/0/0) | 9.2 MB (11/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C3 + columnar index | 9.2 MB (11/0/0) | 14.0 KB (0/0/0) | - | - |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 24.8 MB (31/0/0) | 24.8 MB (31/0/0) | 608 B (0/0/0) | 2.1 KB (0/0/0) |
| C4 + columnar index | 19.9 KB (0/0/0) | 18.9 KB (0/0/0) | - | - |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 36.9 MB (46/0/0) | 2.6 MB (3/0/0) | 672 B (0/0/0) | 1.5 KB (0/0/0) |
| C5 + columnar index | 36.9 MB (46/0/0) | 2.6 MB (3/0/0) | - | - |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 2.1 MB (2/0/0) | 2.1 MB (2/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C6 + columnar index | 16.0 KB (0/0/0) | 15.1 KB (0/0/0) | - | - |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 56.0 MB (70/8/0) | 56.0 MB (70/8/0) | 70.7 KB (0/0/0) | 126.4 KB (0/0/0) |
| C7 + columnar index | 56.0 MB (70/8/0) | 56.0 MB (70/8/0) | - | - |
| C8 ORDER BY Amount DESC LIMIT 10 | 84.0 MB (105/0/0) | 84.0 MB (105/0/0) | 864 B (0/0/0) | 2.1 KB (0/0/0) |
| C8 + columnar index | 84.0 MB (105/0/0) | 84.0 MB (105/0/0) | - | - |

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
