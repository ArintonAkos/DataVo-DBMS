## SQL capability matrix: 1,000,000 orders, 100,000 customers, 1,000 products

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-15 22:36 +02:00.

Cells are the median latency of one call (p99 in brackets). `✗` marks an answer that differs from the oracle; such a cell is not a result. The last column is SQLite's median divided by DataVo LSM's (above 1: DataVo LSM is faster).

| Setup | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| Load (ms) | 3,727 | 539 | 4,982 | 1,023 |
| Reopen (ms) | 401.2 | 0.0 | 2.3 | 243.8 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.3 |
| Create columnar index (ms) | 311 | 190 | - | - |

### Joins

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 89.99 ms (99.49 ms) | 99.94 ms (102.80 ms) | 750.70 ms (767.66 ms) | 2.11 ms (2.22 ms) | 8.34x |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 23.15 ms (25.71 ms) | 10.93 ms (11.55 ms) | 88.60 ms (91.40 ms) | 1.52 ms (1.67 ms) | 3.83x |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 22.31 ms (23.41 ms) | 11.82 ms (15.11 ms) | 89.57 ms (95.20 ms) | 1.96 ms (2.12 ms) | 4.01x |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 84.53 ms (101.73 ms) | 100.04 ms (101.45 ms) | 243.21 ms (248.45 ms) | 3.40 ms (3.47 ms) | 2.88x |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 56.59 ms (58.22 ms) | 90.03 ms (91.31 ms) | 36.95 ms (37.46 ms) | 3.29 ms (3.46 ms) | 0.65x |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 185.43 ms (238.34 ms) | 245.20 ms (301.23 ms) | 815.05 ms (850.40 ms) | 3.42 ms (3.67 ms) | 4.40x |
| J7 Small x large: Products (1 category, 2%) x Orders | 9.22 ms (12.39 ms) | 7.82 ms (14.36 ms) | 15.70 ms (15.91 ms) | 1.05 ms (1.13 ms) | 1.70x |

### Filters

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 354.7 µs (706.5 µs) | 182.0 µs (528.6 µs) | 833.6 µs (1.04 ms) | 1.12 ms (1.30 ms) | 2.35x |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 35.27 ms (37.91 ms) | 69.07 ms (72.47 ms) | 28.39 ms (30.93 ms) | 552.4 µs (683.0 µs) | 0.80x |
| W3 (Status = A OR Status = B) AND Amount range | 93.24 ms (97.14 ms) | 103.30 ms (106.04 ms) | 60.83 ms (62.07 ms) | 2.10 ms (2.16 ms) | 0.65x |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 103.10 ms (105.59 ms) | 104.26 ms (107.27 ms) | 63.05 ms (64.32 ms) | 2.07 ms (2.27 ms) | 0.61x |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 73.23 ms (73.58 ms) | 65.25 ms (66.20 ms) | 42.25 ms (43.30 ms) | 2.90 ms (3.77 ms) | 0.58x |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 78.53 ms (79.17 ms) | 74.62 ms (75.67 ms) | 51.45 ms (51.83 ms) | 4.21 ms (4.27 ms) | 0.66x |

### Primary-key ranges

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| R1 PK range, 100 rows, BETWEEN | 12.9 µs (20.4 µs) | 71.11 ms (78.29 ms) | 18.4 µs (25.5 µs) | 168.2 µs (304.0 µs) | 1.43x |
| R2 PK range, 100 rows, >= AND < | 12.3 µs (14.8 µs) | 69.79 ms (77.78 ms) | 18.6 µs (24.4 µs) | 168.8 µs (214.9 µs) | 1.51x |
| R3 PK range, 10,000 rows, BETWEEN | 1.06 ms (11.69 ms) | 69.73 ms (80.66 ms) | 1.76 ms (1.97 ms) | 601.8 µs (820.6 µs) | 1.65x |
| R4 PK range, 10,000 rows, >= AND < | 1.06 ms (11.26 ms) | 70.69 ms (81.76 ms) | 1.75 ms (1.87 ms) | 595.8 µs (789.9 µs) | 1.64x |

### GROUP BY cardinality

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| G1 GROUP BY Status (4 groups) | 95.78 ms (105.82 ms) | 98.96 ms (100.16 ms) | 238.01 ms (242.29 ms) | 1.12 ms (1.21 ms) | 2.49x |
| G2 GROUP BY Bucket (1,000 groups) | 98.61 ms (100.11 ms) | 110.62 ms (120.93 ms) | 209.17 ms (213.52 ms) | 762.3 µs (796.0 µs) | 2.12x |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 389.39 ms (406.86 ms) | 427.91 ms (476.68 ms) | 30.21 ms (30.52 ms) | 4.40 ms (4.82 ms) | 0.08x |

### Columnar index

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| C1 COUNT(*) WHERE Amount range | 66.80 ms (69.84 ms) | 87.61 ms (90.38 ms) | 43.43 ms (43.94 ms) | 1.14 ms (1.21 ms) | 0.65x |
| C1 + columnar index | 3.17 ms (3.32 ms) pushed | 3.23 ms (3.39 ms) pushed |  |  | 13.71x |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 79.49 ms (93.25 ms) | 81.04 ms (82.47 ms) | 36.22 ms (36.85 ms) | 420.2 µs (510.0 µs) | 0.46x |
| C2 + columnar index | 3.49 ms (3.59 ms) pushed | 3.62 ms (3.82 ms) pushed |  |  | 10.39x |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 76.79 ms (81.04 ms) | 86.37 ms (89.07 ms) | 39.57 ms (39.94 ms) | 475.0 µs (556.9 µs) | 0.52x |
| C3 + columnar index | 76.47 ms (78.26 ms) row scan | 3.06 ms (3.23 ms) pushed |  |  | 0.52x |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 98.37 ms (103.45 ms) | 95.79 ms (98.82 ms) | 57.70 ms (58.53 ms) | 1.41 ms (1.50 ms) | 0.59x |
| C4 + columnar index | 11.22 ms (11.58 ms) pushed | 11.76 ms (12.12 ms) pushed |  |  | 5.14x |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 62.14 ms (63.04 ms) | 71.74 ms (72.70 ms) | 42.36 ms (43.14 ms) | 1.03 ms (1.08 ms) | 0.68x |
| C5 + columnar index | 62.47 ms (74.76 ms) row scan | 72.17 ms (73.35 ms) row scan |  |  | 0.68x |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 72.60 ms (77.62 ms) | 90.07 ms (92.43 ms) | 35.30 ms (36.30 ms) | 1.64 ms (1.83 ms) | 0.49x |
| C6 + columnar index | 4.70 ms (4.89 ms) pushed | 491.0 µs (729.9 µs) pushed |  |  | 7.51x |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 96.88 ms (98.69 ms) | 109.23 ms (111.35 ms) | 200.74 ms (202.06 ms) | 714.8 µs (749.2 µs) | 2.07x |
| C7 + columnar index | 96.65 ms (98.64 ms) row scan | 109.12 ms (110.81 ms) row scan |  |  | 2.08x |
| C8 ORDER BY Amount DESC LIMIT 10 | 78.39 ms (78.95 ms) | 81.71 ms (82.18 ms) | 40.60 ms (42.13 ms) | 1.26 ms (1.45 ms) | 0.52x |
| C8 + columnar index | 78.37 ms (81.50 ms) row scan | 81.37 ms (82.04 ms) row scan |  |  | 0.52x |

### Managed allocation per call and GC collections during the timed calls

Bytes allocated on the calling thread per call (`GC.GetAllocatedBytesForCurrentThread`), with gen0/gen1/gen2 collections over all timed calls. SQLite's and DuckDB's native memory is not visible to this counter; their figures are only what the ADO.NET layer allocates.

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 6.2 MB (4/1/0) | 3.0 MB (0/0/0) | 432 B (0/0/0) | 1.3 KB (0/0/0) |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 9.9 MB (10/3/0) | 1.2 MB (0/0/0) | 696 B (0/0/0) | 1.7 KB (0/0/0) |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 16.6 MB (18/6/0) | 4.1 MB (3/1/0) | 6.5 KB (0/0/0) | 10.3 KB (0/0/0) |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 2.9 MB (0/0/0) | 2.9 MB (0/0/0) | 456 B (0/0/0) | 1.5 KB (0/0/0) |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 2.9 MB (0/0/0) | 2.9 MB (0/0/0) | 408 B (0/0/0) | 1.1 KB (0/0/0) |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 47.3 MB (58/24/4) | 44.2 MB (51/21/1) | 1.0 KB (0/0/0) | 2.5 KB (0/0/0) |
| J7 Small x large: Products (1 category, 2%) x Orders | 6.2 MB (7/2/0) | 334.1 KB (0/0/0) | 560 B (0/0/0) | 1.5 KB (0/0/0) |
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 554.9 KB (67/24/0) | 312.6 KB (38/13/0) | 48.6 KB (5/0/0) | 97.5 KB (11/0/0) |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 318.2 KB (0/0/0) | 318.3 KB (0/0/0) | 48.7 KB (0/0/0) | 98.1 KB (0/0/0) |
| W3 (Status = A OR Status = B) AND Amount range | 37.6 MB (47/0/0) | 3.3 MB (4/0/0) | 688 B (0/0/0) | 1.7 KB (0/0/0) |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 35.5 MB (44/0/0) | 1.1 MB (1/0/0) | 816 B (0/0/0) | 1.9 KB (0/0/0) |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 45.8 MB (57/0/0) | 16.1 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 45.8 MB (57/0/0) | 37.3 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| R1 PK range, 100 rows, BETWEEN | 31.1 KB (3/0/0) | 31.2 KB (3/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R2 PK range, 100 rows, >= AND < | 31.1 KB (3/0/0) | 31.2 KB (3/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R3 PK range, 10,000 rows, BETWEEN | 3.0 MB (40/23/4) | 3.0 MB (38/20/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| R4 PK range, 10,000 rows, >= AND < | 3.0 MB (40/23/4) | 3.0 MB (38/20/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| G1 GROUP BY Status (4 groups) | 64.9 MB (81/0/0) | 30.5 MB (38/0/0) | 720 B (0/0/0) | 1.9 KB (0/0/0) |
| G2 GROUP BY Bucket (1,000 groups) | 56.0 MB (70/8/0) | 56.0 MB (70/7/0) | 70.7 KB (0/0/0) | 142.0 KB (0/0/0) |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 260.2 MB (330/148/18) | 261.3 MB (322/143/9) | 5.2 KB (0/0/0) | 10.8 KB (0/0/0) |
| C1 COUNT(*) WHERE Amount range | 4.6 MB (5/0/0) | 4.6 MB (5/0/0) | 664 B (0/0/0) | 1.5 KB (0/0/0) |
| C1 + columnar index | 15.1 KB (0/0/0) | 14.1 KB (0/0/0) | - | - |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 6.9 MB (8/0/0) | 6.9 MB (8/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C2 + columnar index | 15.0 KB (0/0/0) | 14.0 KB (0/0/0) | - | - |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 9.2 MB (11/0/0) | 9.2 MB (11/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C3 + columnar index | 9.2 MB (11/0/0) | 14.1 KB (0/0/0) | - | - |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 24.8 MB (31/0/0) | 24.8 MB (31/0/0) | 608 B (0/0/0) | 2.1 KB (0/0/0) |
| C4 + columnar index | 19.9 KB (0/0/0) | 18.9 KB (0/0/0) | - | - |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 36.9 MB (46/0/0) | 2.6 MB (3/0/0) | 672 B (0/0/0) | 1.5 KB (0/0/0) |
| C5 + columnar index | 36.9 MB (46/0/0) | 2.6 MB (3/0/0) | - | - |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 2.1 MB (2/0/0) | 2.1 MB (2/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C6 + columnar index | 16.1 KB (0/0/0) | 15.1 KB (0/0/0) | - | - |
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
