## SQL capability matrix: 1,000,000 orders, 100,000 customers, 1,000 products

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-16 00:08 +02:00.

Cells are the median latency of one call (p99 in brackets). `✗` marks an answer that differs from the oracle; such a cell is not a result. The last column is SQLite's median divided by DataVo LSM's (above 1: DataVo LSM is faster).

| Setup | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| Load (ms) | 3,613 | 542 | 4,764 | 1,025 |
| Reopen (ms) | 509.9 | 0.0 | 2.3 | 239.6 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.3 |
| Create columnar index (ms) | 317 | 188 | - | - |

### Joins

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 84.54 ms (92.64 ms) | 98.46 ms (107.14 ms) | 738.31 ms (747.49 ms) | 2.08 ms (2.26 ms) | 8.73x |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 22.03 ms (25.93 ms) | 11.11 ms (11.50 ms) | 86.10 ms (87.63 ms) | 1.49 ms (1.68 ms) | 3.91x |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 18.65 ms (22.91 ms) | 11.75 ms (14.67 ms) | 86.68 ms (90.81 ms) | 1.93 ms (2.04 ms) | 4.65x |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 83.47 ms (86.09 ms) | 98.26 ms (98.55 ms) | 238.24 ms (239.82 ms) | 3.40 ms (3.56 ms) | 2.85x |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 56.36 ms (57.06 ms) | 89.20 ms (89.80 ms) | 36.50 ms (36.76 ms) | 3.30 ms (3.47 ms) | 0.65x |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 154.20 ms (171.65 ms) | 207.46 ms (226.28 ms) | 773.22 ms (780.81 ms) | 3.37 ms (3.58 ms) | 5.01x |
| J7 Small x large: Products (1 category, 2%) x Orders | 7.03 ms (8.05 ms) | 6.10 ms (7.19 ms) | 15.51 ms (15.91 ms) | 1.04 ms (1.11 ms) | 2.21x |

### Filters

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 357.7 µs (684.1 µs) | 181.8 µs (461.8 µs) | 821.5 µs (1.01 ms) | 1.13 ms (1.34 ms) | 2.30x |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 35.43 ms (38.17 ms) | 68.59 ms (71.07 ms) | 28.59 ms (30.58 ms) | 514.4 µs (601.5 µs) | 0.81x |
| W3 (Status = A OR Status = B) AND Amount range | 94.37 ms (97.62 ms) | 103.04 ms (103.86 ms) | 61.71 ms (62.31 ms) | 2.12 ms (7.51 ms) | 0.65x |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 104.92 ms (106.23 ms) | 104.75 ms (105.72 ms) | 64.17 ms (65.29 ms) | 2.12 ms (2.18 ms) | 0.61x |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 72.05 ms (72.80 ms) | 63.99 ms (64.61 ms) | 42.74 ms (42.87 ms) | 2.85 ms (5.85 ms) | 0.59x |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 76.41 ms (76.86 ms) | 73.94 ms (74.78 ms) | 51.51 ms (51.91 ms) | 4.11 ms (4.28 ms) | 0.67x |

### Primary-key ranges

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| R1 PK range, 100 rows, BETWEEN | 12.8 µs (22.0 µs) | 21.7 µs (28.1 µs) | 18.4 µs (26.7 µs) | 170.5 µs (217.1 µs) | 1.44x |
| R2 PK range, 100 rows, >= AND < | 12.5 µs (16.6 µs) | 22.0 µs (30.8 µs) | 18.5 µs (24.4 µs) | 170.8 µs (208.9 µs) | 1.48x |
| R3 PK range, 10,000 rows, BETWEEN | 1.07 ms (12.42 ms) | 2.06 ms (14.88 ms) | 1.72 ms (1.91 ms) | 591.1 µs (763.6 µs) | 1.62x |
| R4 PK range, 10,000 rows, >= AND < | 1.04 ms (12.08 ms) | 1.99 ms (15.87 ms) | 1.72 ms (1.86 ms) | 596.1 µs (2.31 ms) | 1.66x |

### GROUP BY cardinality

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| G1 GROUP BY Status (4 groups) | 81.67 ms (82.60 ms) | 94.66 ms (97.27 ms) | 237.68 ms (239.62 ms) | 1.11 ms (1.19 ms) | 2.91x |
| G2 GROUP BY Bucket (1,000 groups) | 66.29 ms (69.65 ms) | 80.49 ms (86.73 ms) | 210.06 ms (215.23 ms) | 733.5 µs (819.0 µs) | 3.17x |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 67.37 ms (73.08 ms) | 103.80 ms (107.28 ms) | 29.75 ms (30.45 ms) | 4.48 ms (4.86 ms) | 0.44x |

### Columnar index

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| C1 COUNT(*) WHERE Amount range | 69.27 ms (70.17 ms) | 89.09 ms (91.04 ms) | 43.47 ms (44.28 ms) | 1.15 ms (1.19 ms) | 0.63x |
| C1 + columnar index | 3.17 ms (3.25 ms) pushed | 3.25 ms (3.45 ms) pushed |  |  | 13.72x |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 79.53 ms (82.87 ms) | 83.94 ms (84.99 ms) | 37.02 ms (37.26 ms) | 426.0 µs (463.6 µs) | 0.47x |
| C2 + columnar index | 3.43 ms (3.56 ms) pushed | 3.62 ms (3.82 ms) pushed |  |  | 10.79x |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 79.90 ms (83.32 ms) | 91.15 ms (93.71 ms) | 39.95 ms (40.26 ms) | 481.6 µs (542.5 µs) | 0.50x |
| C3 + columnar index | 78.83 ms (80.71 ms) row scan | 3.03 ms (3.27 ms) pushed |  |  | 0.51x |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 104.21 ms (105.59 ms) | 104.42 ms (107.78 ms) | 57.75 ms (58.42 ms) | 1.45 ms (1.48 ms) | 0.55x |
| C4 + columnar index | 11.97 ms (12.08 ms) pushed | 11.90 ms (11.99 ms) pushed |  |  | 4.82x |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 63.52 ms (63.95 ms) | 73.16 ms (74.42 ms) | 42.41 ms (43.62 ms) | 934.5 µs (1.14 ms) | 0.67x |
| C5 + columnar index | 63.41 ms (66.97 ms) row scan | 72.80 ms (73.69 ms) row scan |  |  | 0.67x |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 71.85 ms (73.86 ms) | 88.59 ms (90.71 ms) | 35.52 ms (36.05 ms) | 1.44 ms (1.78 ms) | 0.49x |
| C6 + columnar index | 4.63 ms (4.76 ms) pushed | 490.0 µs (659.0 µs) pushed |  |  | 7.68x |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 65.69 ms (66.76 ms) | 81.24 ms (81.73 ms) | 201.42 ms (230.94 ms) | 720.8 µs (818.8 µs) | 3.07x |
| C7 + columnar index | 66.93 ms (70.52 ms) row scan | 81.40 ms (82.03 ms) row scan |  |  | 3.01x |
| C8 ORDER BY Amount DESC LIMIT 10 | 66.15 ms (67.01 ms) | 83.36 ms (84.44 ms) | 40.66 ms (40.93 ms) | 1.28 ms (1.31 ms) | 0.61x |
| C8 + columnar index | 66.28 ms (67.30 ms) row scan | 83.40 ms (88.16 ms) row scan |  |  | 0.61x |

### Managed allocation per call and GC collections during the timed calls

Bytes allocated on the calling thread per call (`GC.GetAllocatedBytesForCurrentThread`), with gen0/gen1/gen2 collections over all timed calls. SQLite's and DuckDB's native memory is not visible to this counter; their figures are only what the ADO.NET layer allocates.

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 3.0 MB (0/0/0) | 3.0 MB (0/0/0) | 432 B (0/0/0) | 1.3 KB (0/0/0) |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 5.0 MB (4/0/0) | 1.9 MB (0/0/0) | 696 B (0/0/0) | 1.7 KB (0/0/0) |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 11.6 MB (12/3/0) | 4.8 MB (3/1/0) | 6.5 KB (0/0/0) | 10.3 KB (0/0/0) |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 2.9 MB (0/0/0) | 2.9 MB (0/0/0) | 456 B (0/0/0) | 1.5 KB (0/0/0) |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 2.9 MB (0/0/0) | 2.9 MB (0/0/0) | 408 B (0/0/0) | 1.1 KB (0/0/0) |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 24.4 MB (28/15/3) | 21.3 MB (23/12/1) | 1.0 KB (0/0/0) | 2.5 KB (0/0/0) |
| J7 Small x large: Products (1 category, 2%) x Orders | 30.2 KB (0/0/0) | 22.0 KB (0/0/0) | 560 B (0/0/0) | 1.5 KB (0/0/0) |
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 329.1 KB (40/14/0) | 312.7 KB (38/13/0) | 48.6 KB (5/0/0) | 97.5 KB (11/0/0) |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 318.2 KB (0/0/0) | 318.3 KB (0/0/0) | 48.7 KB (0/0/0) | 98.1 KB (0/0/0) |
| W3 (Status = A OR Status = B) AND Amount range | 37.6 MB (47/0/0) | 3.3 MB (4/0/0) | 688 B (0/0/0) | 1.7 KB (0/0/0) |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 35.5 MB (44/0/0) | 1.1 MB (1/0/0) | 816 B (0/0/0) | 1.9 KB (0/0/0) |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 45.8 MB (57/0/0) | 16.8 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 45.8 MB (57/0/0) | 38.0 KB (0/0/0) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| R1 PK range, 100 rows, BETWEEN | 31.1 KB (3/0/0) | 35.1 KB (4/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R2 PK range, 100 rows, >= AND < | 31.1 KB (3/0/0) | 35.1 KB (4/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R3 PK range, 10,000 rows, BETWEEN | 3.0 MB (40/23/4) | 3.4 MB (39/21/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| R4 PK range, 10,000 rows, >= AND < | 3.0 MB (40/23/4) | 3.4 MB (39/21/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| G1 GROUP BY Status (4 groups) | 34.4 MB (43/0/0) | 24.3 KB (0/0/0) | 720 B (0/0/0) | 1.9 KB (0/0/0) |
| G2 GROUP BY Bucket (1,000 groups) | 2.1 MB (2/0/0) | 2.1 MB (2/0/0) | 70.7 KB (0/0/0) | 142.0 KB (0/0/0) |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 15.9 MB (8/3/0) | 16.0 MB (8/3/0) | 5.2 KB (0/0/0) | 10.8 KB (0/0/0) |
| C1 COUNT(*) WHERE Amount range | 4.6 MB (5/0/0) | 4.6 MB (5/0/0) | 664 B (0/0/0) | 1.5 KB (0/0/0) |
| C1 + columnar index | 15.9 KB (0/0/0) | 14.9 KB (0/0/0) | - | - |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 6.9 MB (8/0/0) | 6.9 MB (8/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C2 + columnar index | 16.2 KB (0/0/0) | 15.2 KB (0/0/0) | - | - |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 9.2 MB (11/0/0) | 9.2 MB (11/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C3 + columnar index | 9.2 MB (11/0/0) | 15.3 KB (0/0/0) | - | - |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 24.8 MB (31/0/0) | 24.8 MB (31/0/0) | 608 B (0/0/0) | 2.1 KB (0/0/0) |
| C4 + columnar index | 23.4 KB (0/0/0) | 22.4 KB (0/0/0) | - | - |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 36.9 MB (46/0/0) | 2.6 MB (3/0/0) | 672 B (0/0/0) | 1.5 KB (0/0/0) |
| C5 + columnar index | 36.9 MB (46/0/0) | 2.6 MB (3/0/0) | - | - |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 2.1 MB (2/0/0) | 2.1 MB (2/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C6 + columnar index | 16.8 KB (0/0/0) | 15.8 KB (0/0/0) | - | - |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 2.1 MB (2/1/0) | 2.1 MB (2/1/0) | 70.7 KB (0/0/0) | 126.4 KB (0/0/0) |
| C7 + columnar index | 2.1 MB (2/0/0) | 2.1 MB (2/1/0) | - | - |
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
