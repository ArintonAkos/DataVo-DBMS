## SQL capability matrix: 1,000,000 orders, 100,000 customers, 1,000 products

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-15 16:58 +02:00.

Cells are the median latency of one call (p99 in brackets). `✗` marks an answer that differs from the oracle; such a cell is not a result. The last column is SQLite's median divided by DataVo LSM's (above 1: DataVo LSM is faster).

| Setup | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| Load (ms) | 3,633 | 538 | 4,763 | 1,020 |
| Reopen (ms) | 520.3 | 0.0 | 2.4 | 237.8 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.0 |
| Create columnar index (ms) | 318 | 186 | - | - |

### Joins

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 181.44 ms (185.71 ms) | 165.77 ms (192.10 ms) | 728.81 ms (740.09 ms) | 2.06 ms (2.32 ms) | 4.02x |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 53.06 ms (57.17 ms) | 78.01 ms (83.64 ms) | 85.75 ms (88.36 ms) | 1.50 ms (1.58 ms) | 1.62x |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 53.39 ms (57.26 ms) | 78.32 ms (83.55 ms) | 85.65 ms (86.38 ms) | 1.96 ms (2.03 ms) | 1.60x |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 200.97 ms (210.92 ms) | 184.75 ms (203.23 ms) | 238.29 ms (239.12 ms) | 3.42 ms (3.54 ms) | 1.19x |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 165.49 ms (179.66 ms) | 166.97 ms (189.90 ms) | 36.67 ms (39.26 ms) | 3.32 ms (3.41 ms) | 0.22x |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 211.85 ms (215.61 ms) | 240.47 ms (244.41 ms) | 767.16 ms (770.21 ms) | 3.54 ms (3.63 ms) | 3.62x |
| J7 Small x large: Products (1 category, 2%) x Orders | 37.16 ms (37.59 ms) | 68.17 ms (68.67 ms) | 15.41 ms (15.86 ms) | 1.06 ms (1.10 ms) | 0.41x |

### Filters

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 303.5 µs (1.11 ms) | 190.4 µs (468.5 µs) | 824.0 µs (918.5 µs) | 1.12 ms (1.36 ms) | 2.72x |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 35.04 ms (37.44 ms) | 68.58 ms (71.31 ms) | 28.50 ms (31.06 ms) | 512.3 µs (608.8 µs) | 0.81x |
| W3 (Status = A OR Status = B) AND Amount range | 92.50 ms (96.71 ms) | 103.46 ms (106.57 ms) | 60.49 ms (61.48 ms) | 2.09 ms (2.16 ms) | 0.65x |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 102.89 ms (105.14 ms) | 103.86 ms (106.04 ms) | 62.83 ms (63.87 ms) | 2.09 ms (2.20 ms) | 0.61x |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 71.35 ms (71.72 ms) | 63.82 ms (64.30 ms) | 42.30 ms (42.61 ms) | 2.89 ms (2.99 ms) | 0.59x |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 76.62 ms (77.15 ms) | 73.56 ms (80.54 ms) | 51.20 ms (51.98 ms) | 4.11 ms (4.30 ms) | 0.67x |

### Primary-key ranges

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| R1 PK range, 100 rows, BETWEEN | 12.9 µs (18.7 µs) | 70.78 ms (74.19 ms) | 18.0 µs (20.0 µs) | 172.1 µs (215.7 µs) | 1.40x |
| R2 PK range, 100 rows, >= AND < | 12.2 µs (17.8 µs) | 69.31 ms (73.44 ms) | 18.1 µs (24.1 µs) | 172.5 µs (2.86 ms) | 1.48x |
| R3 PK range, 10,000 rows, BETWEEN | 1.04 ms (12.30 ms) | 69.83 ms (81.82 ms) | 1.72 ms (1.93 ms) | 600.7 µs (4.08 ms) | 1.65x |
| R4 PK range, 10,000 rows, >= AND < | 1.02 ms (12.03 ms) | 71.49 ms (81.04 ms) | 1.72 ms (1.86 ms) | 605.3 µs (4.01 ms) | 1.68x |

### GROUP BY cardinality

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| G1 GROUP BY Status (4 groups) | 89.31 ms (90.26 ms) | 99.22 ms (100.18 ms) | 236.39 ms (238.68 ms) | 1.13 ms (1.21 ms) | 2.65x |
| G2 GROUP BY Bucket (1,000 groups) | 89.81 ms (91.37 ms) | 105.17 ms (106.90 ms) | 209.82 ms (211.15 ms) | 727.4 µs (792.9 µs) | 2.34x |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 371.18 ms (424.42 ms) | 418.28 ms (442.58 ms) | 29.67 ms (30.05 ms) | 4.43 ms (4.70 ms) | 0.08x |

### Columnar index

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| C1 COUNT(*) WHERE Amount range | 68.72 ms (69.81 ms) | 89.66 ms (91.99 ms) | 43.58 ms (44.50 ms) | 1.16 ms (1.27 ms) | 0.63x |
| C1 + columnar index | 3.15 ms (3.24 ms) pushed | 3.23 ms (3.40 ms) pushed |  |  | 13.82x |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 76.30 ms (78.11 ms) | 83.51 ms (84.42 ms) | 36.70 ms (37.46 ms) | 421.6 µs (505.9 µs) | 0.48x |
| C2 + columnar index | 3.41 ms (3.54 ms) pushed | 3.59 ms (3.73 ms) pushed |  |  | 10.75x |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 77.40 ms (78.89 ms) | 87.45 ms (88.65 ms) | 39.69 ms (39.99 ms) | 486.1 µs (559.2 µs) | 0.51x |
| C3 + columnar index | 76.43 ms (77.70 ms) row scan | 3.00 ms (3.28 ms) pushed |  |  | 0.52x |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 97.37 ms (99.72 ms) | 100.65 ms (102.76 ms) | 57.96 ms (58.43 ms) | 1.41 ms (1.45 ms) | 0.60x |
| C4 + columnar index | 11.42 ms (11.43 ms) pushed | 11.84 ms (12.21 ms) pushed |  |  | 5.08x |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 62.27 ms (63.61 ms) | 72.09 ms (73.19 ms) | 42.45 ms (42.69 ms) | 1.02 ms (1.15 ms) | 0.68x |
| C5 + columnar index | 62.21 ms (63.06 ms) row scan | 72.36 ms (73.48 ms) row scan |  |  | 0.68x |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 71.17 ms (72.87 ms) | 89.56 ms (90.48 ms) | 35.12 ms (35.44 ms) | 1.66 ms (1.75 ms) | 0.49x |
| C6 + columnar index | 4.58 ms (4.72 ms) pushed | 495.0 µs (664.2 µs) pushed |  |  | 7.66x |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 89.19 ms (89.90 ms) | 105.03 ms (106.38 ms) | 200.38 ms (201.33 ms) | 705.9 µs (755.4 µs) | 2.25x |
| C7 + columnar index | 88.86 ms (91.54 ms) row scan | 105.74 ms (106.16 ms) row scan |  |  | 2.25x |
| C8 ORDER BY Amount DESC LIMIT 10 | 63.06 ms (64.40 ms) | 81.77 ms (82.77 ms) | 40.62 ms (41.70 ms) | 1.27 ms (1.32 ms) | 0.64x |
| C8 + columnar index | 62.68 ms (63.24 ms) row scan | 81.93 ms (82.62 ms) row scan |  |  | 0.65x |

### Managed allocation per call and GC collections during the timed calls

Bytes allocated on the calling thread per call (`GC.GetAllocatedBytesForCurrentThread`), with gen0/gen1/gen2 collections over all timed calls. SQLite's and DuckDB's native memory is not visible to this counter; their figures are only what the ADO.NET layer allocates.

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 41.2 MB (47/26/6) | 34.4 MB (35/19/2) | 432 B (0/0/0) | 1.3 KB (0/0/0) |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 11.0 MB (12/4/0) | 4.2 MB (4/2/0) | 696 B (0/0/0) | 1.7 KB (0/0/0) |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 11.5 MB (13/4/0) | 4.6 MB (4/2/0) | 6.5 KB (0/0/0) | 10.3 KB (0/0/0) |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 40.8 MB (47/26/6) | 34.0 MB (34/18/2) | 456 B (0/0/0) | 1.5 KB (0/0/0) |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 40.8 MB (47/26/6) | 34.0 MB (34/18/2) | 408 B (0/0/0) | 1.1 KB (0/0/0) |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 63.7 MB (76/36/6) | 56.9 MB (62/27/1) | 1.0 KB (0/0/0) | 2.5 KB (0/0/0) |
| J7 Small x large: Products (1 category, 2%) x Orders | 58.2 KB (0/0/0) | 26.0 KB (0/0/0) | 560 B (0/0/0) | 1.5 KB (0/0/0) |
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
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 260.2 MB (328/148/16) | 261.3 MB (322/143/9) | 5.2 KB (0/0/0) | 10.8 KB (0/0/0) |
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
