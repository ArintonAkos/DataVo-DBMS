## SQL capability matrix: 1,000,000 orders, 100,000 customers, 1,000 products

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-14 17:08 +02:00.

Cells are the median latency of one call (p99 in brackets). `✗` marks an answer that differs from the oracle; such a cell is not a result. The last column is SQLite's median divided by DataVo LSM's (above 1: DataVo LSM is faster).

| Setup | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| Load (ms) | 3,671 | 545 | 4,892 | 1,021 |
| Reopen (ms) | 454.9 | 0.0 | 2.0 | 233.1 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.0 |
| Create columnar index (ms) | 316 | 186 | - | - |

### Joins

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 5.03 s (5.49 s) | 5.17 s (5.73 s) | 724.86 ms (727.99 ms) | 2.09 ms (2.22 ms) | 0.14x |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 2.34 s (2.44 s) | 2.60 s (2.81 s) | 85.29 ms (85.65 ms) | 1.50 ms (1.56 ms) | 0.04x |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 2.20 s (2.36 s) | 2.52 s (2.71 s) | 85.57 ms (88.63 ms) | 1.95 ms (2.05 ms) | 0.04x |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 4.52 s (4.87 s) | 4.34 s (4.40 s) | 239.39 ms (240.39 ms) | 3.48 ms (3.53 ms) | 0.05x |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 4.00 s (4.84 s) | 4.14 s (4.53 s) | 36.76 ms (37.74 ms) | 3.25 ms (3.28 ms) | 0.01x |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 222.52 ms (227.49 ms) | 258.32 ms (293.48 ms) | 764.90 ms (770.24 ms) | 3.35 ms (3.57 ms) | 3.44x |
| J7 Small x large: Products (1 category, 2%) x Orders | 40.56 ms (40.99 ms) | 75.66 ms (75.97 ms) | 15.35 ms (15.59 ms) | 1.07 ms (1.11 ms) | 0.38x |

### Filters

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 303.2 µs (1.10 ms) | 185.3 µs (451.0 µs) | 819.7 µs (937.6 µs) | 1.12 ms (1.31 ms) | 2.70x |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 35.07 ms (37.06 ms) | 68.25 ms (70.74 ms) | 28.70 ms (30.63 ms) | 522.3 µs (617.5 µs) | 0.82x |
| W3 (Status = A OR Status = B) AND Amount range | 3.40 s (3.54 s) | 3.46 s (3.59 s) | 61.26 ms (62.07 ms) | 2.05 ms (2.18 ms) | 0.02x |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 4.35 s (4.82 s) | 4.30 s (5.09 s) | 63.29 ms (63.78 ms) | 2.08 ms (7.52 ms) | 0.01x |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 1.66 s (1.77 s) | 1.92 s (2.07 s) | 42.13 ms (43.51 ms) | 2.96 ms (3.12 ms) | 0.03x |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 1.66 s (1.91 s) | 1.94 s (2.19 s) | 50.91 ms (51.39 ms) | 4.20 ms (4.25 ms) | 0.03x |

### Primary-key ranges

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| R1 PK range, 100 rows, BETWEEN | 12.8 µs (17.2 µs) | 70.66 ms (73.51 ms) | 18.3 µs (21.2 µs) | 169.6 µs (540.2 µs) | 1.43x |
| R2 PK range, 100 rows, >= AND < | 12.5 µs (16.5 µs) | 69.10 ms (73.25 ms) | 18.4 µs (25.0 µs) | 173.1 µs (268.5 µs) | 1.47x |
| R3 PK range, 10,000 rows, BETWEEN | 1.05 ms (12.32 ms) | 69.75 ms (81.86 ms) | 1.74 ms (1.93 ms) | 600.5 µs (4.00 ms) | 1.66x |
| R4 PK range, 10,000 rows, >= AND < | 1.05 ms (11.95 ms) | 70.79 ms (81.31 ms) | 1.73 ms (1.93 ms) | 607.6 µs (765.5 µs) | 1.64x |

### GROUP BY cardinality

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| G1 GROUP BY Status (4 groups) | 89.66 ms (91.28 ms) | 96.38 ms (96.93 ms) | 237.83 ms (239.37 ms) | 1.12 ms (1.16 ms) | 2.65x |
| G2 GROUP BY Bucket (1,000 groups) | 96.88 ms (98.27 ms) | 106.39 ms (110.16 ms) | 209.11 ms (211.91 ms) | 731.1 µs (784.2 µs) | 2.16x |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 391.03 ms (454.18 ms) | 422.69 ms (462.38 ms) | 29.72 ms (30.36 ms) | 4.40 ms (4.59 ms) | 0.08x |

### Columnar index

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| C1 COUNT(*) WHERE Amount range | 65.80 ms (67.41 ms) | 85.26 ms (86.66 ms) | 43.64 ms (44.24 ms) | 1.09 ms (1.18 ms) | 0.66x |
| C1 + columnar index | 3.15 ms (3.27 ms) pushed | 3.33 ms (3.46 ms) pushed |  |  | 13.85x |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 77.51 ms (81.57 ms) | 79.56 ms (85.51 ms) | 37.02 ms (37.17 ms) | 432.5 µs (487.8 µs) | 0.48x |
| C2 + columnar index | 3.44 ms (3.58 ms) pushed | 3.70 ms (3.90 ms) pushed |  |  | 10.77x |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 76.88 ms (80.89 ms) | 84.71 ms (89.39 ms) | 39.81 ms (40.09 ms) | 496.2 µs (535.0 µs) | 0.52x |
| C3 + columnar index | 77.56 ms (78.53 ms) row scan | 3.12 ms (3.41 ms) pushed |  |  | 0.51x |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 99.23 ms (104.96 ms) | 95.76 ms (101.36 ms) | 57.68 ms (59.12 ms) | 1.39 ms (1.45 ms) | 0.58x |
| C4 + columnar index | 11.50 ms (11.64 ms) pushed | 11.99 ms (12.33 ms) pushed |  |  | 5.02x |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 67.82 ms (73.08 ms) | 73.86 ms (79.59 ms) | 42.61 ms (43.73 ms) | 910.9 µs (1.10 ms) | 0.63x |
| C5 + columnar index | 67.82 ms (69.27 ms) row scan | 73.96 ms (75.37 ms) row scan |  |  | 0.63x |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 2.09 s (2.56 s) | 2.32 s (2.59 s) | 35.44 ms (35.76 ms) | 1.63 ms (2.14 ms) | 0.02x |
| C6 + columnar index | 2.25 s (2.37 s) row scan | 2.32 s (2.57 s) row scan |  |  | 0.02x |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 95.89 ms (96.56 ms) | 103.15 ms (105.23 ms) | 200.09 ms (201.04 ms) | 733.9 µs (802.4 µs) | 2.09x |
| C7 + columnar index | 95.91 ms (96.47 ms) row scan | 104.26 ms (106.12 ms) row scan |  |  | 2.09x |
| C8 ORDER BY Amount DESC LIMIT 10 | 65.27 ms (69.02 ms) | 81.95 ms (87.54 ms) | 40.55 ms (42.42 ms) | 1.30 ms (1.44 ms) | 0.62x |
| C8 + columnar index | 65.80 ms (68.27 ms) row scan | 81.99 ms (86.29 ms) row scan |  |  | 0.62x |

### Managed allocation per call and GC collections during the timed calls

Bytes allocated on the calling thread per call (`GC.GetAllocatedBytesForCurrentThread`), with gen0/gen1/gen2 collections over all timed calls. SQLite's and DuckDB's native memory is not visible to this counter; their figures are only what the ADO.NET layer allocates.

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 3,686.9 MB (4060/1410/33) | 3,718.3 MB (4086/1489/33) | 432 B (0/0/0) | 1.3 KB (0/0/0) |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 2,144.3 MB (2372/929/30) | 2,175.7 MB (2392/970/22) | 696 B (0/0/0) | 1.7 KB (0/0/0) |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 2,147.2 MB (2378/895/32) | 2,178.9 MB (2400/973/26) | 6.5 KB (0/0/0) | 10.3 KB (0/0/0) |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 3,543.8 MB (3969/1365/34) | 3,575.3 MB (3992/1454/31) | 456 B (0/0/0) | 1.5 KB (0/0/0) |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 3,283.4 MB (3637/1913/29) | 3,314.9 MB (3669/1428/33) | 408 B (0/0/0) | 1.1 KB (0/0/0) |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 66.8 MB (79/35/6) | 60.0 MB (67/29/2) | 1.0 KB (0/0/0) | 2.5 KB (0/0/0) |
| J7 Small x large: Products (1 category, 2%) x Orders | 495.8 KB (0/0/0) | 494.3 KB (0/0/0) | 560 B (0/0/0) | 1.5 KB (0/0/0) |
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 554.9 KB (67/24/0) | 312.6 KB (38/13/0) | 48.6 KB (5/0/0) | 97.5 KB (11/0/0) |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 318.2 KB (0/0/0) | 318.3 KB (0/0/0) | 48.7 KB (0/0/0) | 98.1 KB (0/0/0) |
| W3 (Status = A OR Status = B) AND Amount range | 3,154.7 MB (3121/1130/31) | 3,198.9 MB (3168/1213/33) | 688 B (0/0/0) | 1.7 KB (0/0/0) |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 4,008.9 MB (3884/1399/41) | 4,053.1 MB (3926/1506/37) | 816 B (0/0/0) | 1.9 KB (0/0/0) |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 1,849.1 MB (2087/757/31) | 1,893.3 MB (2123/869/21) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 1,857.4 MB (2097/758/31) | 1,901.6 MB (2136/862/24) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| R1 PK range, 100 rows, BETWEEN | 31.1 KB (3/0/0) | 31.2 KB (3/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R2 PK range, 100 rows, >= AND < | 31.1 KB (3/0/0) | 31.2 KB (3/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R3 PK range, 10,000 rows, BETWEEN | 3.0 MB (40/23/4) | 3.0 MB (38/20/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| R4 PK range, 10,000 rows, >= AND < | 3.0 MB (40/23/4) | 3.0 MB (38/20/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| G1 GROUP BY Status (4 groups) | 64.9 MB (81/0/0) | 30.5 MB (38/0/0) | 720 B (0/0/0) | 1.9 KB (0/0/0) |
| G2 GROUP BY Bucket (1,000 groups) | 56.0 MB (70/8/0) | 56.0 MB (70/6/0) | 70.7 KB (0/0/0) | 142.0 KB (0/0/0) |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 260.2 MB (328/149/17) | 261.3 MB (322/143/9) | 5.2 KB (0/0/0) | 10.8 KB (0/0/0) |
| C1 COUNT(*) WHERE Amount range | 4.6 MB (5/0/0) | 4.6 MB (5/0/0) | 664 B (0/0/0) | 1.5 KB (0/0/0) |
| C1 + columnar index | 15.1 KB (0/0/0) | 14.1 KB (0/0/0) | - | - |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 6.9 MB (8/0/0) | 6.9 MB (8/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C2 + columnar index | 14.9 KB (0/0/0) | 13.9 KB (0/0/0) | - | - |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 9.2 MB (11/0/0) | 9.2 MB (11/0/0) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C3 + columnar index | 9.2 MB (11/0/0) | 14.0 KB (0/0/0) | - | - |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 24.8 MB (31/0/0) | 24.8 MB (31/0/0) | 608 B (0/0/0) | 2.1 KB (0/0/0) |
| C4 + columnar index | 19.9 KB (0/0/0) | 18.9 KB (0/0/0) | - | - |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 36.9 MB (46/0/0) | 2.6 MB (3/0/0) | 672 B (0/0/0) | 1.5 KB (0/0/0) |
| C5 + columnar index | 36.9 MB (46/0/0) | 2.6 MB (3/0/0) | - | - |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 2,296.6 MB (2395/890/27) | 2,340.8 MB (2439/1048/26) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C6 + columnar index | 2,296.6 MB (2400/880/30) | 2,340.8 MB (2439/1048/26) | - | - |
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
- DataVo (LSM) [DataVo.Core net10.0], C6: row scan (index not used)
- DataVo (LSM) [DataVo.Core net10.0], C7: row scan (index not used)
- DataVo (LSM) [DataVo.Core net10.0], C8: row scan (index not used)
- DataVo (InMemory) [DataVo.Core net10.0], C1: answered by the columnar index
- DataVo (InMemory) [DataVo.Core net10.0], C2: answered by the columnar index
- DataVo (InMemory) [DataVo.Core net10.0], C3: answered by the columnar index
- DataVo (InMemory) [DataVo.Core net10.0], C4: answered by the columnar index
- DataVo (InMemory) [DataVo.Core net10.0], C5: row scan (index not used)
- DataVo (InMemory) [DataVo.Core net10.0], C6: row scan (index not used)
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
