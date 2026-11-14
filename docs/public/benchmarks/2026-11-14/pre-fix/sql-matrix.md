## SQL capability matrix: 1,000,000 orders, 100,000 customers, 1,000 products

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-14 13:21 +02:00.

Cells are the median latency of one call (p99 in brackets). `✗` marks an answer that differs from the oracle; such a cell is not a result. The last column is SQLite's median divided by DataVo LSM's (above 1: DataVo LSM is faster).

| Setup | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| Load (ms) | 3,660 | 559 | 4,935 | 1,023 |
| Reopen (ms) | 500.0 | 0.0 | 2.2 | 239.1 |
| Size on disk (MB) | 90.8 | in memory | 83.8 | 54.0 |
| Create columnar index (ms) | 318 | 186 | - | - |

### Joins

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 5.36 s (5.71 s) | 5.38 s (5.70 s) | 723.58 ms (725.20 ms) | 2.08 ms (2.23 ms) | 0.14x |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 2.35 s (2.53 s) ✗ | 2.46 s (2.57 s) ✗ | 84.63 ms (85.59 ms) | 1.50 ms (1.57 ms) | n/a |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 2.17 s (2.42 s) ✗ | 2.39 s (2.64 s) ✗ | 84.71 ms (85.85 ms) | 1.98 ms (2.15 ms) | n/a |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 4.36 s (4.75 s) | 4.44 s (4.66 s) | 237.92 ms (239.02 ms) | 3.44 ms (3.64 ms) | 0.05x |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 2.00 s (2.23 s) ✗ | 2.01 s (2.44 s) ✗ | 36.18 ms (36.96 ms) | 3.35 ms (3.81 ms) | n/a |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 261.68 ms (359.65 ms) | 257.04 ms (283.09 ms) | 762.87 ms (770.05 ms) | 3.49 ms (4.00 ms) | 2.92x |
| J7 Small x large: Products (1 category, 2%) x Orders | 40.13 ms (40.49 ms) | 76.60 ms (77.05 ms) | 15.14 ms (15.60 ms) | 1.08 ms (1.22 ms) | 0.38x |

### Filters

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 334.0 µs (1.09 ms) | 188.1 µs (452.4 µs) | 817.2 µs (922.8 µs) | 1.13 ms (1.34 ms) | 2.45x |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 35.60 ms (39.58 ms) | 68.74 ms (71.15 ms) | 28.78 ms (31.20 ms) | 573.2 µs (9.43 ms) | 0.81x |
| W3 (Status = A OR Status = B) AND Amount range | 3.31 s (4.64 s) | 3.39 s (3.76 s) | 61.06 ms (61.80 ms) | 2.09 ms (2.14 ms) | 0.02x |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 4.32 s (5.00 s) | 4.21 s (4.91 s) | 63.67 ms (64.45 ms) | 2.08 ms (2.16 ms) | 0.01x |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 1.64 s (1.83 s) | 1.88 s (2.18 s) | 42.68 ms (42.87 ms) | 2.82 ms (2.93 ms) | 0.03x |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 1.81 s (2.07 s) | 1.98 s (2.22 s) | 51.76 ms (52.18 ms) | 4.11 ms (4.15 ms) | 0.03x |

### Primary-key ranges

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| R1 PK range, 100 rows, BETWEEN | 13.2 µs (18.5 µs) | 70.90 ms (74.14 ms) | 18.0 µs (19.3 µs) | 171.7 µs (206.4 µs) | 1.36x |
| R2 PK range, 100 rows, >= AND < | 12.5 µs (15.9 µs) | 69.57 ms (73.77 ms) | 18.0 µs (23.9 µs) | 172.0 µs (207.5 µs) | 1.43x |
| R3 PK range, 10,000 rows, BETWEEN | 1.11 ms (12.77 ms) | 70.11 ms (82.48 ms) | 1.70 ms (1.79 ms) | 589.8 µs (737.1 µs) | 1.54x |
| R4 PK range, 10,000 rows, >= AND < | 1.10 ms (11.53 ms) | 70.84 ms (81.50 ms) | 1.71 ms (1.98 ms) | 594.8 µs (901.4 µs) | 1.55x |

### GROUP BY cardinality

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| G1 GROUP BY Status (4 groups) | 90.48 ms (92.61 ms) | 96.80 ms (97.28 ms) | 236.63 ms (242.32 ms) | 1.10 ms (1.30 ms) | 2.62x |
| G2 GROUP BY Bucket (1,000 groups) | 98.58 ms (104.79 ms) | 110.24 ms (111.76 ms) | 209.24 ms (221.65 ms) | 729.1 µs (812.5 µs) | 2.12x |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 390.66 ms (456.76 ms) | 418.27 ms (487.99 ms) | 29.61 ms (30.52 ms) | 4.36 ms (4.65 ms) | 0.08x |

### Columnar index

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB | SQLite / DataVo LSM |
|---|---:|---:|---:|---:|---:|
| C1 COUNT(*) WHERE Amount range | 67.16 ms (69.31 ms) | 84.79 ms (87.20 ms) | 43.51 ms (44.18 ms) | 1.18 ms (1.34 ms) | 0.65x |
| C1 + columnar index | 3.40 ms (3.46 ms) pushed | 3.35 ms (3.43 ms) pushed |  |  | 12.80x |
| C2 SUM(Qty) WHERE Bucket < b (INT sum) | 77.69 ms (81.56 ms) | 79.05 ms (81.21 ms) | 36.85 ms (37.28 ms) | 428.3 µs (549.8 µs) | 0.47x |
| C2 + columnar index | 3.55 ms (3.73 ms) pushed | 3.67 ms (3.77 ms) pushed |  |  | 10.39x |
| C3 SUM(Amount) WHERE Qty >= q (DOUBLE sum) | 77.89 ms (80.18 ms) | 84.25 ms (85.56 ms) | 40.08 ms (40.37 ms) | 467.3 µs (569.6 µs) | 0.51x |
| C3 + columnar index | 78.64 ms (81.99 ms) row scan | 3.10 ms (3.36 ms) pushed |  |  | 0.51x |
| C4 COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL | 102.83 ms (103.86 ms) | 98.28 ms (99.57 ms) | 58.20 ms (58.69 ms) | 1.42 ms (1.44 ms) | 0.57x |
| C4 + columnar index | 12.14 ms (12.29 ms) pushed | 11.93 ms (12.05 ms) pushed |  |  | 4.80x |
| C5 COUNT(*) WHERE Status = s AND Amount >= x (string column) | 69.34 ms (70.29 ms) | 74.02 ms (74.77 ms) | 42.57 ms (42.83 ms) | 973.9 µs (1.13 ms) | 0.61x |
| C5 + columnar index | 70.35 ms (70.56 ms) row scan | 74.28 ms (75.01 ms) row scan |  |  | 0.61x |
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 2.17 s (2.76 s) | 2.32 s (2.59 s) | 35.90 ms (36.46 ms) | 1.61 ms (1.74 ms) | 0.02x |
| C6 + columnar index | 2.56 s (2.76 s) row scan | 2.34 s (2.58 s) row scan |  |  | 0.01x |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 96.82 ms (97.11 ms) | 109.18 ms (111.11 ms) | 200.69 ms (207.78 ms) | 737.7 µs (801.2 µs) | 2.07x |
| C7 + columnar index | 97.19 ms (100.34 ms) row scan | 109.52 ms (110.41 ms) row scan |  |  | 2.06x |
| C8 ORDER BY Amount DESC LIMIT 10 | 69.25 ms (80.60 ms) | 85.24 ms (90.00 ms) | 40.32 ms (41.11 ms) | 1.28 ms (1.32 ms) | 0.58x |
| C8 + columnar index | 67.67 ms (72.63 ms) row scan | 84.74 ms (90.61 ms) row scan |  |  | 0.60x |

### Managed allocation per call and GC collections during the timed calls

Bytes allocated on the calling thread per call (`GC.GetAllocatedBytesForCurrentThread`), with gen0/gen1/gen2 collections over all timed calls. SQLite's and DuckDB's native memory is not visible to this counter; their figures are only what the ADO.NET layer allocates.

| Query | DataVo (LSM) [DataVo.Core net10.0] | DataVo (InMemory) [DataVo.Core net10.0] | SQLite | DuckDB |
|---|---:|---:|---:|---:|
| J1 3-table inner join, no filter | 3,686.9 MB (4059/1433/35) | 3,718.3 MB (4086/1635/33) | 432 B (0/0/0) | 1.3 KB (0/0/0) |
| J2 3-table inner join, filter on both dimensions (~0.2%) | 2,120.8 MB (2351/884/29) | 2,152.2 MB (2371/945/20) | 696 B (0/0/0) | 1.7 KB (0/0/0) |
| J3 3-table join, selective filter, ORDER BY ... LIMIT 50 | 2,120.8 MB (2353/887/31) | 2,152.2 MB (2376/1026/25) | 6.5 KB (0/0/0) | 10.3 KB (0/0/0) |
| J4 LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM | 3,543.8 MB (3961/1313/26) | 3,575.3 MB (3988/1551/27) | 456 B (0/0/0) | 1.5 KB (0/0/0) |
| J5 LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL) | 1,918.8 MB (2132/787/27) | 1,950.3 MB (2153/856/20) | 408 B (0/0/0) | 1.1 KB (0/0/0) |
| J6 Large x medium: Orders x Customers, GROUP BY Region | 66.8 MB (79/35/6) | 60.0 MB (67/29/2) | 1.0 KB (0/0/0) | 2.5 KB (0/0/0) |
| J7 Small x large: Products (1 category, 2%) x Orders | 495.8 KB (0/0/0) | 494.3 KB (0/0/0) | 560 B (0/0/0) | 1.5 KB (0/0/0) |
| W1 Equality, 0.1% of rows, secondary index (ProductId) | 554.9 KB (67/24/0) | 312.6 KB (38/13/0) | 48.6 KB (5/0/0) | 97.5 KB (11/0/0) |
| W2 Equality, 0.1% of rows, no index (ProductTag) | 318.2 KB (0/0/0) | 318.3 KB (0/0/0) | 48.7 KB (0/0/0) | 98.1 KB (0/0/0) |
| W3 (Status = A OR Status = B) AND Amount range | 3,154.7 MB (3121/1201/33) | 3,198.9 MB (3164/1223/29) | 688 B (0/0/0) | 1.7 KB (0/0/0) |
| W4 W3 + NULL-aware: Discount IS NOT NULL AND Discount < x | 4,008.9 MB (3882/1417/40) | 4,053.1 MB (3926/1492/37) | 816 B (0/0/0) | 1.9 KB (0/0/0) |
| W5 Prefix LIKE 'S0042%' (0.01%, no index) | 1,849.1 MB (2083/747/28) | 1,893.3 MB (2126/827/24) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| W6 Substring LIKE '%042-%' (0.1%, no index) | 1,857.4 MB (2093/739/26) | 1,901.6 MB (2132/822/21) | 552 B (0/0/0) | 1.3 KB (0/0/0) |
| R1 PK range, 100 rows, BETWEEN | 31.1 KB (3/0/0) | 31.2 KB (3/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R2 PK range, 100 rows, >= AND < | 31.1 KB (3/0/0) | 31.2 KB (3/0/0) | 5.3 KB (0/0/0) | 10.8 KB (1/0/0) |
| R3 PK range, 10,000 rows, BETWEEN | 3.0 MB (40/23/4) | 3.0 MB (38/20/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| R4 PK range, 10,000 rows, >= AND < | 3.0 MB (40/23/4) | 3.0 MB (38/20/2) | 469.4 KB (5/0/0) | 939.2 KB (11/0/0) |
| G1 GROUP BY Status (4 groups) | 64.9 MB (81/0/0) | 30.5 MB (38/0/0) | 720 B (0/0/0) | 1.9 KB (0/0/0) |
| G2 GROUP BY Bucket (1,000 groups) | 56.0 MB (70/8/0) | 56.0 MB (70/6/0) | 70.7 KB (0/0/0) | 142.0 KB (0/0/0) |
| G3 GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100 | 260.2 MB (327/146/16) | 260.2 MB (321/143/9) | 5.2 KB (0/0/0) | 10.8 KB (0/0/0) |
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
| C6 COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x | 2,296.6 MB (2399/945/30) | 2,340.8 MB (2440/1048/27) | 536 B (0/0/0) | 1.3 KB (0/0/0) |
| C6 + columnar index | 2,296.6 MB (2393/891/25) | 2,340.8 MB (2441/956/27) | - | - |
| C7 GROUP BY Bucket: COUNT, SUM(Qty) | 56.0 MB (70/8/0) | 56.0 MB (70/8/0) | 70.7 KB (0/0/0) | 126.4 KB (0/0/0) |
| C7 + columnar index | 56.0 MB (70/8/0) | 56.0 MB (70/8/0) | - | - |
| C8 ORDER BY Amount DESC LIMIT 10 | 84.0 MB (105/0/0) | 84.0 MB (105/0/0) | 864 B (0/0/0) | 2.1 KB (0/0/0) |
| C8 + columnar index | 84.0 MB (105/0/0) | 84.0 MB (105/0/0) | - | - |

### Answer check

- **DataVo (LSM) [DataVo.Core net10.0]**, J2: MISMATCH set 0 (@region=R3, @cat=7): expected 1 row(s) [1827|913220.25], got 1 row(s) [0|NULL]
- **DataVo (LSM) [DataVo.Core net10.0]**, J3: MISMATCH set 0 (@region=R3, @cat=7): expected 50 row(s) [134697|999.25|C29419|P856 / 578749|999.25|C63259|P756 / 894094|999|C63219|P756], got 0 row(s) []
- **DataVo (LSM) [DataVo.Core net10.0]**, J5: MISMATCH set 0 (): expected 1 row(s) [250395], got 1 row(s) [0]
- **DataVo (InMemory) [DataVo.Core net10.0]**, J2: MISMATCH set 0 (@region=R3, @cat=7): expected 1 row(s) [1827|913220.25], got 1 row(s) [0|NULL]
- **DataVo (InMemory) [DataVo.Core net10.0]**, J3: MISMATCH set 0 (@region=R3, @cat=7): expected 50 row(s) [134697|999.25|C29419|P856 / 578749|999.25|C63259|P756 / 894094|999|C63219|P756], got 0 row(s) []
- **DataVo (InMemory) [DataVo.Core net10.0]**, J5: MISMATCH set 0 (): expected 1 row(s) [250395], got 1 row(s) [0]
- Every other (engine, query) pair returned the oracle's answer for every parameter set.

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
