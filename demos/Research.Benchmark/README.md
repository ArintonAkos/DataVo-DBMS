# DataVo Research Benchmark

This suite compares live betting/trading risk workloads across embedded engines.

The measured loop is:

1. ingest one live order tick
2. immediately query the risk read model needed by a live dashboard
3. record per-iteration latency

The host reports total time, p50 latency, p99 latency, and GC allocation.

## Engines

| Engine | Architecture | Current read path |
|---|---|---|
| DataVo | Embedded reactive IVM | `Subscribe()` maintained SQL view |
| DuckDB | Embedded polling SQL | Indexed recompute query |
| SQLite | Embedded polling SQL | Indexed recompute query |

Postgres and Redis runners are scaffolded for later phases.

## Complex VIP Exposure Benchmark

This is the default scenario and the one that shows the relational "complexity wall" DataVo is meant to attack. A live risk dashboard needs total exposure by market category, but only for VIP accounts:

```sql
SELECT m.Category, SUM(o.Stake) AS TotalExposure
FROM Orders o
JOIN Accounts a ON o.AccountId = a.Id
JOIN Markets m ON o.MarketId = m.Id
WHERE a.IsVip = true
GROUP BY m.Category
```

Schema:

| Table | Columns | Role |
|---|---|---|
| `Accounts` | `Id`, `IsVip` | Static dimension, 1,000 rows by default |
| `Markets` | `Id`, `Category` | Static dimension, 50 rows by default |
| `Orders` | `Id`, `AccountId`, `MarketId`, `Stake` | Live tick stream |

Default run shape:

| Setting | Value |
|---|---:|
| Baseline orders | 10,000 |
| Live ticks | 50,000 |
| Accounts | 1,000 |
| VIP accounts | 20% |
| Markets | 50 |
| Progress interval | 1,000 |

Engine rules:

| Engine | Rule |
|---|---|
| DataVo | Subscribes to the exact SQL and reads the maintained in-memory category exposure state. |
| DuckDB | Inserts each order, then executes the full indexed JOIN + GROUP BY query. No manual state table. |
| SQLite | Inserts each order, then executes the full indexed JOIN + GROUP BY query. No manual state table or trigger. |

Indexes on the polling engines:

```sql
CREATE INDEX IX_Accounts_Id ON Accounts (Id);
CREATE INDEX IX_Accounts_IsVip ON Accounts (IsVip);
CREATE INDEX IX_Markets_Id ON Markets (Id);
CREATE INDEX IX_Orders_AccountId ON Orders (AccountId);
CREATE INDEX IX_Orders_MarketId ON Orders (MarketId);
```

Run the default complex benchmark:

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host/Research.Benchmark.Host.csproj
```

Useful bounded smoke:

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host/Research.Benchmark.Host.csproj -- --scenario complex-vip --baseline 10000 --iterations 1000 --progress-every 500
```

Bounded smoke result on this machine with 10,000 baseline orders and 1,000 live ticks:

| Engine Name | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|
| DataVo | 17.399 | 0.013250 | 0.028500 | 10.862 |
| DuckDB | 721.202 | 0.636917 | 3.723625 | 2.626 |
| SQLite | 669.044 | 0.662000 | 0.835792 | 2.341 |

Calculated from that smoke run:

| Metric | DuckDB / DataVo | SQLite / DataVo |
|---|---:|---:|
| Total time | 41.45x slower | 38.45x slower |
| P50 latency | 48.07x slower | 49.96x slower |
| P99 latency | 130.65x slower | 29.33x slower |
| GC allocated | 0.24x | 0.22x |

Interpretation: DataVo wins this scenario because it incrementally maintains a relational view. DuckDB and SQLite are strong SQL engines, but in this benchmark they must re-run the join and aggregate after every tick.

## Simple Point-State Benchmark

The earlier benchmark is still available for a narrow key-value style exposure read:

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host/Research.Benchmark.Host.csproj -- --scenario simple-exposure
```

Defaults for both scenarios:

| Setting | Value |
|---|---:|
| Baseline orders | 10,000 |
| Live ticks | 50,000 |
| Progress interval | 1,000 |

Useful smaller simple run:

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host/Research.Benchmark.Host.csproj -- --scenario simple-exposure --baseline 10000 --iterations 5000 --progress-every 1000
```

## Historical Polling/Recompute Baseline

The first SQLite and DuckDB baselines recomputed grouped risk from `Orders` on every read:

```sql
SELECT ...
FROM Orders
WHERE Status = 'OPEN'
GROUP BY ...
```

That is intentionally the standard polling architecture, but it is an `O(N)` read after every tick. With 10,000 baseline orders and 50,000 live ticks, the completed SQLite polling run was:

| Engine | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|
| DataVo | 4,709.608 | 0.087958 | 0.128542 | 1,206.488 |
| SQLite polling/recompute | 2,864,897.176 | 33.878916 | 178.839208 | 99,371.491 |

Calculated from that run:

| Metric | SQLite / DataVo |
|---|---:|
| Total time | 608.31x slower |
| P50 latency | 385.17x slower |
| P99 latency | 1,391.29x slower |
| GC allocated | 82.36x more |

The DuckDB polling/recompute run reached 50,000 iterations in about 247 seconds for the DuckDB phase before SQLite began. That means DuckDB's vectorized OLAP execution is much better than SQLite for repeated grouped scans, but it is still doing an `O(N)` read per tick.

## Current Materialized-State Baseline

To compare raw embedded-engine overhead more fairly, SQLite and DuckDB now maintain a running total table:

```sql
CREATE TABLE RiskExposure (
    RunnerId ...,
    AccountId ...,
    TotalExposure ...,
    PRIMARY KEY (RunnerId, AccountId)
);
```

On each tick, they:

1. insert the raw order into `Orders`
2. update `RiskExposure` with an UPSERT
3. answer the dashboard read with a primary-key lookup

SQLite shape:

```sql
INSERT INTO RiskExposure (RunnerId, AccountId, TotalExposure)
VALUES (@r, @a, @e)
ON CONFLICT(RunnerId, AccountId) DO UPDATE
SET TotalExposure = RiskExposure.TotalExposure + excluded.TotalExposure;
```

Point read:

```sql
SELECT TotalExposure
FROM RiskExposure
WHERE RunnerId = @r AND AccountId = @a;
```

DuckDB uses the equivalent in-memory `RiskExposure` table with positional parameters.

Bounded smoke result after the refactor, with 10,000 baseline orders and 5,000 live ticks:

| Engine | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|
| DataVo | 471.433 | 0.085292 | 0.216625 | 123.313 |
| DuckDB materialized | 5,652.740 | 1.034292 | 3.173208 | 20.836 |
| SQLite materialized | 76.436 | 0.013166 | 0.028208 | 21.211 |

Calculated from that smoke run:

| Metric | DuckDB / DataVo | SQLite / DataVo |
|---|---:|---:|
| Total time | 11.99x slower | 0.16x |
| P50 latency | 12.13x slower | 0.15x |
| P99 latency | 14.65x slower | 0.13x |
| GC allocated | 0.17x | 0.17x |

Interpretation: once SQLite is allowed to maintain the exact materialized point state, it is extremely fast for this narrow key-value style workload. DataVo's advantage is not "can beat a hand-maintained key-value table"; it is maintaining richer SQL-derived reactive views automatically without application-authored triggers or UPSERT state tables.

## Honesty Point

There are two different claims, and the benchmark should keep them separate:

1. **Polling/recompute claim:** DataVo IVM avoids repeatedly scanning and grouping the full order table. This is where DataVo wins by orders of magnitude.
2. **Hand-maintained materialized-state claim:** if an application engineer manually builds and maintains exactly the state table needed for the read, SQLite can be extremely competitive or faster for a narrow point lookup.

The research story is strongest when both are reported: DataVo provides general incremental SQL view maintenance; SQLite/DuckDB can be excellent baselines when the application manually implements equivalent materialized state.
