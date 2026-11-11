# DataVo.Benchmarks

A [BenchmarkDotNet](https://benchmarkdotnet.org) suite that runs the same data and the same questions through
DataVo, SQLite (`Microsoft.Data.Sqlite`) and DuckDB (`DuckDB.NET`).

## Running

```bash
# everything (about 15 minutes)
dotnet run -c Release --project benchmarks/DataVo.Benchmarks -- --filter '*'

# one class, or a quicker and noisier pass
dotnet run -c Release --project benchmarks/DataVo.Benchmarks -- --filter '*FilteredAggregate*'
dotnet run -c Release --project benchmarks/DataVo.Benchmarks -- --filter '*' --job short
```

Results are written to `BenchmarkDotNet.Artifacts/results/` as Markdown, CSV and HTML. Close other programs
first; BenchmarkDotNet reports the error and standard deviation of every number, so check them before comparing.

## What is measured

All three engines hold the same `players` table (`Id` primary key, `Active`, `Level`, `Gold`, `Ratio`, `Name`),
filled with the same rows by `Player.Create`. Each benchmark runs twice: once with every database in memory and
once with every database in a file.

| Class | Question | DataVo | SQLite and DuckDB |
| --- | --- | --- | --- |
| `FilteredAggregateBenchmarks` | `COUNT(*)`, `SUM(Gold)`, `MIN(Gold)`, `MAX(Ratio)` over `WHERE Level >= @min`; 200,000 rows, half match | Compiled LINQ: `Where(...).Count()`, `.Sum()`, `.Min()`, `.Max()` | A statement prepared once, `ExecuteScalar` |
| `PointLookupBenchmarks` | One row by primary key, read into a `Player`; 100,000 rows, keys in shuffled order | Compiled LINQ: `FirstOrDefault(id, static (p, k) => p.Id == k)` | A statement prepared once, `ExecuteReader`, one `Player` built |
| `BulkInsertBenchmarks` | Load 100,000 rows into an empty table | `InsertTypedBatch` | SQLite: prepared `INSERT` in one transaction. DuckDB: appender |

Before timing, each class checks that the three engines return the same answer.

## How each engine is configured

| | In memory | In a file |
| --- | --- | --- |
| DataVo | `StorageMode.InMemory` | `StorageMode.Lsm`, default settings (no `fsync` per write) |
| SQLite | `Data Source=:memory:` | WAL journal, `synchronous=NORMAL` (no `fsync` per commit) |
| DuckDB | `Data Source=:memory:` | A database file, default settings |

Things to keep in mind when reading the numbers:

- DuckDB runs a query on several threads by default; DataVo and SQLite use one. Set
  `DATAVO_BENCH_DUCKDB_THREADS=1` to compare single-threaded.
- DuckDB is a column store built for scans and aggregates; SQLite and DataVo store rows. The aggregate benchmarks
  play to DuckDB's design and the point lookup to the row stores'.
- SQLite and DuckDB are called through ADO.NET, which is how .NET programs use them. That cost is part of their
  numbers, as the cost of DataVo's own API is part of DataVo's.
- The bulk insert builds its rows inside the measurement for every engine and does not include opening or
  closing the database.
