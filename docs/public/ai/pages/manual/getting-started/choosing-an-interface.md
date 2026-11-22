# Choosing an Interface

> Source route: /manual/getting-started/choosing-an-interface
> Source file: manual/getting-started/choosing-an-interface.md

DataVo exposes five ways to query the same embedded engine. They differ in ergonomics and per-call allocation overhead — not in storage modes, durability, or index support — and you can freely mix them in the same process.

## Decision Matrix

| Interface | Packages | Best For | Per-Call Overhead | Native AOT |
| --- | --- | --- | --- | --- |
| [Compiled LINQ](/manual/client-interfaces/compiled-linq) | `DataVo.Core` + `DataVo.Generators` | Hot paths: point lookups, filtered scans, aggregates, projections | Lowest — reads stored bytes directly; zero-alloc aggregates | Yes |
| [`DataVoContext`](/manual/client-interfaces/datavo-context-api) | `DataVo.Core` | Ad-hoc SQL, DDL, prepared statements, vector search, reactive views | Low with `db.Prepare(...)`; materializes dictionaries on general path | Yes |
| [ADO.NET](/manual/client-interfaces/ado-net) | `DataVo.Data` | Libraries expecting `DbConnection`, `DbCommand`, `DbDataReader` | Same engine behind standard `System.Data.Common` wrappers | Yes |
| [Dapper](/manual/client-interfaces/dapper) | `DataVo.Data` + `Dapper` | Plain SQL mapped to POCOs with familiar micro-ORM APIs | ADO.NET overhead plus Dapper row mapping | Per Dapper |
| [EF Core Bridge](/manual/client-interfaces/entity-framework) | `DataVo.EntityFrameworkCore` | Existing EF Core `DbContext` models (experimental preview) | Highest — EF Core infrastructure sits above DataVo | No |

**Rule of thumb:** start with `DataVoContext` for DDL and general SQL, move hot-path reads and aggregates to **Compiled LINQ**, use **ADO.NET / Dapper** when integrating existing SQL codebases, and use the **EF Core bridge** only when you already depend on EF Core.

## How Every Interface Connects

<div class="sketch-diagram light-only">
  <img src="/manual/fig-08-client-interfaces-light.svg" alt="Five .NET 10 client interfaces routing into the DataVo.Core engine and storage layers" />
</div>
<div class="sketch-diagram dark-only">
  <img src="/manual/fig-08-client-interfaces-dark.svg" alt="Five .NET 10 client interfaces routing into the DataVo.Core engine and storage layers" />
</div>

## The Same Query, Five Ways

Counting players with `Score >= 1000` in the `players` table:

**1. `DataVoContext` (SQL)**

```csharp
var result = db.Execute("SELECT COUNT(*) AS Total FROM players WHERE Score >= 1000")[0];
object? total = result.Data[0]["Total"];
```

**2. Compiled LINQ (`DataVo.Generators`)**

```csharp
int total = db.Table<Player>().Where(1000, static (p, min) => p.Score >= min).Count();
```

**3. ADO.NET (`DataVo.Data`)**

```csharp
using var cmd = connection.CreateCommand();
cmd.CommandText = "SELECT COUNT(*) FROM players WHERE Score >= @min";
cmd.Parameters.AddWithValue("@min", 1000);
object? total = cmd.ExecuteScalar();
```

**4. Dapper**

```csharp
int total = connection.ExecuteScalar<int>(
    "SELECT COUNT(*) FROM players WHERE Score >= @min",
    new { min = 1000 });
```

**5. Entity Framework Core Bridge**

```csharp
List<Player> matches = efContext.QueryFromDataVo<Player>(q =>
    q.Where(static p => p.Score >= 1000));
int total = matches.Count;
```

::: info DataVo Extension
Compiled LINQ reads the `Score` cell directly from packed row buffers without allocating `Player` instances. See [Compiled Scan Kernels](/manual/internals/compiled-scan-kernels) for how the generated code works.
:::

## Performance, Heap Allocation & Trade-offs

All five interfaces execute against the same storage engine, indexes, and MVCC transaction coordinator. Their runtime cost differs strictly by **how much marshaling, SQL parsing, and object materialization sits between your C# code and stored row bytes**.

| Interface | Measured / Architectural Latency & Heap Allocation | Native AOT | Key Advantages | Key Disadvantages |
| --- | --- | --- | --- | --- |
| **Compiled LINQ** (`DataVo.Generators`) | **55.0 ns / 56 B** (InMemory) and **216.2 ns / 144 B** (LSM) for PK point lookups over 100k rows; **27.1–89.6 µs / 0 B** for compiled `Count`/`Min`/`Sum`/`Max` over 200k `[Columnar]` rows *(M4 Pro, .NET 10.0.3)* | **100% AOT-safe** | Fastest reads & aggregates; zero-alloc scan kernels; compile-time type safety; `.SubscribeInto()` zero-GC reactive views | Requires `[DataVoTable]` + `<InterceptorsNamespaces>`; limited to single-table LINQ shapes (use SQL for joins/CTEs/DDL) |
| **`DataVoContext` SQL** (`DataVo.Core`) | **1.2–1.4 µs** for prepared-statement PK point reads over 1M LSM rows (`db.Prepare(...)`). Ad-hoc `db.Execute(...)` adds SQL lex/parse overhead and materializes `Dictionary<string, object?>` per row | **100% AOT-safe** | Full SQL dialect (joins, CTEs, DDL, vector `<->`/`<=>`, `EXPLAIN`, transactions); `StreamRef` & `SubscribeZeroAlloc` available | String-typed results unless paired with `[DataVoQuery]` or `Map<T>`; higher GC pressure if returning large dictionary result sets |
| **ADO.NET** (`DataVo.Data`) | *Not separately microbenchmarked.* Wraps `DataVoContext` prepared/ad-hoc SQL execution with `DataVoCommand`, `DataVoParameter`, and `DataVoDataReader` over `System.Data.Common` | **100% AOT-safe** | Standard .NET `DbConnection`/`DbDataReader` contract; drop-in for libraries expecting ADO.NET; exposes `BeginTransaction()` | More verbose boilerplate than `DataVoContext` or Compiled LINQ; no `DbProviderFactory`; misses DataVo-specific `StreamRef`/IVM APIs |
| **Dapper** (`DataVo.Data` + `Dapper`) | *Not separately microbenchmarked.* Runs over `DataVoConnection`/`DataVoDataReader`, adding Dapper's parameter binding and POCO row materialization per returned row | **Reflection-based** *(per Dapper; AOT requires Dapper.AOT)* | Minimal boilerplate for plain SQL → POCO mapping; familiar API for existing .NET teams | Extra ADO.NET + Dapper mapping allocations vs. Compiled LINQ; reflection-based default mapping is not Native AOT clean |
| **EF Core Bridge** (`DataVo.EntityFrameworkCore`) | *Not separately microbenchmarked.* Runs EF Core's model metadata, LINQ expression-tree inspection, and `DbContext` entity tracking on top of `DataVo.Core` | **Excluded from AOT** | Reuses existing EF Core `DbContext` entity models and `UseDataVo(...)` bootstrapping | Experimental preview; highest per-query overhead and memory footprint; limited LINQ translation vs. relational EF providers |

::: tip Benchmark Grounding
Point-lookup and filtered-aggregate numbers above are taken directly from the verified BenchmarkDotNet reports in `docs/public/benchmarks/2026-11-21-m4pro/` (Apple M4 Pro, .NET 10.0.3), and raw prepared-SQL PK lookup latency is from the 1M-row `sql-workload` suite in [Benchmarks](/manual/performance/benchmarks#sql-over-1-000-000-rows). ADO.NET, Dapper, and EF Core are thin or layered wrappers over `DataVo.Core` and are not separately timed in the BDN suite.
:::

## See Also

- [DataVoContext API](/manual/client-interfaces/datavo-context-api) — Primary SQL, prepared statement, and batch write API.
- [Roslyn Source Generators](/manual/client-interfaces/source-generators) — `[DataVoTable]`, `[DataVoQuery]`, and build-time codegen.
- [Compiled LINQ](/manual/client-interfaces/compiled-linq) — Supported LINQ operators and interceptor setup.
- [Benchmarks](/manual/performance/benchmarks) — Measured latency and allocation cost across interfaces.
