---
title: Dapper
description: Using Dapper over DataVoConnection for parameterized SQL, POCO mapping, and transactions.
---

# Dapper

Dapper works out of the box with DataVo via `DataVo.Data`. Because `DataVoConnection` implements `DbConnection`, Dapper's `IDbConnection` extension methods require no DataVo-specific adapter package.

```csharp
using Dapper;
using DataVo.Data;

using var conn = new DataVoConnection("StorageMode=InMemory;DataSource=game");
conn.Open();

conn.Execute("CREATE TABLE players (id INT PRIMARY KEY, name VARCHAR(80), score INT)");
conn.Execute(
    "INSERT INTO players (id, name, score) VALUES (@Id, @Name, @Score)",
    new { Id = 1, Name = "Ada", Score = 1200 });

List<Player> top = conn
    .Query<Player>("SELECT id, name, score FROM players WHERE score >= @min", new { min = 500 })
    .ToList();

int total = conn.ExecuteScalar<int>("SELECT COUNT(*) FROM players");
```

Install both packages:

```bash
dotnet add package DataVo.Data --prerelease
dotnet add package Dapper
```

## Transactions & Parameterized Queries

Pass an `IDbTransaction` from `conn.BeginTransaction()` directly to Dapper's `Execute` and `Query` calls. Named parameters (`@Id`, `@min`) bind through `DataVoCommand`'s prepared-statement pipeline.

```csharp
using var tx = conn.BeginTransaction();

conn.Execute(
    "UPDATE players SET score = score + @bonus WHERE id = @id",
    new { bonus = 250, id = 1 },
    tx);

tx.Commit(); // or tx.Rollback()
```

### Verified Dapper Surface

Tested against `DataVoConnection` with Dapper `2.1.35`:

| Dapper Call | Behavior |
| --- | --- |
| `Execute` with anonymous-object parameters | Returns exact rows-affected count. |
| `Query<T>` / `QuerySingleOrDefault<T>` | Maps columns to POCO properties by name. |
| `ExecuteScalar<T>` | Returns the first column of the first row. |
| `QueryAsync<T>` | Completes synchronously (in-process engine). |
| `Execute` with `IDbTransaction` | Full `Commit()` / `Rollback()` participation. |

Features outside this verified set (such as multi-mapping, `QueryMultiple`, or list parameter expansion) pass through standard ADO.NET; validate them against your schema before relying on them.

## Dapper vs. Compiled LINQ

| Aspect | Dapper | [Compiled LINQ](/manual/client-interfaces/compiled-linq) |
| --- | --- | --- |
| Query style | Raw SQL strings | Compile-time-checked C# lambdas |
| Row mapping | Reflection-based (per Dapper) | Source-generated, zero reflection |
| Heap allocations | Per-row POCO + Dapper overhead | **0 B** warm aggregates & buffered views |
| Native AOT | Governed by Dapper AOT support | Full Native AOT support |
| Best fit | Joins, ad-hoc SQL, existing Dapper code | Hot-path lookups, filtered aggregates, game loops |

Both interfaces can share the same underlying database: use Dapper for flexible SQL and compiled LINQ for hot paths.

## See Also

- [ADO.NET Provider (DataVo.Data)](/manual/client-interfaces/ado-net) — Connection strings, savepoints, and reader behavior
- [Compiled LINQ & Spatial Queries](/manual/client-interfaces/compiled-linq) — Zero-reflection compile-time queries
- [Choosing an Interface](/manual/getting-started/choosing-an-interface) — Compare all DataVo client APIs
