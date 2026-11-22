# Quickstart

> Source route: /manual/getting-started/quickstart
> Source file: manual/getting-started/quickstart.md

Get a working DataVo database running in a `.NET 10` console app in under a minute. You will start with `StorageMode.InMemory` for zero-setup iteration, then switch to crash-safe `StorageMode.Lsm` with a two-line config change.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

db.Execute("CREATE DATABASE GameDb");
db.Execute("USE GameDb");

db.Execute("""
    CREATE TABLE players (
        Id INT PRIMARY KEY,
        Name VARCHAR(80),
        Score INT,
        Region VARCHAR(32)
    );
    """);

db.Execute("INSERT INTO players (Id, Name, Score, Region) VALUES (1, 'Ada', 980, 'EU')");
db.Execute("INSERT INTO players (Id, Name, Score, Region) VALUES (2, 'Grace', 1120, 'US')");

var result = db.Execute("SELECT Id, Name, Score FROM players WHERE Score >= 900 ORDER BY Score DESC LIMIT 10")[0];
foreach (var row in result.Data)
{
    Console.WriteLine($"{row["Id"]}: {row["Name"]} ({row["Score"]})");
}
```

## 1. Create a Project and Install `DataVo.Core`

```bash
dotnet new console -n DataVoQuickstart
cd DataVoQuickstart
dotnet add package DataVo.Core --prerelease
```

## 2. Open a `DataVoContext`

Construct a `DataVoContext` with a `DataVoConfig`. Always dispose the context with `using` so background flushes and file locks release cleanly.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});
```

## 3. Create a Database and Table

DataVo hosts multiple logical databases inside one context. Create a database, select it with `USE`, and define your table schema.

```csharp
db.Execute("CREATE DATABASE GameDb");
db.Execute("USE GameDb");

db.Execute("""
    CREATE TABLE players (
        Id INT PRIMARY KEY,
        Name VARCHAR(80),
        Score INT,
        Region VARCHAR(32)
    );
    """);
```

## 4. Insert and Query with Parameters

Use prepared statements (`db.Prepare`) when running the same query repeatedly to avoid re-parsing SQL and to bind parameters safely.

```csharp
var insert = db.Prepare(
    "INSERT INTO players (Id, Name, Score, Region) VALUES (@id, @name, @score, @region)");

insert.Execute(
    new DataVoQueryParameter("id", 1),
    new DataVoQueryParameter("name", "Ada"),
    new DataVoQueryParameter("score", 980),
    new DataVoQueryParameter("region", "EU"));

var topPlayers = db.Prepare("SELECT Id, Name, Score FROM players WHERE Score >= @min ORDER BY Score DESC");
var result = topPlayers.Execute(new DataVoQueryParameter("min", 900))[0];

foreach (var row in result.Data)
{
    Console.WriteLine($"{row["Id"]}: {row["Name"]} ({row["Score"]})");
}
```

## 5. Persist Data with LSM Storage

Switch from `StorageMode.InMemory` to `StorageMode.Lsm` to persist tables to disk with a crash-safe write-ahead log (`LsmStrictFsync = true` by default).

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

::: info DataVo Extension
Add `DataVo.Generators` and annotate a C# `Player` class with `[DataVoTable("players")]` to query `db.Table<Player>()` with compile-time LINQ and zero per-row allocations.
:::

## See Also

- [Choosing an Interface](/manual/getting-started/choosing-an-interface) — Compare SQL, Compiled LINQ, ADO.NET, Dapper, and EF Core.
- [Storage Modes](/manual/fundamentals/storage-modes) — Understand `InMemory`, `Lsm`, and `Disk` trade-offs.
- [DataVoContext API](/manual/client-interfaces/datavo-context-api) — Full reference for prepared statements, typed writes, and streaming.
- [Vector Search](/manual/fundamentals/vector-search) — Store `VECTOR(n)` embeddings alongside relational columns.
