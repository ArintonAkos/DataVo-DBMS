---
title: Roslyn Source Generators
description: Compile-time table helpers, schema annotations, and [DataVoQuery] static partial SQL methods with zero reflection.
---

# Roslyn Source Generators

`DataVo.Generators` generates table helpers, strongly-typed row mappers, and pre-compiled SQL methods at build time. Because all code is emitted by Roslyn and depends only on `DataVo.Core`, it eliminates runtime reflection and is 100% compatible with Native AOT.

```csharp
using System.Numerics;
using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.StorageEngine.Config;

[DataVoTable("players")]
public sealed class Player
{
    [PrimaryKey] public int Id { get; set; }
    public string Name { get; set; } = "";
    [Indexed] public int Score { get; set; }
    [Columnar] public string Region { get; set; } = "";
    [SpatialIndex] public Vector3 Position { get; set; }
}

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
db.Execute("CREATE DATABASE game; USE game;");

db.EnsurePlayerTable();
db.Insert(new Player { Id = 1, Name = "Ada", Score = 1200, Region = "EU", Position = new(10, 0, 5) });
Player? ada = db.GetPlayerById(1);
```

## Table Annotations & Generated Helpers

Annotating a model with `[DataVoTable("table_name")]` generates zero-reflection extension methods on `DataVoContext`:

| Attribute | Effect |
| --- | --- |
| `[DataVoTable("players")]` | Binds the class to a table and generates `EnsurePlayerTable()`, `GetPlayerById(key)`, `Insert(player)`, and `Update(player)`. |
| `[PrimaryKey]` | Marks the primary key column used by `Get<Model>ById` and `Update`. |
| `[Indexed]` | Causes `Ensure<Model>Table()` to create a secondary B-Tree index (or HNSW index on `Vector3`). |
| `[Columnar]` | Adds the column to the table's columnar index (`cx_<table>`) for fast analytical aggregations (`int`, `long`, `float`, `double`, `bool`, and date columns). |
| `[SpatialIndex]` | Creates a Euclidean HNSW index (`sx_<table>_<column>`) on a `System.Numerics.Vector3` property for 3D spatial queries. |

Calling `db.EnsurePlayerTable()` creates the table and all annotated indexes if they do not exist. On compilers without module initializers (such as Unity), call `Ensure<Model>Table()` before accessing `db.Table<Player>()`.

## Compile-Time SQL with `[DataVoQuery]`

For fixed-shape SQL statements called on hot paths, annotate a `static partial` method with `[DataVoQuery]`. The generator validates parameter names at compile time and emits direct execution and projection mapping code.

```csharp
using DataVo.Core;
using DataVo.Core.CompiledQueries;

public sealed record PlayerSummary(int Id, string Name, int Score);

public static partial class PlayerQueries
{
    [DataVoQuery("SELECT id, name, score FROM players WHERE id = @id")]
    public static partial PlayerSummary? GetSummary(DataVoContext db, int id);

    [DataVoQuery("INSERT INTO players (id, name, score) VALUES (@id, @name, @score)")]
    public static partial IReadOnlyList<long> InsertPlayer(DataVoContext db, int id, string name, int score);

    [DataVoQuery("UPDATE players SET score = @score WHERE id = @id")]
    public static partial int UpdateScore(DataVoContext db, int id, int score);
}
```

Rules for `[DataVoQuery]` methods:
- The first parameter must be `DataVoContext`.
- Every SQL placeholder (`@id`, `@score`) must match a subsequent C# parameter name.
- Use `[DataVoQuery]` for stable point lookups, inserts, and narrow updates; use runtime SQL for dynamic queries, joins, or vector search.

### SQL `DATE` Projections

Generated models map SQL `DATE` columns to `DataVo.Core.DataVoDate` (and `DataVoDate?`) across all target frameworks. On modern .NET (`net10.0`), `System.DateOnly` and `System.DateOnly?` are also supported as explicit adapters.

## Schema Manifest Validation

To let the generator validate column names and pre-resolve single-column index hints at build time, include your DDL file as an `AdditionalFiles` entry with `DataVoSchemaManifest="true"`:

```xml
<ItemGroup>
  <AdditionalFiles Include="datavo.schema.sql" DataVoSchemaManifest="true" />
</ItemGroup>
```

```sql
CREATE TABLE players (
    id INT PRIMARY KEY,
    name VARCHAR(80),
    score INT
);

CREATE INDEX ix_players_score ON players (score);
```

## See Also

- [Compiled LINQ & Spatial Queries](/manual/client-interfaces/compiled-linq) — `DataVoTable<T>` LINQ interceptors, 3D spatial queries, and reactive subscriptions
- [Native AOT Deployment](/manual/client-interfaces/native-aot) — Publishing AOT binaries with `DataVo.Generators`
- [Compiled Scan Kernels](/manual/internals/compiled-scan-kernels) — How generated predicates execute directly over storage bytes
