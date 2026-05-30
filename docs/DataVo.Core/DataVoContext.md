# DataVoContext

`DataVoContext` is the simplest embedding entry point for developers who want to execute SQL against a dedicated `DataVoEngine` instance.

## What it does

- initializes an engine from `DataVoConfig`
- manages a default session identifier
- executes SQL through `QueryEngine`
- disposes engine-owned resources when the context is disposed

## Typical usage

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var context = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

context.Execute("CREATE DATABASE Demo");
context.Execute("USE Demo");
context.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(50))");
var results = context.Execute("SELECT * FROM Users");
```

## Runtime diagnostics

Runtime diagnostics are disabled by default. Enable `context.Diagnostics.Enabled` to record `RuntimeQueryStats` for SQL execution and direct context operations such as `BulkInsert` and `SearchNearest`.

```csharp
context.Diagnostics.Enabled = true;
context.Diagnostics.SlowQueryThreshold = TimeSpan.FromMilliseconds(25);

context.Execute("SELECT * FROM Users WHERE Id = 1");

RuntimeQueryStats? stats = context.Diagnostics.LastQuery;
```

Stats include storage mode, database, tables, rows, indexes, vector usage, elapsed time, and errors. Recent and slow histories are bounded by `RecentQueryCapacity` and `SlowQueryCapacity`.

## Compiled queries

Use `[DataVoQuery]` on `static partial` methods for source-generated compiled queries with fixed SQL shapes. The generated method builds a `DataVoCompiledQueryPlan` and calls `DataVoCompiledQuery` runtime helpers.

```csharp
[DataVoQuery("SELECT Id, Name FROM Users WHERE Id = @id")]
public static partial UserRow? GetUser(DataVoContext db, int id);
```

Use `Execute(...)` for dynamic SQL or unsupported query shapes.

## Snapshots for deterministic tests

`CreateSnapshot()` captures the current in-memory engine state for the context session. `RestoreSnapshot(...)` restores that state, including storage, catalog, session database selection, MVCC metadata, and rebuilt indexes.

```csharp
context.Execute("CREATE TABLE PlayerState (Id INT PRIMARY KEY, Level INT)");
context.Execute("INSERT INTO PlayerState VALUES (1, 5)");

DataVoSnapshot baseline = context.CreateSnapshot();

context.Execute("UPDATE PlayerState SET Level = 10 WHERE Id = 1");
context.RestoreSnapshot(baseline);
```

Snapshots are available for `StorageMode.InMemory`.

## Bulk insert for runtime/test data

`BulkInsert` inserts application-owned row dictionaries without building SQL strings in a loop:

```csharp
IReadOnlyList<long> rowIds = context.BulkInsert("Telemetry",
[
    new Dictionary<string, object?> { ["Id"] = 1, ["EventName"] = "level_start", ["Frame"] = 10 },
    new Dictionary<string, object?> { ["Id"] = 2, ["EventName"] = "death", ["Frame"] = 42 }
]);
```

The API uses the selected database for the context session and updates DataVo indexes consistently.

## When to use it

Use `DataVoContext` when you want a friendly application-facing API. Contributors working deeper in the engine will usually interact directly with `DataVoEngine`, `StorageContext`, and parser actions.

## Related files

- `Runtime/DataVoEngine.cs`
- `Parser/QueryEngine.cs`
- `StorageEngine/Config/DataVoConfig.cs`
