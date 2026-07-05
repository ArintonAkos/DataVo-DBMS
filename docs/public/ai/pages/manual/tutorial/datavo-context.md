# Using DataVoContext

> Source route: /manual/tutorial/datavo-context
> Source file: manual/tutorial/datavo-context.md

`DataVoContext` is the main embedded API. It owns the engine instance, the default session, and the storage configuration for the process. Most applications start with one context per embedded database lifecycle.

Create a context with explicit configuration. During development, in-memory mode keeps setup simple.

```csharp
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});
```

Every statement runs in a logical session. If you call `Execute(sql)` without a session ID, DataVo uses the context's default session.

```csharp
db.Execute("CREATE DATABASE App");
db.Execute("USE App");
db.Execute("CREATE TABLE Events (Id INT PRIMARY KEY, Name VARCHAR(80))");
db.Execute("INSERT INTO Events (Id, Name) VALUES (1, 'started')");
```

Read results through `QueryResult`. The general SQL path returns dictionaries keyed by column name.

```csharp
QueryResult result = db.Execute("""
SELECT Id, Name
FROM Events
ORDER BY Id ASC;
""")[0];

foreach (var row in result.Data)
{
    int id = Convert.ToInt32(row["Id"]);
    string name = Convert.ToString(row["Name"]) ?? "";
    Console.WriteLine($"{id}: {name}");
}
```

Use explicit sessions when you need independent transaction scopes. A transaction started in one session does not belong to another session.

```csharp
Guid writerSession = Guid.NewGuid();

db.Execute("USE App", writerSession);
db.Execute("BEGIN TRANSACTION", writerSession);
db.Execute("INSERT INTO Events (Id, Name) VALUES (2, 'queued')", writerSession);
db.Execute("COMMIT", writerSession);
```

Rollback uses the same session-bound pattern.

```csharp
Guid rollbackSession = Guid.NewGuid();

db.Execute("USE App", rollbackSession);
db.Execute("BEGIN TRANSACTION", rollbackSession);
db.Execute("INSERT INTO Events (Id, Name) VALUES (3, 'temporary')", rollbackSession);
db.Execute("ROLLBACK", rollbackSession);
```

For persistent storage, the context configuration is the only part that changes. Disk mode is the simpler file-backed backend.

```csharp
using var diskDb = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DiskStoragePath = "./datavo_data",
    WalEnabled = true,
    SyncDiskWrites = true
});
```

LSM mode is the high-throughput backend. Keep `LsmStrictFsync = true` for durability-sensitive tests.

```csharp
using var lsmDb = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

## DataVoContext API Summary

`DataVoContext` supports `Execute(string query)` against the default session and `Execute(string query, Guid sessionId)` against an explicit session, session-bound transactions (`BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK`), dictionary row results in `QueryResult.Data`, and persistent `Disk` and `Lsm` storage (with WAL and durability settings). The public examples use synchronous `Execute`; an async general SQL API is planned.
