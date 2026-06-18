# Game Runtime Engine Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add reusable engine-level features that make DataVo a strong fit for Unity/game runtime code: deterministic in-memory snapshots/restore and typed bulk insert through `DataVoContext`.

**Architecture:** Implement snapshots at the engine/storage boundary, not as a Unity wrapper. In-memory storage deep-copies serialized rows; engine restore reloads catalog/session state, clears stale MVCC state, and rebuilds catalog indexes from restored rows. Bulk insert uses a shared insert service so SQL `INSERT INTO` and `DataVoContext.BulkInsert(...)` preserve the same constraints, defaults, storage, MVCC, and index behavior.

**Tech Stack:** C#/.NET 10, DataVo.Core, xUnit, in-memory storage backend, DataVo catalog/index/MVCC runtime.

---

## File Structure

- Create: `DataVo.Core/StorageEngine/Memory/InMemoryStorageSnapshot.cs`
  - Immutable internal snapshot of in-memory serialized rows.
- Create: `DataVo.Core/StorageEngine/Memory/IInMemoryStorageSnapshotProvider.cs`
  - Internal storage snapshot/restore interface.
- Modify: `DataVo.Core/StorageEngine/Memory/InMemoryStorageEngine.cs`
  - Implement deep-copy snapshot/restore.
- Modify: `DataVo.Core/StorageEngine/Backends/InMemoryStorageBackend.cs`
  - Forward snapshot/restore to the inner in-memory engine.
- Modify: `DataVo.Core/StorageEngine/StorageContext.cs`
  - Add internal snapshot/restore methods used by `DataVoEngine`.
- Create: `DataVo.Core/Runtime/DataVoSnapshot.cs`
  - Public immutable snapshot handle returned by `DataVoContext`.
- Modify: `DataVo.Core/Runtime/DataVoEngine.cs`
  - Create/restore snapshots and rebuild indexes.
- Modify: `DataVo.Core/Runtime/SessionDatabaseStore.cs`
  - Add session binding removal for restore with no selected database.
- Modify: `DataVo.Core/MVCC/VersionStorageManager.cs`
  - Add clear API for restore.
- Create: `DataVo.Core/Parser/DML/InsertRowService.cs`
  - Shared insert validation/write service for SQL and API bulk inserts.
- Modify: `DataVo.Core/Parser/DML/InsertInto.cs`
  - Delegate insertion to `InsertRowService`.
- Modify: `DataVo.Core/DataVoContext.cs`
  - Add `CreateSnapshot`, `RestoreSnapshot`, and `BulkInsert`.
- Test: `DataVo.Tests/E2E/GameRuntimeSnapshotTests.cs`
  - Snapshot/restore regression tests.
- Test: `DataVo.Tests/E2E/GameRuntimeBulkInsertTests.cs`
  - Bulk insert regression tests.
- Modify docs:
  - `docs/features/unity-and-godot.md`
  - `docs/features/setup-and-packaging.md`
  - `docs/DataVo.Core/DataVoContext.md`

---

## Task 1: Add Snapshot API Red Tests

**Files:**
- Create: `DataVo.Tests/E2E/GameRuntimeSnapshotTests.cs`

- [ ] **Step 1: Write failing snapshot/restore tests**

Create `DataVo.Tests/E2E/GameRuntimeSnapshotTests.cs`:

```csharp
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class GameRuntimeSnapshotTests
{
    [Fact]
    public void InMemorySnapshot_RestoreRevertsInsertedUpdatedAndDeletedRows()
    {
        using var context = CreateContext();

        context.Execute("CREATE TABLE PlayerState (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.Execute("INSERT INTO PlayerState VALUES (1, 'Ada', 5)");
        context.Execute("INSERT INTO PlayerState VALUES (2, 'Bea', 8)");

        DataVoSnapshot snapshot = context.CreateSnapshot();

        context.Execute("UPDATE PlayerState SET Level = 99 WHERE Id = 1");
        context.Execute("DELETE FROM PlayerState WHERE Id = 2");
        context.Execute("INSERT INTO PlayerState VALUES (3, 'Cai', 12)");

        context.RestoreSnapshot(snapshot);

        var rows = Select(context, "SELECT Id, Name, Level FROM PlayerState ORDER BY Id ASC");

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, (int)rows[0]["Id"]);
        Assert.Equal("Ada", rows[0]["Name"]);
        Assert.Equal(5, (int)rows[0]["Level"]);
        Assert.Equal(2, (int)rows[1]["Id"]);
        Assert.Equal("Bea", rows[1]["Name"]);
        Assert.Equal(8, (int)rows[1]["Level"]);
    }

    [Fact]
    public void InMemorySnapshot_RestorePreservesRowIdsAcrossTombstones()
    {
        using var context = CreateContext();

        context.Execute("CREATE TABLE Events (Id INT PRIMARY KEY, Name VARCHAR(50))");
        context.Execute("INSERT INTO Events VALUES (1, 'start')");
        context.Execute("INSERT INTO Events VALUES (2, 'middle')");
        context.Execute("DELETE FROM Events WHERE Id = 2");

        DataVoSnapshot snapshot = context.CreateSnapshot();

        context.Execute("INSERT INTO Events VALUES (3, 'after')");
        context.RestoreSnapshot(snapshot);
        context.Execute("INSERT INTO Events VALUES (4, 'restored')");

        var rows = Select(context, "SELECT Id, Name FROM Events ORDER BY Id ASC");

        Assert.Equal(2, rows.Count);
        Assert.Equal("start", rows[0]["Name"]);
        Assert.Equal("restored", rows[1]["Name"]);
    }

    [Fact]
    public void InMemorySnapshot_RestoreKeepsPrimaryKeyIndexUsable()
    {
        using var context = CreateContext();

        context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(50))");
        context.Execute("INSERT INTO Items VALUES (1, 'Sword')");

        DataVoSnapshot snapshot = context.CreateSnapshot();

        context.Execute("INSERT INTO Items VALUES (2, 'Shield')");
        context.RestoreSnapshot(snapshot);
        context.Execute("INSERT INTO Items VALUES (2, 'Shield')");

        var rows = Select(context, "SELECT Name FROM Items WHERE Id = 2");

        Assert.Single(rows);
        Assert.Equal("Shield", rows[0]["Name"]);
    }

    [Fact]
    public void InMemorySnapshot_RestoreKeepsVectorIndexSearchUsable()
    {
        using var context = CreateContext();

        context.Execute("CREATE TABLE UserEmbeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(50))");
        context.Execute("INSERT INTO UserEmbeddings VALUES (1, '[1,0,0]', 'aggressive')");
        context.Execute("INSERT INTO UserEmbeddings VALUES (2, '[0,1,0]', 'builder')");
        context.Execute("CREATE INDEX idx_user_emb ON UserEmbeddings (Emb) USING HNSW");

        DataVoSnapshot snapshot = context.CreateSnapshot();

        context.Execute("INSERT INTO UserEmbeddings VALUES (3, '[0,0,1]', 'explorer')");
        context.RestoreSnapshot(snapshot);

        List<Dictionary<string, object?>> results =
            context.SearchNearest("UserEmbeddings", "idx_user_emb", "[0.9,0.1,0]", topK: 1);

        Assert.Single(results);
        Assert.Equal("aggressive", results[0]["Label"]);
    }

    [Fact]
    public void InMemorySnapshot_RestoreCanBeRepeatedForSimulationLoops()
    {
        using var context = CreateContext();

        context.Execute("CREATE TABLE ScoreState (Id INT PRIMARY KEY, Score INT)");
        context.Execute("INSERT INTO ScoreState VALUES (1, 10)");

        DataVoSnapshot snapshot = context.CreateSnapshot();

        for (int i = 0; i < 3; i++)
        {
            context.Execute("UPDATE ScoreState SET Score = Score + 5 WHERE Id = 1");
            Assert.Equal(15, (int)Select(context, "SELECT Score FROM ScoreState WHERE Id = 1")[0]["Score"]);

            context.RestoreSnapshot(snapshot);
            Assert.Equal(10, (int)Select(context, "SELECT Score FROM ScoreState WHERE Id = 1")[0]["Score"]);
        }
    }

    [Fact]
    public void Snapshot_OnDiskStorageThrowsNotSupported()
    {
        string path = Path.Combine(Path.GetTempPath(), $"datavo_snapshot_disk_{Guid.NewGuid():N}");
        using var context = new DataVoContext(new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = path
        });

        Assert.Throws<NotSupportedException>(() => context.CreateSnapshot());
    }

    [Fact]
    public void Snapshot_DuringActiveTransactionThrows()
    {
        using var context = CreateContext();

        context.Execute("BEGIN TRANSACTION");

        Assert.Throws<InvalidOperationException>(() => context.CreateSnapshot());
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string dbName = $"GameRuntime_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {dbName}");
        context.Execute($"USE {dbName}");
        return context;
    }

    private static List<Dictionary<string, object?>> Select(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Single();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return result.Data;
    }
}
```

- [ ] **Step 2: Run tests to verify red state**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter GameRuntimeSnapshotTests
```

Expected: compile failure because `DataVoSnapshot`, `CreateSnapshot`, and `RestoreSnapshot` do not exist.

---

## Task 2: Add In-Memory Storage Snapshot Primitives

**Files:**
- Create: `DataVo.Core/StorageEngine/Memory/InMemoryStorageSnapshot.cs`
- Create: `DataVo.Core/StorageEngine/Memory/IInMemoryStorageSnapshotProvider.cs`
- Modify: `DataVo.Core/StorageEngine/Memory/InMemoryStorageEngine.cs`
- Modify: `DataVo.Core/StorageEngine/Backends/InMemoryStorageBackend.cs`
- Modify: `DataVo.Core/StorageEngine/StorageContext.cs`

- [ ] **Step 1: Add snapshot model and interface**

Create `DataVo.Core/StorageEngine/Memory/InMemoryStorageSnapshot.cs`:

```csharp
namespace DataVo.Core.StorageEngine.Memory;

internal sealed class InMemoryStorageSnapshot
{
    public InMemoryStorageSnapshot(Dictionary<string, List<byte[]?>> tables)
    {
        Tables = tables;
    }

    public Dictionary<string, List<byte[]?>> Tables { get; }
}
```

Create `DataVo.Core/StorageEngine/Memory/IInMemoryStorageSnapshotProvider.cs`:

```csharp
namespace DataVo.Core.StorageEngine.Memory;

internal interface IInMemoryStorageSnapshotProvider
{
    InMemoryStorageSnapshot CreateSnapshot();
    void RestoreSnapshot(InMemoryStorageSnapshot snapshot);
}
```

- [ ] **Step 2: Implement snapshot provider on `InMemoryStorageEngine`**

Modify class declaration in `DataVo.Core/StorageEngine/Memory/InMemoryStorageEngine.cs`:

```csharp
public class InMemoryStorageEngine : IStorageEngine, IInMemoryStorageSnapshotProvider
```

Add methods before the closing brace:

```csharp
public InMemoryStorageSnapshot CreateSnapshot()
{
    var tables = new Dictionary<string, List<byte[]?>>(StringComparer.OrdinalIgnoreCase);

    foreach (var entry in _databases)
    {
        List<byte[]> table = entry.Value;
        lock (table)
        {
            var rows = new List<byte[]?>(table.Count);
            foreach (byte[]? row in table)
            {
                rows.Add(row == null ? null : [.. row]);
            }

            tables[entry.Key] = rows;
        }
    }

    return new InMemoryStorageSnapshot(tables);
}

public void RestoreSnapshot(InMemoryStorageSnapshot snapshot)
{
    ArgumentNullException.ThrowIfNull(snapshot);

    _databases.Clear();
    foreach (var entry in snapshot.Tables)
    {
        var rows = new List<byte[]>(entry.Value.Count);
        foreach (byte[]? row in entry.Value)
        {
            rows.Add(row == null ? null! : [.. row]);
        }

        _databases[entry.Key] = rows;
    }
}
```

- [ ] **Step 3: Forward snapshot provider through `InMemoryStorageBackend`**

Modify `DataVo.Core/StorageEngine/Backends/InMemoryStorageBackend.cs`:

```csharp
internal sealed class InMemoryStorageBackend : IStorageBackend, IInMemoryStorageSnapshotProvider
```

Add methods:

```csharp
public InMemoryStorageSnapshot CreateSnapshot() => _inner.CreateSnapshot();

public void RestoreSnapshot(InMemoryStorageSnapshot snapshot) => _inner.RestoreSnapshot(snapshot);
```

- [ ] **Step 4: Expose internal storage snapshot through `StorageContext`**

Add `using DataVo.Core.StorageEngine.Memory;` to `DataVo.Core/StorageEngine/StorageContext.cs`.

Add methods after `Backend`:

```csharp
internal InMemoryStorageSnapshot CreateInMemorySnapshot()
{
    if (_storageEngine is not IInMemoryStorageSnapshotProvider provider)
    {
        throw new NotSupportedException("In-memory snapshots are supported only by the in-memory storage backend.");
    }

    return provider.CreateSnapshot();
}

internal void RestoreInMemorySnapshot(InMemoryStorageSnapshot snapshot)
{
    if (_storageEngine is not IInMemoryStorageSnapshotProvider provider)
    {
        throw new NotSupportedException("In-memory snapshots are supported only by the in-memory storage backend.");
    }

    provider.RestoreSnapshot(snapshot);
}
```

- [ ] **Step 5: Run compile check**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter GameRuntimeSnapshotTests
```

Expected: compile failure still references missing runtime/context APIs, but storage snapshot files compile.

---

## Task 3: Add Runtime Snapshot API

**Files:**
- Create: `DataVo.Core/Runtime/DataVoSnapshot.cs`
- Modify: `DataVo.Core/DataVoContext.cs`
- Modify: `DataVo.Core/Runtime/DataVoEngine.cs`
- Modify: `DataVo.Core/Runtime/SessionDatabaseStore.cs`
- Modify: `DataVo.Core/MVCC/VersionStorageManager.cs`

- [ ] **Step 1: Add public snapshot handle**

Create `DataVo.Core/Runtime/DataVoSnapshot.cs`:

```csharp
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Memory;

namespace DataVo.Core.Runtime;

public sealed class DataVoSnapshot
{
    internal DataVoSnapshot(
        StorageMode storageMode,
        string catalogState,
        string? selectedDatabase,
        InMemoryStorageSnapshot storageSnapshot)
    {
        StorageMode = storageMode;
        CatalogState = catalogState;
        SelectedDatabase = selectedDatabase;
        StorageSnapshot = storageSnapshot;
    }

    public StorageMode StorageMode { get; }

    public string CatalogState { get; }

    public string? SelectedDatabase { get; }

    internal InMemoryStorageSnapshot StorageSnapshot { get; }
}
```

- [ ] **Step 2: Add session remove API**

Modify `DataVo.Core/Runtime/SessionDatabaseStore.cs`:

```csharp
public void Remove(Guid session)
{
    _selectedDatabases.TryRemove(session, out _);
}
```

- [ ] **Step 3: Add MVCC clear API**

Modify `DataVo.Core/MVCC/VersionStorageManager.cs`:

```csharp
public void Clear()
{
    _versionLock.EnterWriteLock();
    try
    {
        _versionMetadata.Clear();
    }
    finally
    {
        _versionLock.ExitWriteLock();
    }
}
```

- [ ] **Step 4: Add engine snapshot methods**

Add to `DataVo.Core/Runtime/DataVoEngine.cs` after `PushCurrent(...)`:

```csharp
public DataVoSnapshot CreateSnapshot(Guid session)
{
    if (Config.StorageMode != StorageMode.InMemory)
    {
        throw new NotSupportedException("DataVo snapshots are currently supported only for StorageMode.InMemory.");
    }

    if (TransactionManager.HasActiveTransaction(session))
    {
        throw new InvalidOperationException("Cannot create a DataVo snapshot while the session has an active transaction.");
    }

    return new DataVoSnapshot(
        Config.StorageMode,
        Catalog.ExportState(),
        Sessions.Get(session),
        StorageContext.CreateInMemorySnapshot());
}

public void RestoreSnapshot(Guid session, DataVoSnapshot snapshot)
{
    ArgumentNullException.ThrowIfNull(snapshot);

    if (Config.StorageMode != StorageMode.InMemory)
    {
        throw new NotSupportedException("DataVo snapshots are currently supported only for StorageMode.InMemory.");
    }

    if (snapshot.StorageMode != Config.StorageMode)
    {
        throw new InvalidOperationException($"Snapshot storage mode '{snapshot.StorageMode}' cannot be restored into '{Config.StorageMode}'.");
    }

    if (TransactionManager.HasActiveTransaction(session))
    {
        throw new InvalidOperationException("Cannot restore a DataVo snapshot while the session has an active transaction.");
    }

    StorageContext.RestoreInMemorySnapshot(snapshot.StorageSnapshot);
    Catalog.LoadState(snapshot.CatalogState);

    if (string.IsNullOrWhiteSpace(snapshot.SelectedDatabase))
    {
        Sessions.Remove(session);
    }
    else
    {
        Sessions.Set(session, snapshot.SelectedDatabase);
    }

    VersionStorageManager.Clear();
    RebuildAllIndexesFromCatalog();
}
```

Continue directly to Task 4 before compiling; Task 4 adds the required `RebuildAllIndexesFromCatalog()` implementation used by `RestoreSnapshot(...)`.

- [ ] **Step 5: Add context API**

Modify `DataVo.Core/DataVoContext.cs` with `using DataVo.Core.Runtime;` already present. Add methods after `Execute(string, Guid)`:

```csharp
public DataVoSnapshot CreateSnapshot()
{
    return Engine.CreateSnapshot(SessionId);
}

public void RestoreSnapshot(DataVoSnapshot snapshot)
{
    Engine.RestoreSnapshot(SessionId, snapshot);
}
```

- [ ] **Step 6: Continue to index rebuild**

Do not run tests at the end of Task 3. The restore method intentionally references `RebuildAllIndexesFromCatalog()`, which is implemented in Task 4.

---

## Task 4: Rebuild Scalar and Vector Indexes After Restore

**Files:**
- Modify: `DataVo.Core/Runtime/DataVoEngine.cs`

- [ ] **Step 1: Add index rebuild methods**

In `DataVo.Core/Runtime/DataVoEngine.cs`, add these private methods after `RestoreSnapshot(...)`:

```csharp
private void RebuildAllIndexesFromCatalog()
{
    using IDisposable _ = PushCurrent(this);

    foreach (string databaseName in Catalog.GetDatabases())
    {
        foreach (string tableName in Catalog.GetTables(databaseName))
        {
            RebuildTableIndexes(databaseName, tableName);
        }
    }
}

private void RebuildTableIndexes(string databaseName, string tableName)
{
    var indexFiles = Catalog.GetTableIndexes(tableName, databaseName);
    if (indexFiles.Count == 0)
    {
        return;
    }

    var rows = StorageContext.GetTableContents(tableName, databaseName);

    foreach (var index in indexFiles)
    {
        string indexName = index.IndexFileName ?? string.Empty;
        if (string.IsNullOrWhiteSpace(indexName))
        {
            continue;
        }

        string indexKind = index.IndexKind ?? string.Empty;
        if (IndexManager.SupportsVectorIndexType(indexKind))
        {
            RebuildVectorIndex(databaseName, tableName, index, rows);
            continue;
        }

        var values = new Dictionary<string, List<long>>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            if (index.AttributeNames.Any(attr => !row.Value.TryGetValue(attr, out var value) || value == null))
            {
                continue;
            }

            string key = DataVo.Core.BTree.IndexKeyEncoder.BuildKeyString(row.Value, index.AttributeNames);
            if (!values.TryGetValue(key, out List<long>? rowIds))
            {
                rowIds = [];
                values[key] = rowIds;
            }

            rowIds.Add(row.Key);
        }

        IndexManager.RebuildIndex(values, indexName, tableName, databaseName);
    }
}

private void RebuildVectorIndex(
    string databaseName,
    string tableName,
    DataVo.Core.Models.Catalog.IndexFile index,
    Dictionary<long, Dictionary<string, object?>> rows)
{
    string indexName = index.IndexFileName ?? string.Empty;
    if (index.AttributeNames.Count != 1)
    {
        throw new InvalidOperationException($"Vector index '{indexName}' must reference exactly one column.");
    }

    string vectorColumn = index.AttributeNames[0];
    var vectors = new List<(long RowId, float[] Vector)>();

    foreach (var row in rows)
    {
        if (!row.Value.TryGetValue(vectorColumn, out object? rawValue) || rawValue == null)
        {
            continue;
        }

        if (!DataVo.Core.Utils.VectorParser.TryCoerceToVector(rawValue, out float[] vector))
        {
            throw new InvalidOperationException($"Cannot rebuild vector index '{indexName}' because row {row.Key} does not contain a valid vector.");
        }

        vectors.Add((row.Key, vector));
    }

    IndexManager.CreateVectorIndex(
        vectors,
        indexName,
        tableName,
        databaseName,
        metric: "cosine",
        indexType: index.IndexKind);
}
```

- [ ] **Step 2: Run snapshot tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter GameRuntimeSnapshotTests
```

Expected: all `GameRuntimeSnapshotTests` pass.

- [ ] **Step 3: Run existing vector context tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter VectorContextTests
```

Expected: pass.

- [ ] **Step 4: Commit snapshot feature**

Run:

```bash
git add DataVo.Core/StorageEngine/Memory/InMemoryStorageSnapshot.cs \
        DataVo.Core/StorageEngine/Memory/IInMemoryStorageSnapshotProvider.cs \
        DataVo.Core/StorageEngine/Memory/InMemoryStorageEngine.cs \
        DataVo.Core/StorageEngine/Backends/InMemoryStorageBackend.cs \
        DataVo.Core/StorageEngine/StorageContext.cs \
        DataVo.Core/Runtime/DataVoSnapshot.cs \
        DataVo.Core/Runtime/DataVoEngine.cs \
        DataVo.Core/Runtime/SessionDatabaseStore.cs \
        DataVo.Core/MVCC/VersionStorageManager.cs \
        DataVo.Core/DataVoContext.cs \
        DataVo.Tests/E2E/GameRuntimeSnapshotTests.cs
git commit -m "feat: add in-memory database snapshots"
```

---

## Task 5: Add Bulk Insert Red Tests

**Files:**
- Create: `DataVo.Tests/E2E/GameRuntimeBulkInsertTests.cs`

- [ ] **Step 1: Write failing bulk insert tests**

Create `DataVo.Tests/E2E/GameRuntimeBulkInsertTests.cs`:

```csharp
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class GameRuntimeBulkInsertTests
{
    [Fact]
    public void BulkInsert_InsertsRowsAndReturnsRowIdsInOrder()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Telemetry (Id INT PRIMARY KEY, EventName VARCHAR(50), Frame INT)");

        IReadOnlyList<long> rowIds = context.BulkInsert("Telemetry",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["EventName"] = "level_start", ["Frame"] = 10 },
            new Dictionary<string, object?> { ["Id"] = 2, ["EventName"] = "death", ["Frame"] = 42 }
        ]);

        Assert.Equal([1L, 2L], rowIds);

        var rows = Select(context, "SELECT Id, EventName, Frame FROM Telemetry ORDER BY Id ASC");
        Assert.Equal(2, rows.Count);
        Assert.Equal("level_start", rows[0]["EventName"]);
        Assert.Equal("death", rows[1]["EventName"]);
    }

    [Fact]
    public void BulkInsert_AppliesDefaultsAndPrimaryKeyConstraints()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT DEFAULT 1)");

        IReadOnlyList<long> rowIds = context.BulkInsert("Players",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada" },
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Duplicate" }
        ]);

        Assert.Single(rowIds);

        var rows = Select(context, "SELECT Id, Name, Level FROM Players");
        Assert.Single(rows);
        Assert.Equal("Ada", rows[0]["Name"]);
        Assert.Equal(1, (int)rows[0]["Level"]);
    }

    [Fact]
    public void BulkInsert_UpdatesScalarIndexes()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Events (Id INT PRIMARY KEY, Kind VARCHAR(50))");
        context.Execute("CREATE INDEX idx_kind ON Events (Kind)");

        context.BulkInsert("Events",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Kind"] = "spawn" },
            new Dictionary<string, object?> { ["Id"] = 2, ["Kind"] = "despawn" }
        ]);

        var rows = Select(context, "SELECT Id FROM Events WHERE Kind = 'despawn'");

        Assert.Single(rows);
        Assert.Equal(2, (int)rows[0]["Id"]);
    }

    [Fact]
    public void BulkInsert_UpdatesVectorIndexes()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE UserEmbeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(50))");
        context.Execute("CREATE INDEX idx_emb ON UserEmbeddings (Emb) USING HNSW");

        context.BulkInsert("UserEmbeddings",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Emb"] = new float[] { 1f, 0f, 0f }, ["Label"] = "combat" },
            new Dictionary<string, object?> { ["Id"] = 2, ["Emb"] = new float[] { 0f, 1f, 0f }, ["Label"] = "builder" }
        ]);

        List<Dictionary<string, object?>> nearest = context.SearchNearest("UserEmbeddings", "idx_emb", "[0.9,0.1,0]", topK: 1);

        Assert.Single(nearest);
        Assert.Equal("combat", nearest[0]["Label"]);
    }

    [Fact]
    public void BulkInsert_WithoutSelectedDatabaseThrows()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

        Assert.Throws<InvalidOperationException>(() => context.BulkInsert("Events",
        [
            new Dictionary<string, object?> { ["Id"] = 1 }
        ]));
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string dbName = $"GameBulk_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {dbName}");
        context.Execute($"USE {dbName}");
        return context;
    }

    private static List<Dictionary<string, object?>> Select(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Single();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return result.Data;
    }
}
```

- [ ] **Step 2: Run tests to verify red state**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter GameRuntimeBulkInsertTests
```

Expected: compile failure because `DataVoContext.BulkInsert(...)` does not exist.

---

## Task 6: Introduce Shared Insert Service

**Files:**
- Create: `DataVo.Core/Parser/DML/InsertRowService.cs`
- Modify: `DataVo.Core/Parser/DML/InsertInto.cs`
- Modify: `DataVo.Core/DataVoContext.cs`

- [ ] **Step 1: Create shared insert result/service**

Create `DataVo.Core/Parser/DML/InsertRowService.cs`:

```csharp
using DataVo.Core.BTree;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine;
using DataVo.Core.Transactions;
using DataVo.Core.Utils;
using DataVo.Core.MVCC;
using PolyIndexManager = DataVo.Core.Indexing.IndexManager;

namespace DataVo.Core.Parser.DML;

internal sealed class InsertRowService(
    DataVoEngine engine,
    StorageContext context,
    EngineCatalog catalog,
    PolyIndexManager indexes)
{
    public InsertRowsResult InsertRows(
        string databaseName,
        string tableName,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> rows,
        TransactionContext? txContext,
        long statementTxId)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name is required.", nameof(tableName));
        }

        if (rows.Count == 0)
        {
            return new InsertRowsResult([], []);
        }

        List<string> primaryKeys = catalog.GetTablePrimaryKeys(tableName, databaseName);
        List<string> uniqueKeys = catalog.GetTableUniqueKeys(tableName, databaseName);
        List<ForeignKey> foreignKeys = catalog.GetTableForeignKeys(tableName, databaseName);
        List<IndexFile> indexFiles = catalog.GetTableIndexes(tableName, databaseName);
        List<Column> tableColumns = catalog.GetTableColumns(tableName, databaseName);

        var uniqueKeySet = uniqueKeys.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var foreignKeysByAttribute = foreignKeys
            .GroupBy(foreignKey => foreignKey.AttributeName, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First(), StringComparer.OrdinalIgnoreCase);

        var acceptedRows = new List<Dictionary<string, object?>>();
        var messages = new List<string>();

        for (int i = 0; i < rows.Count; i++)
        {
            int rowNumber = i + 1;
            if (TryNormalizeRow(
                tableName,
                rows[i],
                tableColumns,
                uniqueKeySet,
                foreignKeysByAttribute,
                primaryKeys,
                databaseName,
                rowNumber,
                messages,
                out Dictionary<string, object?> normalized))
            {
                acceptedRows.Add(normalized);
            }
        }

        if (txContext != null)
        {
            foreach (var row in acceptedRows)
            {
                txContext.BufferInsert(tableName, row);
            }

            return new InsertRowsResult([], messages);
        }

        List<long> rowIds = context.InsertIntoTable(acceptedRows, tableName, databaseName);
        for (int i = 0; i < acceptedRows.Count; i++)
        {
            long rowId = rowIds[i];
            MvccCoordinator.RegisterInsertVersion(engine, databaseName, tableName, rowId, statementTxId);
            InsertIndexes(tableName, databaseName, acceptedRows[i], rowId, indexFiles);
        }

        return new InsertRowsResult(rowIds, messages);
    }

    private bool TryNormalizeRow(
        string tableName,
        IReadOnlyDictionary<string, object?> input,
        List<Column> tableColumns,
        HashSet<string> uniqueKeySet,
        Dictionary<string, ForeignKey> foreignKeysByAttribute,
        List<string> primaryKeys,
        string databaseName,
        int rowNumber,
        List<string> messages,
        out Dictionary<string, object?> normalized)
    {
        normalized = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (Column tableColumn in tableColumns)
        {
            object? rawInput = input.TryGetValue(tableColumn.Name, out object? supplied)
                ? supplied
                : tableColumn.DefaultValue;

            if (rawInput == null)
            {
                normalized[tableColumn.Name] = null;
            }
            else
            {
                tableColumn.Value = FormatColumnValue(rawInput);
                object? parsed = tableColumn.ParsedValue;
                if (parsed == null && !string.Equals(tableColumn.Value, "null", StringComparison.OrdinalIgnoreCase))
                {
                    messages.Add($"Type of argument doesn't match with column type in row {rowNumber}!");
                    return false;
                }

                normalized[tableColumn.Name] = parsed;
            }

            if (normalized[tableColumn.Name] != null
                && uniqueKeySet.Contains(tableColumn.Name)
                && UniqueValueExists(tableName, tableColumn.Name, tableColumn.Value!, databaseName))
            {
                messages.Add($"Unique key violation in row {rowNumber}!");
                return false;
            }

            if (foreignKeysByAttribute.TryGetValue(tableColumn.Name, out ForeignKey? foreignKey)
                && normalized[tableColumn.Name] != null
                && !ReferenceExists(foreignKey, tableColumn.Value!, databaseName))
            {
                messages.Add($"Foreign key violation in row {rowNumber}!");
                return false;
            }
        }

        if (!PrimaryKeyIsValid(tableName, normalized, primaryKeys, databaseName, rowNumber, messages))
        {
            return false;
        }

        return true;
    }

    private static string FormatColumnValue(object value)
    {
        return value switch
        {
            string text => text,
            float[] vector => $"[{string.Join(",", vector.Select(v => v.ToString(System.Globalization.CultureInfo.InvariantCulture)))}]",
            double[] vector => $"[{string.Join(",", vector.Select(v => v.ToString(System.Globalization.CultureInfo.InvariantCulture)))}]",
            IEnumerable<float> vector => $"[{string.Join(",", vector.Select(v => v.ToString(System.Globalization.CultureInfo.InvariantCulture)))}]",
            IEnumerable<double> vector => $"[{string.Join(",", vector.Select(v => v.ToString(System.Globalization.CultureInfo.InvariantCulture)))}]",
            _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "null"
        };
    }

    private bool PrimaryKeyIsValid(
        string tableName,
        Dictionary<string, object?> row,
        List<string> primaryKeys,
        string databaseName,
        int rowNumber,
        List<string> messages)
    {
        if (primaryKeys.Count == 0)
        {
            return true;
        }

        if (primaryKeys.Any(pk => !row.TryGetValue(pk, out object? value) || value == null))
        {
            messages.Add($"Primary key cannot be null in row {rowNumber}!");
            return false;
        }

        string key = IndexKeyEncoder.BuildKeyString(row, primaryKeys);
        bool exists;
        try
        {
            exists = indexes.IndexContainsKey(key, $"_PK_{tableName}", tableName, databaseName);
        }
        catch
        {
            var rows = context.GetTableContents(tableName, databaseName);
            exists = rows.Values.Any(existing => string.Equals(IndexKeyEncoder.BuildKeyString(existing, primaryKeys), key, StringComparison.Ordinal));
        }

        if (exists)
        {
            messages.Add($"Primary key violation in row {rowNumber}!");
            return false;
        }

        return true;
    }

    private bool UniqueValueExists(string tableName, string columnName, string candidate, string databaseName)
    {
        try
        {
            return indexes.IndexContainsKey(candidate, $"_UK_{columnName}", tableName, databaseName);
        }
        catch
        {
            var rows = context.GetTableContents(tableName, databaseName);
            return rows.Values.Any(row => row.TryGetValue(columnName, out object? value)
                                          && value != null
                                          && string.Equals(value.ToString(), candidate, StringComparison.Ordinal));
        }
    }

    private bool ReferenceExists(ForeignKey foreignKey, string columnValue, string databaseName)
    {
        foreach (var reference in foreignKey.References)
        {
            try
            {
                if (indexes.IndexContainsKey(columnValue, $"_PK_{reference.ReferenceTableName}", reference.ReferenceTableName, databaseName))
                {
                    continue;
                }
            }
            catch
            {
                var rows = context.GetTableContents(reference.ReferenceTableName, databaseName);
                if (rows.Values.Any(row => row.TryGetValue(reference.ReferenceAttributeName, out object? value)
                                           && value != null
                                           && string.Equals(value.ToString(), columnValue, StringComparison.Ordinal)))
                {
                    continue;
                }
            }

            return false;
        }

        return true;
    }

    private void InsertIndexes(
        string tableName,
        string databaseName,
        Dictionary<string, object?> row,
        long rowId,
        List<IndexFile> indexFiles)
    {
        foreach (var index in indexFiles)
        {
            if (index.AttributeNames.Any(attr => !row.TryGetValue(attr, out object? value) || value == null))
            {
                continue;
            }

            string indexName = index.IndexFileName ?? string.Empty;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                continue;
            }

            string indexKind = index.IndexKind ?? string.Empty;
            if (indexes.SupportsVectorIndexType(indexKind))
            {
                string vectorColumn = index.AttributeNames.Single();
                if (!VectorParser.TryCoerceToVector(row[vectorColumn], out float[] vector))
                {
                    throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
                }

                indexes.InsertIntoVectorIndex(vector, rowId, indexName, tableName, databaseName, indexKind);
                continue;
            }

            string indexValue = IndexKeyEncoder.BuildKeyString(row, index.AttributeNames);
            indexes.InsertIntoIndex(indexValue, rowId, indexName, tableName, databaseName);
        }
    }
}

internal sealed record InsertRowsResult(IReadOnlyList<long> RowIds, IReadOnlyList<string> Messages);
```

- [ ] **Step 2: Refactor `InsertInto` to use the service**

In `DataVo.Core/Parser/DML/InsertInto.cs`, replace `ProcessAndInsertTableRows(...)` internals with a call that converts `_model.RawRows` to dictionaries using `_model.Columns` and table columns. Keep existing SQL string parsing behavior by normalizing raw rows before passing to the service.

Use this shape:

```csharp
private int ProcessAndInsertTableRows(string databaseName, TransactionContext? txContext, long statementTxId)
{
    List<Column> tableColumns = Catalog.GetTableColumns(_model.TableName, databaseName);
    VerifyTableColumnsExist(tableColumns);

    bool hasColumns = _model.Columns.Count > 0;
    var inputRows = new List<IReadOnlyDictionary<string, object?>>();

    foreach (var rawRow in _model.RawRows)
    {
        VerifyRowColumnCountMatches(rawRow, tableColumns.Count, hasColumns);
        inputRows.Add(BuildInputRow(rawRow, tableColumns, hasColumns));
    }

    var service = new InsertRowService(Engine, Context, Catalog, Indexes);
    InsertRowsResult result = service.InsertRows(databaseName, _model.TableName, inputRows, txContext, statementTxId);

    foreach (string message in result.Messages)
    {
        Messages.Add(message);
    }

    return txContext == null ? result.RowIds.Count : inputRows.Count - result.Messages.Count;
}

private Dictionary<string, object?> BuildInputRow(List<string> rawRow, List<Column> tableColumns, bool hasColumns)
{
    var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
    if (!hasColumns)
    {
        for (int i = 0; i < tableColumns.Count; i++)
        {
            row[tableColumns[i].Name] = rawRow[i];
        }

        return row;
    }

    for (int i = 0; i < _model.Columns.Count; i++)
    {
        row[_model.Columns[i]] = rawRow[i];
    }

    return row;
}
```

After this compiles, delete now-unused private methods from `InsertInto.cs`: `TryParseRow`, `ResolveRawValue`, `VerifyUniqueConstraint`, `VerifyPrimaryKeys`, `LogInsertError`, `MakeInsertion`, `CheckForeignKeyConstraint`, and `ReferenceExists`.

- [ ] **Step 3: Add `DataVoContext.BulkInsert`**

Add to `DataVo.Core/DataVoContext.cs` after `RestoreSnapshot(...)`:

```csharp
public IReadOnlyList<long> BulkInsert(
    string tableName,
    IEnumerable<IReadOnlyDictionary<string, object?>> rows)
{
    ArgumentNullException.ThrowIfNull(rows);
    string databaseName = ResolveCurrentDatabase();

    var materializedRows = rows
        .Select(row => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase))
        .ToList();

    if (materializedRows.Count == 0)
    {
        return [];
    }

    if (Engine.TransactionManager.HasActiveTransaction(SessionId))
    {
        throw new InvalidOperationException("BulkInsert cannot run while the current session has an active transaction.");
    }

    var service = new DataVo.Core.Parser.DML.InsertRowService(
        Engine,
        Engine.StorageContext,
        Engine.Catalog,
        Engine.IndexManager);

    long statementTxId = DataVo.Core.MVCC.MvccCoordinator.ResolveStatementTransactionId(Engine, null);
    InsertRowsResult result = service.InsertRows(databaseName, tableName, materializedRows, txContext: null, statementTxId);
    return result.RowIds;
}
```

Keep `InsertRowsResult` internal. `DataVoContext` can reference it because it is in the same assembly, and no public API exposes `InsertRowsResult`.

- [ ] **Step 4: Run bulk tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter GameRuntimeBulkInsertTests
```

Expected: all bulk tests pass.

- [ ] **Step 5: Run insert/default/vector regression tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "DefaultTests|VectorIndexTests|AdoNetTests"
```

Expected: pass.

---

## Task 7: Documentation Updates

**Files:**
- Modify: `docs/features/unity-and-godot.md`
- Modify: `docs/features/setup-and-packaging.md`
- Modify: `docs/DataVo.Core/DataVoContext.md`

- [ ] **Step 1: Update Unity/Godot page**

Add this section to `docs/features/unity-and-godot.md` after "Typical use cases":

```markdown
## Deterministic in-memory workflows

For gameplay tests, runtime simulations, and local playtest tooling, use `StorageMode.InMemory` with snapshots:

```csharp
using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

db.Execute("CREATE DATABASE GameTests");
db.Execute("USE GameTests");
db.Execute("CREATE TABLE PlayerState (Id INT PRIMARY KEY, Level INT)");
db.Execute("INSERT INTO PlayerState VALUES (1, 5)");

DataVoSnapshot baseline = db.CreateSnapshot();

db.Execute("UPDATE PlayerState SET Level = 10 WHERE Id = 1");
db.RestoreSnapshot(baseline);
```

This lets game teams seed one database, run a scenario, restore instantly, and reuse the same database surface in runtime code and automated tests.
```

- [ ] **Step 2: Add bulk insert docs**

Add this section to `docs/DataVo.Core/DataVoContext.md`:

```markdown
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
```

- [ ] **Step 3: Mention feature in setup docs**

Add one bullet to `docs/features/setup-and-packaging.md` under Unity/Godot guidance:

```markdown
- For deterministic tests and simulations, prefer `StorageMode.InMemory` plus `CreateSnapshot()` / `RestoreSnapshot(...)`.
```

- [ ] **Step 4: Run docs grep**

Run:

```bash
rg -n "CreateSnapshot|RestoreSnapshot|BulkInsert|deterministic" docs/features/unity-and-godot.md docs/features/setup-and-packaging.md docs/DataVo.Core/DataVoContext.md
```

Expected: all three docs files contain the new API names or deterministic workflow text.

---

## Task 8: Final Verification and Commits

**Files:**
- All files changed by Tasks 5-7.

- [ ] **Step 1: Run focused feature tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "GameRuntimeSnapshotTests|GameRuntimeBulkInsertTests|VectorContextTests"
```

Expected: pass.

- [ ] **Step 2: Run broader regression tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "DefaultTests|VectorIndexTests|AdoNetTests|TransactionTests"
```

Expected: pass.

- [ ] **Step 3: Run full tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore
```

Expected: all tests pass.

- [ ] **Step 4: Check whitespace**

Run:

```bash
git diff --check
```

Expected: no output.

- [ ] **Step 5: Commit bulk insert and docs**

Run:

```bash
git add DataVo.Core/Parser/DML/InsertRowService.cs \
        DataVo.Core/Parser/DML/InsertInto.cs \
        DataVo.Core/DataVoContext.cs \
        DataVo.Tests/E2E/GameRuntimeBulkInsertTests.cs \
        docs/features/unity-and-godot.md \
        docs/features/setup-and-packaging.md \
        docs/DataVo.Core/DataVoContext.md
git commit -m "feat: add game runtime bulk insert workflows"
```

- [ ] **Step 6: Show final status**

Run:

```bash
git status --short
git log -3 --oneline
```

Expected: worktree clean and recent commits include snapshot/bulk feature commits.
