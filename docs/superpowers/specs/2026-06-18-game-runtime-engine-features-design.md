# Game Runtime Engine Features Design

## Purpose

DataVo should become a strong fit for Unity and other C# game developers by improving reusable database engine capabilities, not by adding a Unity-only wrapper. The first feature slice should make the same C# native database practical for runtime prototypes, deterministic unit tests, local simulation loops, telemetry buffers, and per-user embedding experiments.

## Approved Feature Set

This design covers two engine features:

1. **Deterministic in-memory snapshots and restore**
2. **Typed bulk insert for application-owned rows**

Both features must work through normal DataVo APIs, especially `DataVoContext`, so game developers can use them in Unity runtime code, editor tooling, and unit tests without running a separate database service.

## Developer Value

Unity developers often need a fast local database for gameplay state, test fixtures, playtest telemetry, and player-specific AI/vector data. They also need deterministic reset behavior: seed a world/profile once, run a scenario, then restore the exact starting point for the next test or simulation.

DataVo should support this workflow directly:

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("CREATE DATABASE GameTests");
db.Execute("USE GameTests");
db.Execute("CREATE TABLE PlayerState (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
db.Execute("INSERT INTO PlayerState VALUES (1, 'Ada', 5)");

DataVoSnapshot snapshot = db.CreateSnapshot();

db.Execute("UPDATE PlayerState SET Level = 99 WHERE Id = 1");

db.RestoreSnapshot(snapshot);

List<QueryResult> result = db.Execute("SELECT Level FROM PlayerState WHERE Id = 1");
```

After restore, `Level` must be `5`.

## Architecture

### Snapshot Scope

Snapshots are initially supported only for `StorageMode.InMemory`.

A snapshot captures:

- catalog state (`EngineCatalog.ExportState()`)
- selected database binding for the context session
- serialized in-memory table rows, including tombstone positions, so RowIds remain deterministic
- index metadata through the catalog, with physical index contents rebuilt after restore

A snapshot does not capture:

- active transactions
- locks
- authenticated security principals
- disk-backed data
- process-global debug counters or telemetry counters

If a transaction is active when snapshot or restore is requested, DataVo should throw a clear `InvalidOperationException`. A snapshot should describe a stable database baseline, not a half-buffered transaction state.

### Storage Layer

The in-memory storage implementation already stores rows as serialized byte arrays keyed by `database.table`. Add an internal snapshot interface implemented by in-memory storage backends:

```csharp
internal interface IInMemoryStorageSnapshotProvider
{
    InMemoryStorageSnapshot CreateSnapshot();
    void RestoreSnapshot(InMemoryStorageSnapshot snapshot);
}
```

The snapshot must deep-copy byte arrays and preserve `null` tombstone slots.

### Runtime Layer

Add immutable snapshot model types in `DataVo.Core.Runtime`:

```csharp
public sealed class DataVoSnapshot
{
    public StorageMode StorageMode { get; }
    public string CatalogState { get; }
    public string? SelectedDatabase { get; }
    internal InMemoryStorageSnapshot StorageSnapshot { get; }
}
```

`DataVoContext` exposes:

```csharp
public DataVoSnapshot CreateSnapshot();
public void RestoreSnapshot(DataVoSnapshot snapshot);
```

`DataVoEngine` owns the actual snapshot/restore operations because it can coordinate catalog, storage, sessions, transactions, indexes, and MVCC state.

### Index Rebuild

Snapshot restore should not attempt to deep-copy index internals. Instead, restore storage and catalog first, then rebuild all catalog-registered indexes from table contents.

Scalar indexes can be rebuilt using existing `IndexManager.RebuildIndex(...)` behavior.

Vector indexes must be rebuilt from storage using the same column metadata and vector values used by `CREATE INDEX ... USING HNSW`. Add a small engine-level index rebuild helper so `DataVoContext` does not duplicate index creation logic.

### Bulk Insert

Add a typed bulk insert API on `DataVoContext`:

```csharp
public IReadOnlyList<long> BulkInsert(
    string tableName,
    IEnumerable<IReadOnlyDictionary<string, object?>> rows);
```

Behavior:

- uses the currently selected database from `SessionId`
- validates that a database is selected
- rejects empty/whitespace table names
- materializes the row sequence once
- returns assigned RowIds in insertion order
- updates indexes consistently
- preserves normal insert constraint behavior

The first implementation should introduce a shared insert service used by both `INSERT INTO` execution and `DataVoContext.BulkInsert(...)`. That avoids duplicating primary-key, unique-key, foreign-key, default-value, type-coercion, storage, and index-update behavior.

## Data Flow

### Create Snapshot

1. `DataVoContext.CreateSnapshot()` calls `Engine.CreateSnapshot(SessionId)`.
2. Engine verifies `StorageMode.InMemory`.
3. Engine verifies there is no active transaction for the session.
4. Engine exports catalog state.
5. Engine captures selected database for the session.
6. Engine asks storage for an in-memory storage snapshot.
7. Engine returns immutable `DataVoSnapshot`.

### Restore Snapshot

1. `DataVoContext.RestoreSnapshot(snapshot)` calls `Engine.RestoreSnapshot(SessionId, snapshot)`.
2. Engine verifies snapshot is non-null and `StorageMode.InMemory`.
3. Engine verifies no active transaction for the session.
4. Engine restores storage snapshot.
5. Engine loads catalog state.
6. Engine restores or clears selected database binding for the session.
7. Engine clears MVCC/version state that may reference stale RowIds.
8. Engine rebuilds catalog-registered indexes from restored table data.

### Bulk Insert

1. `DataVoContext.BulkInsert(...)` resolves the selected database.
2. Rows are materialized and copied into mutable dictionaries.
3. DataVo validates schema and constraints.
4. Storage inserts serialized rows using the batch storage API.
5. Indexes receive the inserted values.
6. Assigned RowIds are returned in insertion order.

## Error Handling

- `CreateSnapshot()` on non-in-memory storage throws `NotSupportedException`.
- `RestoreSnapshot()` on non-in-memory storage throws `NotSupportedException`.
- Restoring a snapshot from another storage mode throws `InvalidOperationException`.
- Snapshot/restore during an active transaction throws `InvalidOperationException`.
- Bulk insert without a selected database throws `InvalidOperationException`.
- Bulk insert with no rows returns an empty list.
- Bulk insert with missing required columns or duplicate keys returns the same style of failure as existing inserts.

## Testing Strategy

Add focused tests in `DataVo.Tests`:

- snapshot restores inserted rows back to the original baseline
- snapshot restores updates and deletes
- snapshot preserves RowIds after tombstones
- snapshot restore keeps primary-key indexes usable
- snapshot restore keeps vector HNSW search usable
- snapshot rejects disk storage
- restore rejects snapshots during active transactions
- bulk insert returns RowIds in order
- bulk insert updates scalar indexes
- bulk insert supports vector columns and vector indexes
- same seeded in-memory database can be restored repeatedly across multiple simulated test runs

## Documentation

Update:

- `docs/features/unity-and-godot.md`
- `docs/features/setup-and-packaging.md`
- `docs/DataVo.Core/DataVoContext.md`

The docs should frame this as reusable database functionality for game/runtime/test workflows:

> Seed once, snapshot, mutate during gameplay or tests, restore instantly, and use the same native C# database surface in runtime code and automated tests.

## Out of Scope

- Unity `.asmdef` package generation
- Unity Editor windows
- cloud sync
- disk snapshot/restore
- transaction snapshot capture
- replacing SQL with game-specific wrappers
- telemetry-specific tables or event APIs

## Success Criteria

- Unity/game docs show a clear in-memory snapshot/testing workflow.
- The feature is accessible through `DataVoContext`.
- Snapshot restore is deterministic for rows, catalog, session database binding, scalar indexes, and vector indexes.
- Bulk insert avoids SQL string loops for application-owned row batches.
- Full `DataVo.Tests` suite passes.
