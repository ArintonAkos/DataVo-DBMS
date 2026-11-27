# DataVoContext API

> Source route: /manual/client-interfaces/datavo-context-api
> Source file: manual/client-interfaces/datavo-context-api.md

`DataVoContext` is DataVo's primary embedded C# entry point. It owns the in-process storage engine, default session, and configuration with no server, socket, or connection pool to manage.

```csharp
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("CREATE DATABASE game; USE game;");
db.Execute("CREATE TABLE players (id INT PRIMARY KEY, name VARCHAR(80), score INT)");
db.Execute("INSERT INTO players (id, name, score) VALUES (1, 'Ada', 1200)");

QueryResult result = db.Execute("SELECT id, name, score FROM players WHERE id = 1")[0];
Console.WriteLine($"{result.Data[0]["name"]}: {result.Data[0]["score"]}");
```

## Executing SQL & Sessions

`Execute(string sql)` accepts one or more semicolon-separated SQL statements and returns a `List<QueryResult>`. Each `QueryResult` exposes `Fields`, `Data` (row dictionaries), `Messages`, `IsError`, and `ErrorCode`. To map a row dictionary into a model with a public parameterless constructor and writable properties, call `db.Map<T>(row)` (or `db.Get<T>(row, "column")` for a single column).

Transactions and `USE <database>` bindings are scoped to a session. Pass an explicit `Guid sessionId` to `Execute` when you need independent transaction scopes on the same context:

```csharp
Guid workerSession = Guid.NewGuid();
db.Execute("USE game", workerSession);
db.Execute("BEGIN TRANSACTION", workerSession);
db.Execute("UPDATE players SET score = 1500 WHERE id = 1", workerSession);
db.Execute("COMMIT", workerSession);
```

`DataVoContext` has no `BeginTransaction()` C# method; manage transactions with SQL (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`). See [Transactions & MVCC](/manual/fundamentals/transactions-and-mvcc).

## Prepared Statements

Use `db.Prepare(sql)` to parse and plan a parameterized SQL statement once and execute it repeatedly. Placeholders support `@param`, `$param`, and `:param` prefixes.

```csharp
DataVoPreparedStatement findPlayer = db.Prepare(
    "SELECT id, name, score FROM players WHERE score >= @min");

QueryResult rows = findPlayer.Execute(new DataVoQueryParameter("min", 1000));
```

For hot loops that read results immediately without retaining `QueryResult` across calls, `ExecuteBorrowed(...)` reuses pooled internal result buffers to reduce allocations.

## Typed, Bulk, and Atomic Batch Writes

When ingesting many rows or writing from strongly-typed models, bypass SQL parsing with direct write methods:

| Method | Best For | Notes |
| --- | --- | --- |
| `Insert<T>(table, entity)` | Single POCO insert | Maps public properties to catalog columns (cached per schema version). |
| `BulkInsert(table, rows)` | Dictionary batches | All-or-nothing insert of `IEnumerable<IReadOnlyDictionary<string, object?>>`. |
| `InsertTyped` / `InsertTypedBatch` | Zero-boxing pipelines | Accepts `ReactiveRowSchema` and `ReadOnlySpan<CellValue>`. |
| `ApplyBatch(DataVoWriteBatch)` | Multi-table atomic mutations | Combines `Upsert`, `Update`, and `Delete` in one commit (`InMemory` and `Lsm` only). |

```csharp
var batch = new DataVoWriteBatch();
batch.Upsert(playersLayout, 1, "Ada", 1500);
batch.Delete(playersLayout, 2, null, null);
DataVoBatchResult stats = db.ApplyBatch(batch);
```

## Snapshots, Auth & Change Capture

- **Snapshots:** Capture and restore point-in-time engine state for the current session with `DataVoSnapshot snap = db.CreateSnapshot()` and `db.RestoreSnapshot(snap)`, useful for deterministic test resets or branching simulation state. Both `InMemory` and `Lsm` support snapshots. A snapshot holds the catalog and every table's rows in memory; a restore requires the schema to be unchanged since the snapshot, rebuilds scalar and vector indexes, and on `Lsm` rewrites all tables in one durable write batch.
- **Session Auth:** When `EnableAuthorization = true`, switch the active principal per session with `db.Login(username, password)` and `db.Logout()`. See [Databases & Security SQL](/manual/sql/statements/databases-and-security).
- **Change Capture:** `db.Changes` exposes the engine's `ChangeCapture` stream, which feeds reactive subscriptions and custom CDC consumers.

For lazy streaming (`StreamQuery`), zero-allocation scanning (`StreamRef`), and live reactive views (`Subscribe`, `SubscribeZeroAlloc`), see [Streaming Queries](/manual/fundamentals/streaming-queries) and [Reactive Views](/manual/fundamentals/reactive-views).

::: details API Reference

```csharp
// SQL & Prepared Statements
public List<QueryResult> Execute(string query);
public List<QueryResult> Execute(string query, Guid sessionId);
public DataVoPreparedStatement Prepare(string sql);
public DataVoPreparedStatement Prepare(string sql, Guid sessionId);

// Prepared Statement Execution (DataVoPreparedStatement)
public QueryResult Execute(params DataVoQueryParameter[] parameters);
public QueryResult Execute(ReadOnlySpan<DataVoQueryParameter> parameters, Guid sessionId);
public QueryResult ExecuteBorrowed(params DataVoQueryParameter[] parameters);
public QueryResult ExecuteBorrowed(ReadOnlySpan<DataVoQueryParameter> parameters, Guid sessionId);

// Result Row Mapping
public T Map<T>(IReadOnlyDictionary<string, object?> row); // [DynamicallyAccessedMembers] parameterless ctor + public properties
public T Get<T>(IReadOnlyDictionary<string, object?> row, string columnName);

// Typed & Batch Writes
public RowKey Insert<T>(string tableName, T entity);
public IReadOnlyList<RowKey> BulkInsert(string tableName, IEnumerable<IReadOnlyDictionary<string, object?>> rows);
public RowKey InsertTyped(string tableName, ReactiveRowSchema columns, ReadOnlySpan<CellValue> row);
public IReadOnlyList<RowKey> InsertTypedBatch(string tableName, ReactiveRowSchema columns, IReadOnlyList<CellValue[]> rows);
public DataVoBatchResult ApplyBatch(DataVoWriteBatch batch);
public DataVoBatchResult ApplyBatch(DataVoWriteBatch batch, Guid sessionId);

// Snapshots, Auth & CDC
public DataVoSnapshot CreateSnapshot();
public void RestoreSnapshot(DataVoSnapshot snapshot);
public bool Login(string username, string password);
public bool Login(string username, string password, Guid sessionId);
public void Logout();
public void Logout(Guid sessionId);
public ChangeCapture Changes { get; }
```

:::

## See Also

- [Streaming Queries](/manual/fundamentals/streaming-queries): Lazy `StreamQuery` and zero-allocation `StreamRef` scans
- [Reactive Views](/manual/fundamentals/reactive-views): Z-set incremental view maintenance via `Subscribe` and `SubscribeZeroAlloc`
- [Source Generators](/manual/client-interfaces/source-generators): Compile-time table helpers and `[DataVoQuery]` methods
- [Configuration Reference](/manual/reference/configuration): All `DataVoConfig` storage, LSM, and security options
