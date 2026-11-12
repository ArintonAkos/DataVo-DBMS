# ADO.NET Provider (DataVo.Data)

`DataVo.Data` is a standard ADO.NET provider. Its types derive from the `System.Data.Common` base classes (`DbConnection`, `DbCommand`, `DbDataReader`, `DbTransaction`, `DbParameter`), so any code or library written against those abstractions can use DataVo without DataVo-specific adapters. That includes micro-ORMs such as [Dapper](/manual/client-interfaces/dapper).

```bash
dotnet add package DataVo.Data --prerelease
```

## Types

| ADO.NET base class | DataVo type | Notes |
| --- | --- | --- |
| `DbConnection` | `DataVoConnection` | Opens the embedded engine in-process. There is no network connection. |
| `DbCommand` | `DataVoCommand` | Text commands. Parameters use `@name` placeholders. |
| `DbDataReader` | `DataVoDataReader` | Forward-only reader over the command's results. |
| `DbTransaction` | `DataVoTransaction` | `Commit`, `Rollback` and savepoints. |
| `DbParameter` | `DataVoParameter` | `AddWithValue` is available on `DataVoParameterCollection`. |
| none | `DataVoConnectionStringBuilder` | Parses the [connection string](/manual/reference/configuration). |
| `Exception` | `DataVoException` | Carries `ErrorCode`, `Line` and `Column`. |

## Connect, create, insert, query

```csharp
using DataVo.Data;

using var connection = new DataVoConnection("StorageMode=Lsm;DataSource=./app-data");
connection.Open();

using (var create = connection.CreateCommand())
{
    create.CommandText = "CREATE TABLE IF NOT EXISTS Players (Id INT PRIMARY KEY, Name VARCHAR(40), Level INT)";
    create.ExecuteNonQuery();
}

using (var insert = connection.CreateCommand())
{
    insert.CommandText = "INSERT INTO Players (Id, Name, Level) VALUES (@id, @name, @level)";
    insert.Parameters.AddWithValue("@id", 1);
    insert.Parameters.AddWithValue("@name", "Ada");
    insert.Parameters.AddWithValue("@level", 12);
    int affected = insert.ExecuteNonQuery(); // 1
}

using var query = connection.CreateCommand();
query.CommandText = "SELECT Id, Name FROM Players WHERE Level >= @min";
query.Parameters.AddWithValue("@min", 5);

using var reader = query.ExecuteReader();
while (reader.Read())
{
    Console.WriteLine($"{reader.GetInt32(0)}: {reader.GetString(1)}");
}
```

`Open()` initializes the storage engine, creates the database named by `DataSource` if it does not exist, and selects it. `Close()` rolls back any open transaction and releases the engine.

## Transactions

A transaction is bound to the connection's session. `BeginTransaction()` starts it, and statements run on the same connection join it.

```csharp
using var transaction = connection.BeginTransaction();

using var update = connection.CreateCommand();
update.CommandText = "UPDATE Players SET Level = @level WHERE Id = @id";
update.Parameters.AddWithValue("@level", 13);
update.Parameters.AddWithValue("@id", 1);
update.ExecuteNonQuery();

transaction.Save("afterUpdate");   // SAVEPOINT
// ... more work ...
transaction.Rollback("afterUpdate"); // ROLLBACK TO SAVEPOINT

transaction.Commit();
```

Savepoints (`Save`, `Rollback(name)`, `Release`) are supported. For the isolation guarantees you can rely on, read [Transactions, ACID & MVCC](/manual/storage-engine/transactions-acid-mvcc). `DataVoTransaction.IsolationLevel` is reported for ADO.NET compatibility; the engine's documented contract is the source of truth, including its limits on predicate-level isolation.

## Prepared and parameterized commands

A command with parameters, or one on which you call `Prepare()`, runs through the engine's prepared-statement pipeline, so repeated execution does not re-parse the SQL.

```csharp
using var lookup = connection.CreateCommand();
lookup.CommandText = "SELECT Name FROM Players WHERE Id = @id";
lookup.Parameters.AddWithValue("@id", 1);
lookup.Prepare();

for (int id = 1; id <= 100; id++)
{
    lookup.Parameters[0].Value = id;
    object? name = lookup.ExecuteScalar();
}
```

## Errors

A failed statement throws `DataVoException`. Branch on `ErrorCode`, not on the message text. See [Error Handling](/manual/reference/error-handling).

```csharp
try
{
    insert.ExecuteNonQuery();
}
catch (DataVoException ex)
{
    Console.WriteLine($"{ex.ErrorCode} at line {ex.Line}, column {ex.Column}: {ex.Message}");
}
```

## Behavior to know about

- **The reader is forward-only over a materialized result.** `ExecuteReader()` runs the command and the reader walks the returned rows; it does not stream pages from storage. For zero-allocation streaming, use the typed table API, `RowRef` and compiled LINQ (see [Zero-Allocation Row Model](/manual/internals/zero-allocation-rows)).
- **Metadata is available before `Read()`.** `GetFieldType` and `GetName` work before the first row, as ADO.NET consumers expect. Column types are taken from the first non-null value in the result, so a column that is null in every row reports `object`.
- **Async methods run synchronously.** `ExecuteReaderAsync` and friends come from the `DbCommand` base class and complete on the calling thread; the engine is in-process.
- **`Cancel()` is a no-op and `CommandTimeout` does not cancel a running statement.** Lock waits are bounded by `LockAcquireTimeoutMs`; see [Configuration](/manual/reference/configuration).
- **One process owns a persisted database directory.** Connections in the same process that open the same directory share one engine and must use equivalent settings. A second process opening the directory gets `DatabaseInUseException`; see [LSM-Tree Engine](/manual/storage-engine/lsm-mode#opening-the-same-directory-twice).
- **There is no connection pool and no server.** Opening a connection is opening the engine.

## Native AOT

`DataVo.Data` builds with the trim and AOT analyzers enabled and the IL warnings treated as errors, so the provider itself is AOT-clean. If you add a reflection-based mapper on top, that library's AOT support applies. See [Native AOT Deployment](/manual/client-interfaces/native-aot).

## Related

- [ADO.NET & Dapper tutorial](/manual/tutorial/ado-net-and-dapper)
- [Dapper](/manual/client-interfaces/dapper)
- [Configuration Reference](/manual/reference/configuration)
