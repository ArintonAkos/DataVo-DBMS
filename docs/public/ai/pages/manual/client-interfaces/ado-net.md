# ADO.NET Provider (`DataVo.Data`)

> Source route: /manual/client-interfaces/ado-net
> Source file: manual/client-interfaces/ado-net.md

`DataVo.Data` implements the standard `System.Data.Common` abstractions (`DbConnection`, `DbCommand`, `DbDataReader`, `DbTransaction`, `DbParameter`) over the in-process DataVo engine. Any library targeting `IDbConnection`, including [Dapper](/manual/client-interfaces/dapper), works without custom adapters.

```csharp
using DataVo.Data;

using var conn = new DataVoConnection("StorageMode=Lsm;DataSource=./game-data");
conn.Open();

using var cmd = conn.CreateCommand();
cmd.CommandText = "CREATE TABLE IF NOT EXISTS players (id INT PRIMARY KEY, name VARCHAR(80), score INT)";
cmd.ExecuteNonQuery();

cmd.CommandText = "INSERT INTO players (id, name, score) VALUES (@id, @name, @score)";
cmd.Parameters.AddWithValue("@id", 1);
cmd.Parameters.AddWithValue("@name", "Ada");
cmd.Parameters.AddWithValue("@score", 1200);
cmd.ExecuteNonQuery();
```

## Connection Strings & Lifecycle

Use `DataVoConnectionStringBuilder` or a semicolon-delimited string with three supported keys:

| Key | Default | Description |
| --- | --- | --- |
| `StorageMode` | `InMemory` | `InMemory` or `Lsm`. Any other value throws `ArgumentException`. |
| `DataSource` | `"datavo"` | Database name (and storage directory path for `Lsm`). |

When `conn.Open()` runs, it initializes the embedded engine, executes `CREATE DATABASE IF NOT EXISTS` for the `DataSource` database name, and selects it with `USE`. `Close()` rolls back any uncommitted transaction and releases the session.

## Provider Types

| `System.Data.Common` Base | DataVo Type | Notes |
| --- | --- | --- |
| `DbConnection` | `DataVoConnection` | In-process engine handle; no network socket or connection pool. |
| `DbCommand` | `DataVoCommand` | Text commands with `@param` placeholders and `Prepare()`. |
| `DbDataReader` | `DataVoDataReader` | Forward-only reader over materialized command results. |
| `DbTransaction` | `DataVoTransaction` | Supports `Commit()`, `Rollback()`, and savepoints (`Save`, `Rollback(name)`, `Release`). |
| `DbParameter` | `DataVoParameter` | Supports `Parameters.AddWithValue("@name", value)`. |
| `DbConnectionStringBuilder` | `DataVoConnectionStringBuilder` | Strongly-typed builder for `StorageMode` and `DataSource`. |
| `Exception` | `DataVoException` | Exposes `ErrorCode`, `Line`, and `Column`. |

`DataVo.Data` does **not** ship a `DbProviderFactory`.

## Transactions & Prepared Commands

```csharp
using var tx = conn.BeginTransaction();

using var update = conn.CreateCommand();
update.CommandText = "UPDATE players SET score = @score WHERE id = @id";
update.Parameters.AddWithValue("@score", 1500);
update.Parameters.AddWithValue("@id", 1);
update.Prepare();
update.ExecuteNonQuery();

tx.Save("checkpoint1");
tx.Rollback("checkpoint1");
tx.Commit();
```

Calling `Prepare()` (or using parameterized commands) routes execution through DataVo's prepared-statement cache so repeated executions skip SQL parsing.

## Behavior Notes

- **Materialized reader:** `ExecuteReader()` executes the statement and walks materialized rows; for zero-allocation streaming, use `DataVoContext.StreamRef()` or compiled LINQ.
- **Metadata before `Read()`:** `GetName` and `GetFieldType` are available before the first `Read()` call. Column types are inferred from the first non-null value (`object` if all values are null).
- **Synchronous async wrappers:** `ExecuteReaderAsync` and other async methods complete synchronously on the calling thread because storage is in-process.
- **Timeouts and cancellation:** `Cancel()` is a no-op and `CommandTimeout` does not abort running statements; lock waits are bounded by `LockAcquireTimeoutMs`.
- **Single-process directory ownership:** Multiple `DataVoConnection` instances in the same process share one engine for a given directory; a second process opening the same directory fails with `DV7001_DATABASE_IN_USE`.

## See Also

- [Dapper](/manual/client-interfaces/dapper): Using Dapper over `DataVoConnection`
- [Error Handling](/manual/reference/error-handling): `DataVoException` and `DV*` error codes
- [Native AOT Deployment](/manual/client-interfaces/native-aot): Trim and AOT compatibility for `DataVo.Data`
