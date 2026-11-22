---
title: Error Handling
description: Stable DV1001–DV9001 error codes, QueryResult diagnostics, and exception types across DataVo.Core, ADO.NET, and EF Core.
---

# Error Handling

Every failed SQL statement in `DataVoContext.Execute` returns a `QueryResult` with `IsError = true`, a human-readable message in `Messages`, and a stable `ErrorCode` from `DataVoErrorCodes`. Syntax errors also populate `ErrorLine` and `ErrorColumn`. Always branch on `ErrorCode` rather than message text.

```csharp
using DataVo.Core.Contracts.Results;

QueryResult result = db.Execute(
    "INSERT INTO players (id, name, score) VALUES (1, 'Ada', 1200)")[0];

if (result.IsError && result.ErrorCode == DataVoErrorCodes.PrimaryKeyViolation)
{
    Console.WriteLine("Player 1 already exists.");
}
```

## Error Surfaces by API

Every DML statement and bulk insert is all-or-nothing: if any row violates a constraint, nothing from that statement is written.

| API Layer | Failure Surface | Error Code Property |
| --- | --- | --- |
| `DataVoContext.Execute` / `Prepare` | `QueryResult` (`IsError = true`) | `result.ErrorCode`, `ErrorLine`, `ErrorColumn` |
| `Insert<T>`, `InsertTyped`, `InsertTypedBatch`, `BulkInsert` | `ConstraintViolationException` | `ex.ErrorCode` |
| `DataVo.Data` (ADO.NET / Dapper) | `DataVo.Data.DataVoException` | `ex.ErrorCode`, `ex.Line`, `ex.Column` |
| `DataVo.EntityFrameworkCore` | `DataVoEfException` | `ex.ErrorCode` (propagated from the engine or EF bridge) |

```csharp
try
{
    cmd.ExecuteNonQuery();
}
catch (DataVo.Data.DataVoException ex) when (ex.ErrorCode == DataVoErrorCodes.Deadlock)
{
    // Retry transaction after deadlock victim selection
}
```

## Error Code Catalog (`DataVoErrorCodes`)

### Syntax & Input (`DV1xxx`)

| Code | Constant | Meaning |
| --- | --- | --- |
| `DV1001_SYNTAX` | `Syntax` | Invalid or incomplete SQL. Check `ErrorLine` and `ErrorColumn`. |
| `DV1002_EMPTY_STATEMENT` | `EmptyStatement` | The input string contained no executable statement. |

### Binding & Catalog (`DV2xxx`)

| Code | Constant | Meaning |
| --- | --- | --- |
| `DV2001_NO_DATABASE_SELECTED` | `NoDatabaseSelected` | No active database; run `USE <database>` first. |
| `DV2002_UNKNOWN_DATABASE` | `UnknownDatabase` | Referenced database does not exist (`USE` keeps the prior selection). |
| `DV2003_UNKNOWN_TABLE` | `UnknownTable` | Table does not exist in the active database. |
| `DV2004_UNKNOWN_COLUMN` | `UnknownColumn` | Column does not exist on the target table. |
| `DV2005_UNKNOWN_INDEX` | `UnknownIndex` | Index does not exist. |
| `DV2006_AMBIGUOUS_COLUMN` | `AmbiguousColumn` | Unqualified column matches multiple joined tables; qualify with table/alias. |
| `DV2007_UNKNOWN_TABLE_ALIAS` | `UnknownTableAlias` | Qualified reference names a table or alias not in the query. |
| `DV2101_DATABASE_EXISTS` | `DatabaseExists` | Database already exists. |
| `DV2102_TABLE_EXISTS` | `TableExists` | Table already exists. |
| `DV2103_INDEX_EXISTS` | `IndexExists` | Index already exists on the table. |
| `DV2900_BINDING` | `Binding` | Uncategorized binding error. |
| `DV2901_CATALOG` | `Catalog` | Uncategorized catalog error. |
| `DV2902_INDEX` | `Index` | Uncategorized index error. |

### Constraints (`DV3xxx`)

| Code | Constant | Meaning |
| --- | --- | --- |
| `DV3001_PRIMARY_KEY_VIOLATION` | `PrimaryKeyViolation` | Duplicate primary key value. |
| `DV3002_NULL_PRIMARY_KEY` | `NullPrimaryKey` | Primary key column received `NULL`. |
| `DV3003_UNIQUE_VIOLATION` | `UniqueViolation` | Duplicate value in a `UNIQUE` column. |
| `DV3004_FOREIGN_KEY_VIOLATION` | `ForeignKeyViolation` | Missing parent row or blocked parent delete. |
| `DV3005_NOT_NULL_VIOLATION` | `NotNullViolation` | `NOT NULL` column received `NULL`. |

### Type & Evaluation (`DV4xxx`)

| Code | Constant | Meaning |
| --- | --- | --- |
| `DV4001_TYPE_MISMATCH` | `TypeMismatch` | Value does not match column type (e.g., `'abc'` for `INT`). |
| `DV4002_VALUE_TOO_LONG` | `ValueTooLong` | String exceeds `VARCHAR(n)` length (values are never silently truncated). |
| `DV4003_VALUE_COUNT_MISMATCH` | `ValueCountMismatch` | `VALUES` count does not match target column count. |
| `DV4004_DIVISION_BY_ZERO` | `DivisionByZero` | Division by zero during expression evaluation. |
| `DV4900_EVALUATION` | `Evaluation` | Uncategorized expression evaluation error. |

### Transactions, Auth, Storage & Internal (`DV5xxx`–`DV9xxx`)

| Code | Constant | Meaning |
| --- | --- | --- |
| `DV5001_NO_ACTIVE_TRANSACTION` | `NoActiveTransaction` | `COMMIT` or `ROLLBACK` called without an active `BEGIN`. |
| `DV5002_TRANSACTION_ALREADY_ACTIVE` | `TransactionAlreadyActive` | `BEGIN` called while a transaction is already active in the session. |
| `DV5003_DEADLOCK` | `Deadlock` | Transaction selected as deadlock victim; safe to retry. |
| `DV6001_PERMISSION_DENIED` | `PermissionDenied` | Active session lacks required privileges. |
| `DV7001_DATABASE_IN_USE` | `DatabaseInUse` | Data directory is locked by another process or opened with incompatible settings. |
| `DV7900_STORAGE` | `Storage` | Uncategorized storage I/O or engine failure. |
| `DV9000_ERROR` | `General` | General unclassified error. |
| `DV9001_INTERNAL` | `Internal` | Unexpected internal failure; please file a bug report with the offending statement. |

## See Also

- [DataVoContext API](/manual/client-interfaces/datavo-context-api): Checking `QueryResult` on SQL execution
- [ADO.NET Provider](/manual/client-interfaces/ado-net): Catching `DataVoException`
- [Limits & Unsupported Features](/manual/reference/limits): Known engine and SQL dialect boundaries
