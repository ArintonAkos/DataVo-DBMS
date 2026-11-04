# Error Handling

Every failed statement returns a `QueryResult` with `IsError = true`, one readable message, and a stable `ErrorCode`. Syntax errors also carry `ErrorLine` and `ErrorColumn`. Messages may be reworded between releases; codes never change meaning, so branch on the code.

```csharp
QueryResult result = db.Execute("INSERT INTO Players (Id, Name) VALUES (1, 'Ada')").Single();
if (result.IsError && result.ErrorCode == DataVoErrorCodes.PrimaryKeyViolation)
{
    // the player already exists
}
```

A statement is all-or-nothing: when any row of an `INSERT`, `UPDATE`, or bulk insert is rejected, nothing from that statement is written.

Typed and bulk APIs (`InsertTyped`, `InsertTypedBatch`, `BulkInsert`) throw `ConstraintViolationException`, and ADO.NET throws `DataVo.Data.DataVoException`; both expose the same `ErrorCode`. Entity Framework failures surface as `DataVoEfException` with `ErrorCode` taken from the underlying engine error.

## Error Codes

| Code | Meaning |
| --- | --- |
| `DV1001_SYNTAX` | The statement is not valid SQL, including incomplete statements such as `DELETE FROM T WHERE`. `ErrorLine` and `ErrorColumn` point at the offending token. |
| `DV1002_EMPTY_STATEMENT` | The input contains no statement. |
| `DV2001_NO_DATABASE_SELECTED` | Run `USE <database>` first. |
| `DV2002_UNKNOWN_DATABASE` | The database does not exist. `USE` keeps the current database. |
| `DV2003_UNKNOWN_TABLE` | The table does not exist in the current database. |
| `DV2004_UNKNOWN_COLUMN` | The column does not exist in the table. |
| `DV2005_UNKNOWN_INDEX` | The index does not exist. |
| `DV2006_AMBIGUOUS_COLUMN` | An unqualified column matches several joined tables; qualify it. |
| `DV2007_UNKNOWN_TABLE_ALIAS` | A qualified name refers to a table or alias that is not in the query. |
| `DV2101_DATABASE_EXISTS` | A database with that name already exists. |
| `DV2102_TABLE_EXISTS` | A table with that name already exists. |
| `DV2103_INDEX_EXISTS` | An index with that name already exists on the table. |
| `DV3001_PRIMARY_KEY_VIOLATION` | A row repeats an existing primary key. |
| `DV3002_NULL_PRIMARY_KEY` | A primary key value is NULL. |
| `DV3003_UNIQUE_VIOLATION` | A row repeats a value of a UNIQUE column. |
| `DV3004_FOREIGN_KEY_VIOLATION` | A row references a missing parent row, or a referenced parent row would be deleted. |
| `DV3005_NOT_NULL_VIOLATION` | A NOT NULL column would hold NULL. |
| `DV4001_TYPE_MISMATCH` | A value does not match the column type, for example `WHERE Id = 'abc'` on an `INT` column. |
| `DV4002_VALUE_TOO_LONG` | A string is longer than its `VARCHAR(n)` column. Values are never truncated. |
| `DV4003_VALUE_COUNT_MISMATCH` | The number of values does not match the number of columns. |
| `DV4004_DIVISION_BY_ZERO` | Division by zero. |
| `DV5001_NO_ACTIVE_TRANSACTION` | `COMMIT` or `ROLLBACK` without `BEGIN`. |
| `DV5002_TRANSACTION_ALREADY_ACTIVE` | `BEGIN` while a transaction is already active in the session. |
| `DV5003_DEADLOCK` | The statement was chosen as a deadlock victim; retry it. |
| `DV6001_PERMISSION_DENIED` | The session may not perform the statement. |
| `DV7001_DATABASE_IN_USE` | The data directory is held by another process or opened with incompatible settings. |
| `DV2900_BINDING`, `DV2901_CATALOG`, `DV2902_INDEX`, `DV4900_EVALUATION`, `DV7900_STORAGE`, `DV9000_ERROR` | Other failures of that category without a more specific code yet. |
| `DV9001_INTERNAL` | An unexpected internal failure. Please report it with the statement that caused it. |
