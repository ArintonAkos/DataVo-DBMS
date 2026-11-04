# Error Handling Audit

> **Date:** 2026-11-05
> **Scope:** SQL through `DataVoContext.Execute`, in-memory and Lsm modes (results were identical in both).
> **Method:** 45 failure cases, each run against a freshly seeded database. For each case we recorded
> whether an error is reported (`QueryResult.IsError`), whether an exception escapes, whether data
> changed, and the message text.

## Status (2026-11-05)

All Critical, High, and Medium findings below are fixed, and the proposed contract is implemented
(see [Error Handling](../manual/client-interfaces/error-handling.md)). The cases are kept as permanent
tests in `DataVo.Tests/Errors`, run in both in-memory and Lsm mode. Found during the fixes and also
fixed: `WHERE Code >= '150'` compared numeric text incorrectly (returned every row), the `<>` operator
failed with an internal error, and `ALTER TABLE ... MODIFY COLUMN` silently truncated existing values
when shrinking a `VARCHAR`. Division by zero is an error for any zero divisor, as in PostgreSQL.

## Summary

| Severity | Count | Meaning |
| --- | --- | --- |
| Critical | 9 | Wrong or lost data, or a malformed statement that runs anyway, with no error |
| High | 8 | The failure is detected but reported as success (`IsError = false`) |
| Medium | 4 | Questionable semantics or poor message quality |
| Good | 19 | Clear error, correct state |

## Critical: silent data loss or malformed statements that run

| Case | What happens today |
| --- | --- |
| `DELETE FROM T WHERE` (nothing after `WHERE`) | **Deletes every row**, reports success |
| `SELECT * FROM T WHERE` | Returns every row, no error |
| `UPDATE T SET Code = 5 WHERE` | Reports "Rows affected: 0", no error |
| `INSERT ... VALUES (9, 'x', 9` (unclosed parenthesis) | Inserts the row |
| `CREATE TABLE X (Id INT PRIMARY KEY,` (unfinished) | Creates the table |
| `USE NoSuchDb` | Silently switches the session to a database that does not exist; later queries fail with misleading errors |
| `UPDATE T SET NoCol = 1 WHERE Id = 1` (unknown column) | Reports "Rows affected: 1", changes nothing |
| `INSERT ... 'muchtoolong'` into `VARCHAR(5)` | Silently truncates to `'mucht'` |
| Multi-row `INSERT` where the third row has a duplicate key | Inserts the first two rows; the statement is not atomic |

## High: failures reported as success

All of these return `IsError = false` with "Rows affected: 0" plus a message. A .NET caller cannot
tell them apart from a successful zero-row statement without parsing text (the EF bridge does exactly
that today).

- Duplicate primary key ("Primary key violation in row 1!")
- NULL primary key
- UNIQUE violation
- Foreign key to a missing parent row
- String into an `INT` column ("Type of argument doesn't match with column type")
- Vector dimension mismatch
- Empty statement: returns no result at all
- Constraint: `NOT NULL` is **not supported** in `CREATE TABLE` (parser error), so NULL can be stored in any non-key column

## Medium

- **Stack traces in every error message.** Errors are built with `Exception.ToString()`, so users get the
  full .NET stack trace including local source paths (for example `/Users/.../DataVo.Core/Parser/Parser.cs:line 164`).
- **Inconsistent prefixes.** Messages start with `DataVo.Core.Exceptions.ParserException: ...`, `Error: ...`,
  or nothing, depending on the statement.
- **Integer division by zero** returns `∞` instead of an error (or NULL).
- **`WHERE Id = 'abc'`** on an `INT` column silently matches nothing instead of reporting a type mismatch.

## Good (keep as is)

Lexer and parser errors give the token and the line and column; unknown table and column names are
named; duplicate database/table/index; wrong value counts; `COMMIT`/`ROLLBACK` without `BEGIN`; nested
`BEGIN`; deleting a referenced parent row; `UPDATE` producing a duplicate key (rejected atomically).

## Proposed Error Contract (for approval)

1. **Every failure is an error.** `QueryResult.IsError = true` for parse, binding, constraint, and type
   failures. "Rows affected: 0" means only that the statement succeeded and matched nothing.
2. **Structured error data.** `QueryResult` gains `ErrorCode` (stable string, for example
   `DV1001_SYNTAX`, `DV2001_UNKNOWN_TABLE`, `DV3001_PRIMARY_KEY_VIOLATION`) and, for syntax errors,
   `Line` and `Column`. Codes are documented and never reused.
3. **Clean messages.** One human sentence without exception type names or stack traces; the stack trace
   is still available to diagnostics logging. Unknown names get a "did you mean" suggestion when a close
   match exists.
4. **Statements are all-or-nothing.** A failed statement leaves no changes (multi-row `INSERT` included).
5. **No silent coercion.** Over-long strings and type mismatches in comparisons are errors, not
   truncation or empty results. Integer division by zero is an error.
6. **ADO.NET and EF.** `DataVoCommand` throws a `DataVoException` carrying `ErrorCode`; the EF bridge
   maps codes instead of parsing message text.

Breaking change: callers that relied on "Rows affected: 0" for constraint violations (the EF bridge,
possibly demos) are updated in the same change.
