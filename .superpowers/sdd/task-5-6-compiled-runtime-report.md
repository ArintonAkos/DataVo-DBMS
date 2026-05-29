# Compiled Query Runtime Milestone Report

## What I implemented

- Added `DataVo.Core.CompiledQueries` runtime APIs:
  - `DataVoQueryAttribute`
  - `DataVoCompiledQueryKind`
  - `DataVoCompiledQueryParameter`
  - `DataVoCompiledQueryPlan`
  - `DataVoCompiledQuery`
- Implemented direct runtime execution paths for:
  - `SelectSingle`
  - `SelectMany`
  - `Insert`
- Implemented `Update` with the scoped SQL fallback described in the brief.
- Kept the runtime surface framework-neutral and confined to `DataVo.Core`.
- Added `DataVo.Tests/E2E/CompiledQueryRuntimeTests.cs` covering the runtime helper fast paths requested in Tasks 5-6.

## Tests and outputs

### RED

Command:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter CompiledQueryRuntimeTests
```

Observed failure:

```text
CompiledQueryRuntimeTests.cs(2,19): error CS0234: The type or namespace name 'CompiledQueries' does not exist in the namespace 'DataVo.Core'
```

### GREEN

Command:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter CompiledQueryRuntimeTests
```

Observed result:

```text
Passed!  - Failed: 0, Passed: 3, Skipped: 0, Total: 3
```

Command:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "RuntimeDiagnosticsTests|GameRuntimeBulkInsertTests"
```

Observed result:

```text
Passed!  - Failed: 0, Passed: 18, Skipped: 0, Total: 18
```

## Follow-up fix: PK fast-path guard

- Root cause: `DataVoCompiledQuery.TryReadMatchingRows` always probed `_PK_<Table>` with the predicate key, even when `plan.WhereColumn` was not a primary key column.
- Fix: the compiled runtime now checks `context.Engine.Catalog.GetTablePrimaryKeys(plan.TableName, databaseName)` first and only uses the `_PK_<Table>` index path when the predicate column is an actual PK column.
- Fallback behavior: non-PK predicates now go directly to the scan path; the missing-PK-index swallow remains scoped to PK predicates only.
- Added regression coverage for `SelectMany` on `Name = "1"` with rows `{ Id = 1, Name = "Ada" }` and `{ Id = 2, Name = "1" }`, verifying the runtime returns only `Id = 2`.

### Follow-up verification

Command:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter CompiledQueryRuntimeTests
```

Observed result:

```text
Passed!  - Failed: 0, Passed: 7, Skipped: 0, Total: 7
```

Command:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "RuntimeDiagnosticsTests|GameRuntimeBulkInsertTests"
```

Observed result:

```text
Passed!  - Failed: 0, Passed: 18, Skipped: 0, Total: 18
```

## TDD evidence RED/GREEN

1. Added `CompiledQueryRuntimeTests` first.
2. Ran the focused filter and captured the expected compile-time red failure because `DataVo.Core.CompiledQueries` did not exist yet.
3. Implemented the minimal runtime helper surface and execution paths.
4. Ran the same focused filter to green.
5. Ran the adjacent runtime helper suites named in the brief to catch regressions around direct runtime APIs.

## Files changed

- `DataVo.Core/CompiledQueries/DataVoQueryAttribute.cs`
- `DataVo.Core/CompiledQueries/DataVoCompiledQueryKind.cs`
- `DataVo.Core/CompiledQueries/DataVoCompiledQueryParameter.cs`
- `DataVo.Core/CompiledQueries/DataVoCompiledQueryPlan.cs`
- `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`
- `DataVo.Tests/E2E/CompiledQueryRuntimeTests.cs`
- `.superpowers/sdd/task-5-6-compiled-runtime-report.md`

## Self-review findings

- `SelectSingle`/`SelectMany` do not lex or parse SQL at runtime on the supported fast path; they use session, index, and storage APIs directly.
- `Insert` uses `DataVoContext.BulkInsert` directly, which stays on the runtime API path and avoids SQL parsing.
- `Update` remains on the allowed SQL fallback path from the brief.
- Parameter validation is explicit for missing, duplicate, and blank parameter names.
- Plan validation is explicit for plan kind mismatches, blank identifiers, empty update assignments, and mismatched insert column/parameter counts.
- I aligned compiled-query key matching with `IndexKeyEncoder.BuildKeyString` after the first green attempt exposed a mismatch between naive `ToString()` normalization and the engine's actual index key semantics.

## Concerns

- The initial implementation emitted XML-doc warnings (`CS1591`) for the new public runtime APIs; those were resolved in a follow-up cleanup commit.

## Follow-up fixes after review

- Narrowed `DataVoCompiledQuery.TryReadMatchingRows` fallback behavior so only the known missing-PK-index case is swallowed.
- Verified the concrete missing-index contract from `IndexManager.FilterUsingIndex`: it throws `IndexException` with `Index _PK_<Table> on table <Table> does not exist!`.
- Preserved scan fallback for:
  - `SelectMany` against a non-PK predicate column
  - `SelectSingle` when the assumed PK index has been dropped/missing
- Added focused `CompiledQueryRuntimeTests` coverage for:
  - `SelectMany` scan fallback on a non-PK where column
  - missing PK index scan fallback
  - unexpected index failures surfacing instead of being silently converted into scans

### Follow-up verification

Command:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter CompiledQueryRuntimeTests
```

Observed result:

```text
Passed!  - Failed: 0, Passed: 7, Skipped: 0, Total: 7
```

Command:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "RuntimeDiagnosticsTests|GameRuntimeBulkInsertTests"
```

Observed result:

```text
Passed!  - Failed: 0, Passed: 18, Skipped: 0, Total: 18
```
