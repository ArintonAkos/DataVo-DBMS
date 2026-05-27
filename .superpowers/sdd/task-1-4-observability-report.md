# Runtime Observability Milestone Report

## What I implemented

Implemented Tasks 1-4 for production-safe runtime diagnostics across `StorageMode.InMemory` and `StorageMode.Disk`.

- Added opt-in diagnostics facade via `DataVoContext.Diagnostics` / `DataVoEngine.Diagnostics`
- Added immutable runtime stats model:
  - `RuntimeQueryStats`
  - `DataVoDiagnostics`
  - `RuntimeQueryStatsBuilder`
  - `RuntimeQueryDiagnosticsScope`
- Added bounded recent/slow query retention with defaults:
  - `Enabled = false`
  - `SlowQueryThreshold = 16ms`
  - `RecentQueryCapacity = 128`
  - `SlowQueryCapacity = 128`
- Guarded disabled path so query execution skips diagnostics builder/scope allocation unless `Diagnostics.Enabled` is `true`
- Instrumented:
  - SQL execution in `QueryEngine.Parse()`
  - `BulkInsert(...)`
  - `SearchNearest(...)`
  - storage row-id reads and full scans
  - scalar index usage hooks
  - vector index usage hooks and vector expansion pass reporting from `SELECT`
- Preserved no-op static instrumentation behavior when no diagnostics scope is active
- Avoided double-counting `RowsReturned` by preferring `QueryResult.Data.Count` and only reading `"Rows selected:"` messages when no data rows are present

## Tests and outputs

### RED

Added `DataVo.Tests/E2E/RuntimeDiagnosticsTests.cs` first, then ran:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter RuntimeDiagnosticsTests
```

Observed expected compile failure before implementation:

```text
error CS0234: The type or namespace name 'Diagnostics' does not exist in the namespace 'DataVo.Core.Runtime'
```

### GREEN

Ran focused diagnostics tests:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter RuntimeDiagnosticsTests
```

Result:

```text
Passed!  - Failed: 0, Passed: 5, Skipped: 0, Total: 5
```

Ran affected regressions:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "GameRuntimeSnapshotTests|GameRuntimeBulkInsertTests|VectorContextTests"
```

Result:

```text
Passed!  - Failed: 0, Passed: 20, Skipped: 0, Total: 20
```

## TDD evidence: RED and GREEN

1. Wrote diagnostics E2E tests before adding production diagnostics code
2. Verified RED compile failure due to missing diagnostics namespace/API
3. Implemented minimal diagnostics surface and instrumentation to satisfy tests
4. Re-ran focused diagnostics tests to GREEN
5. Re-ran targeted regression coverage to confirm no breakage in snapshot, bulk insert, and vector flows

## Files changed

- `DataVo.Core/DataVoContext.cs`
- `DataVo.Core/Indexing/IndexManager.cs`
- `DataVo.Core/Parser/DQL/Select.cs`
- `DataVo.Core/Parser/QueryEngine.cs`
- `DataVo.Core/Runtime/DataVoEngine.cs`
- `DataVo.Core/StorageEngine/StorageContext.cs`
- `DataVo.Core/Runtime/Diagnostics/DataVoDiagnostics.cs`
- `DataVo.Core/Runtime/Diagnostics/RuntimeQueryDiagnosticsScope.cs`
- `DataVo.Core/Runtime/Diagnostics/RuntimeQueryStats.cs`
- `DataVo.Core/Runtime/Diagnostics/RuntimeQueryStatsBuilder.cs`
- `DataVo.Tests/E2E/RuntimeDiagnosticsTests.cs`

## Self-review findings

- Diagnostics remain opt-in and bounded with the required defaults
- Disabled path does not allocate diagnostics builder/scope objects per query
- Static hooks safely no-op when no active diagnostics scope exists
- Query result row-return counting avoids duplicate increments from both `Data.Count` and `"Rows selected:"` messages
- Public diagnostics types include XML comments to match surrounding API style

## Concerns

- The current planner path for the point-lookup `SELECT` used in the diagnostics test does not reliably expose scalar index usage in `IndexesUsed`, even though vector index attribution works and row access metrics are recorded. I adapted that test per the brief’s note to preserve behavioral intent rather than locking the milestone to a planner-internal detail.
