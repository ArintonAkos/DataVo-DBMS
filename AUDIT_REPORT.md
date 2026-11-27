# DataVo-DBMS Repository-Wide Read-Only Audit Report

**Audit Date:** 2026-11-26  
**Target Version:** `0.5.1` (`Directory.Build.props`)  
**Scope:** Full read-only audit of `DataVo.Core`, `DataVo.Data`, `DataVo.EntityFrameworkCore`, `DataVo.VectorData`, `DataVo.Generators`, `DataVo.Browser`, `DataVo.AotSmoke`, `integrations/unity`, `tools/`, `examples/`, `demos/`, `benchmarks/`, `tests/`, `DataVo.Tests/`, `.github/workflows/`, `scripts/`, `docs/`, `README.md`, `CLAUDE.md`, and root build/config files.  
**Policy Baseline:** Zero backward compatibility is required for legacy deployments; `netstandard2.1` / Unity constraints are evaluated separately based on active consumption by `integrations/unity`, `tools/DataVo.Portable*`, and `tests/DataVo.GeneratedCodeTargets`.

---

## 1. Executive Summary

| Subsystem | Backward Compatibility (`BC-*`) | Unfinished / Loose Ends (`LE-*`) | Total | Item Ranges |
| :--- | ---: | ---: | ---: | :--- |
| `DataVo.Core` | 18 | 16 | 34 | `BC-01`–`BC-18`, `LE-01`–`LE-16` |
| `DataVo.Data` | 2 | 6 | 8 | `BC-19`–`BC-20`, `LE-17`–`LE-22` |
| `DataVo.EntityFrameworkCore` | 2 | 6 | 8 | `BC-21`–`BC-22`, `LE-23`–`LE-28` |
| `DataVo.VectorData` | 1 | 5 | 6 | `BC-23`, `LE-29`–`LE-33` |
| `DataVo.Generators` | 2 | 4 | 6 | `BC-24`–`BC-25`, `LE-34`–`LE-37` |
| `DataVo.Browser` | 2 | 3 | 5 | `BC-26`–`BC-27`, `LE-38`–`LE-40` |
| `integrations/unity` | 2 | 5 | 7 | `BC-28`–`BC-29`, `LE-41`–`LE-45` |
| `docs/` | 8 | 7 | 15 | `BC-30`–`BC-37`, `LE-46`–`LE-52` |
| `CI/scripts` *(incl. Tests, Tools, Demos, Examples)* | 10 | 8 | 18 | `BC-38`–`BC-47`, `LE-53`–`LE-60` |
| **Total** | **47** | **60** | **107** | `BC-01`–`BC-47`, `LE-01`–`LE-60` |

### Global Explicit Marker Inventory

| Marker | Occurrences in Production & Test Code | Notes |
| :--- | :---: | :--- |
| `TODO` / `FIXME` / `HACK` / `XXX` / `WIP` | **0** | Clean across all tracked `.cs`, `.csproj`, `.sh`, `.yml`, `.ts`, `.js`, `.cjs` files. |
| `[Obsolete]` | **0** | Zero `[Obsolete]` attributes anywhere in the repository. |
| `NotImplementedException` | **10** (2 files) | Exclusively in unreferenced benchmark stubs `PostgresEngine.cs:11-24` and `RedisStreamEngine.cs:11-24` (`LE-58`). |
| `NotSupportedException` | **~90+** | Mix of intentional guardrails and unfinished follow-ups — every non-trivial group is cataloged under `LE-*` below. |
| `[Fact(Skip = ...)]` / `[Theory(Skip = ...)]` / Silent early `return;` | **12** | 1 custom skip attribute (`Stage2BHotPathProbeTests.cs`, `BC-44`), 10 silent early-return HNSW perf tests (`HNSWIndexTests.cs`, `LE-57`), 1 silent early-return `sqlite-vec` test (`VectorSearchEngineTests.cs`, `LE-57`), plus 1 dead Playwright `test.skip(true)` (`datavo-wasm-needs-specific.spec.ts`, `BC-36`) and 3 conditional Playwright skips (`datavo-wasm.spec.ts:502,517,560`). |

### Verified Active Compatibility Shims (`netstandard2.1` / Unity / Roslyn 4.10 — DO NOT DELETE)

Before listing removable legacy items, the following compatibility seams were audited and verified as **actively required** by the shipped `netstandard2.1` TFM (`DataVo.Core`, `DataVo.Data`, `tests/DataVo.GeneratedCodeTargets`, `tools/DataVo.Portable*`, `integrations/unity`) and Unity 6's Roslyn 4.10 compiler:

- **`DataVo.Core/Compat/` (active):** `ThrowHelper.{Modern,Portable}.cs` (172 refs), `MethodImplCompat.cs` (31 refs), `ScalarKeyFormatter.{Modern,Portable}.cs` (18 refs), `MonotonicClock.{Modern,Portable}.cs` (16 refs), `DataVoPriorityQueue.{Modern,Portable}.cs` (11 refs), `DateObjectAdapter.{Modern,Portable}.cs` (7 refs) & `DataVoDate.{cs,Modern.cs}`, `FileCompat.{Modern,Portable}.cs` (7 refs). *(Exception: `TaskCancellationWaiter.{Modern,Portable}.cs` has **0 production callers** — see `BC-07`.)*
- **`DataVo.Core/Internal/Polyfills/` (active):** `RandomAccessCompat.{Modern,Portable}.cs` (11 refs in `FileHandlePool`/`WalFileStore`), `BitOperationsCompat.cs` (3 refs in `Memory/Flat/Bitmask.cs`), `NetStandard21Polyfills.cs` (compiler/trimming attributes for `netstandard2.1`).
- **`DataVo.Core` Modern/Portable splits (active):** `PasswordHashPlatform.{Modern,Portable}.cs`, `SessionSecurityStore.{Modern,Portable}.cs`, `CompiledIntegerScan.{Modern,Portable}.cs`, `CompiledRowReader.Modern.cs`, `LsmDurableFileOperations.{Modern,Portable}.cs`, `DataVoConfigSharing.cs:35-46`.
- **`DataVo.Data` (active):** `DataVo.Data/Internal/Polyfills/NetStandard21Polyfills.cs`, `DataVo.Data/DataVoTransaction.cs:47-80` (`#if NET8_0_OR_GREATER` savepoint overrides).
- **`DataVo.Generators` (active):** `netstandard2.0` + Roslyn 4.10 pin (`DataVo.Generators.csproj:6,31-32`), `IsExternalInit` (`Tables/TableModel.cs:6-10`), reflection lookup of `GetInterceptableLocation` with path/line/column fallback (`Linq/DataVoLinqAnalyzer.cs:23-26,297-321`), dual `InterceptsLocationAttribute` constructors & `file`/`internal` fallback (`Linq/DataVoLinqEmitter.cs:23-36`), `InterceptorsPreviewNamespaces` (`DataVoLinqInterceptorGenerator.cs:123`), and `[ModuleInitializer]` guard (`DataVoTableGenerator.cs:40-43`).

---

## 2. Section A — Backward Compatibility & Legacy Items (`BC-*`)

### Subsystem: `DataVo.Core`

#### [DONE] `BC-01` — Legacy Row-Keyed SSTable Formats (v2 Fixed & v4 Compact), `UnknownLiveRowCount`, & Unused `expectedEntries`
- **Exact Location(s):**
  - `DataVo.Core/StorageEngine/Lsm/V2/SsTableV2Format.cs:36-51,75-113,124-130,164-249`
  - `DataVo.Core/StorageEngine/Lsm/V2/SsTableV2Writer.cs:8-21,65,82-120,155-174,293-300,499`
  - `DataVo.Core/StorageEngine/Lsm/V2/SsTableV2Reader.cs:11-28,47-57,107-112,175-178,250-300,473,523-524,826-827`
  - `DataVo.Core/StorageEngine/Lsm/V2/LsmRowKeyCodec.cs:67-88`
  - `DataVo.Core/StorageEngine/Lsm/LsmStorageEngine.cs:950-988`
  - `DataVo.Tests/Lsm/LsmWideRowKeyTests.cs:174-235`, `SsTableV2Tests.cs`, `LsmBlockCacheTests.cs:262`
  - `docs/manual/getting-started/roadmap.md:15`
- **What it is & Why it's there:** `SsTableV2Format` carries four row-keyed SSTable versions: `Version = 2` (fixed 24-byte entries, 8-byte int keys), `CompactVersion = 4` (delta/varint, 8-byte int keys), `WideVersion = 6` (16-byte `RowKey`), and `ColumnarVersion = 8` (v6 + columnar segment). Production (`LsmTable`, `LsmCompactor`) exclusively writes v6/v8 (`SsTableRowFormat.Wide`). `SsTableV2Writer.cs:84-85` explicitly notes v2/v4 remain only for reading old files and format tests, and `int expectedEntries = 0` (`:82,95,120`) is explicitly discarded (`_ = expectedEntries; // kept for callers`). `SsTableV2Properties.UnknownLiveRowCount = -1` and `LsmRowKeyCodec.DecodeTableMetadata` retain 8-byte metadata decoding for *"files upgraded from v1"*, triggering full-table recount fallbacks in `LsmStorageEngine.LoadTableMetadata`.
- **Proposed Action:** Delete `SsTableRowFormat.Fixed` and `SsTableRowFormat.Compact` writer paths, v2/v4 reader branches, 8-byte manifest/metadata fallbacks, `UnknownLiveRowCount`, the unused `expectedEntries` parameter, and the obsolete v1 docstring mentions. Update `roadmap.md:15` and format tests accordingly.
- **Risk / Dependencies:** Breaks reading pre-v6 SSTables on disk (explicitly acceptable per zero-BC policy). Touches `SsTableV2Tests.cs`, `LsmWideRowKeyTests.cs`, and `LsmBlockCacheTests.cs`.
- **Effort Estimate:** `M`

#### [DONE] `BC-02` — Legacy Uncompressed Keyed SSTable Format v3 Alongside v5 (Compact)
- **Exact Location(s):**
  - `DataVo.Core/StorageEngine/Lsm/V2/SsTableKeyed.cs:28-33,159,164-172,202-209,335,348,433,593-595,679,817,938,1055-1134`
  - `DataVo.Tests/Lsm/SsTableKeyedTests.cs:230`
- **What it is & Why it's there:** `SsTableKeyedFormat` maintains both `Version = 3` (uncompressed fixed-offset blocks) and `CompactVersion = 5` (prefix-compressed `SsTableCompactKeyedBlock`). Every production call site (`LsmStorageEngine.Indexes.cs:345`, `LsmTable.cs:680`, `LsmCompactor.cs:256`) uses default `compact: true` (v5). Only `SsTableKeyedTests.cs:230` passes `compact: false`.
- **Proposed Action:** Remove v3 writer path (`AddFixed`, `_compact` flag) and v3 reader/cursor branches in `SsTableKeyed.cs`, making v5 the sole keyed SSTable format; remove v3 test case in `SsTableKeyedTests.cs`.
- **Risk / Dependencies:** Low. Index SSTables are derivable from row tables anyway (and `BUILT.v2` already forces index rebuilds on older layouts).
- **Effort Estimate:** `S`

#### [DONE] `BC-03` — Dead v1 `BloomFilter` Superseded by `V2/BlockedBloomFilter`
- **Exact Location(s):**
  - `DataVo.Core/StorageEngine/Lsm/BloomFilter.cs:1-142`
  - `DataVo.Tests/Lsm/BloomFilterTests.cs`
- **What it is & Why it's there:** Standalone v1 `BloomFilter` class using custom serialization, completely superseded by `DataVo.Core/StorageEngine/Lsm/V2/BlockedBloomFilter.cs`. Has **0 callers** anywhere in `DataVo.Core/`.
- **Proposed Action:** Delete `BloomFilter.cs` and `BloomFilterTests.cs`.
- **Risk / Dependencies:** Public class in `DataVo.Core.StorageEngine.Lsm`, but zero internal or external callers within the solution.
- **Effort Estimate:** `XS`

#### [DONE] `BC-04` — Dead Per-Table LSM WAL Infrastructure (`LsmWalWriter`, `LsmWalRecordCodec`, `LsmTable` WAL Segments)
- **Exact Location(s):**
  - `DataVo.Core/StorageEngine/Lsm/LsmWalWriter.cs:1-213`
  - `DataVo.Core/StorageEngine/Lsm/LsmWalRecordCodec.cs:1-207`
  - `DataVo.Core/StorageEngine/Lsm/LsmTable.cs:106,115-120,140-147,163-167,176-182,211-243,425,1034-1115`
  - `DataVo.Core/Transactions/WalFrameFormat.cs:8-18` (`Insert = 1`, `Delete = 3`, gaps at `2`, `4`, `5`; unused `Flags`/`TableId`)
  - `DataVo.Tests/Lsm/LsmWalRecordCodecTests.cs`, `LsmTableTests.cs`, `LsmBackgroundPipelineTests.cs:99-131`
- **What it is & Why it's there:** `LsmStorageEngine` migrated to the engine-wide shared `LsmCommitLog` (`commit.wal`) using `WalFrameOperationType.CommitBatch = 6`. Every production instantiation of `LsmTable` passes `walPath: null` (`LsmStorageEngine.cs:925-927`, `LsmStorageEngine.Indexes.cs:502`). The entire per-table WAL writer, codec, segment rotation/recovery machinery (`RecoverWalSegments`, `CreateNextWalSegmentNoLock`, `CleanupRecoveredWalsNoLock`, `PutDeferDurability`, `DeleteDeferDurability`), `LsmWalWriter.FlushBufferedMutations()` (0 callers even in tests), and `WalFrameOperationType.Insert`/`Delete` are completely unused in production.
- **Proposed Action:** Delete `LsmWalWriter.cs`, `LsmWalRecordCodec.cs`, and per-table WAL paths in `LsmTable.cs`; simplify `WalFrameOperationType` / `WalFrameFormat` to only what `LsmCommitLog` uses; prune or adapt the 3 unit test files.
- **Risk / Dependencies:** `LsmTable` and `LsmWalRecordCodec` are `public`, so removing per-table WAL is a public surface cleanup for standalone `LsmTable` callers (none exist in-repo outside unit tests).
- **Effort Estimate:** `M`

#### [DONE] `BC-05` — Dead Process-Wide Static `Catalog` Class Duplicating `CatalogStore`
- **Exact Location(s):**
  - `DataVo.Core/Models/Catalog/Catalog.cs:13-728`
  - `DataVo.Core/Runtime/EngineCatalog.cs:10-15` (stale XML remarks claiming it delegates to `Models.Catalog.Catalog`)
  - `DataVo.Core/Runtime/Catalog/CatalogXml.cs:9`
- **What it is & Why it's there:** A 728-line unsynchronized static `Catalog` class left over from before engine-scoped `Runtime/CatalogStore.cs` (`internal sealed class CatalogStore`). Its static constructor still creates `databases/Catalog.xml` if touched, and it lacks newer schema operations (`AddColumn`, `DropColumn`, `ModifyColumn`, `IsTableClustered`, caching). Has **0 live code callers** anywhere in the repository.
- **Proposed Action:** Delete `DataVo.Core/Models/Catalog/Catalog.cs` and update stale XML comments in `EngineCatalog.cs:10-15` and `CatalogXml.cs:9`.
- **Risk / Dependencies:** `public static class Catalog` is part of the public API surface, but completely unused internally and unmentioned in `docs/manual/`.
- **Effort Estimate:** `XS`

#### [DONE] `BC-06` — Dead Legacy Session Store `CacheStorage`
- **Exact Location(s):**
  - `DataVo.Core/Cache/CacheStorage.cs:1-51`
  - `DataVo.Tests/AuditFixes/AuditFixTests.cs:20` (stale doc comment)
- **What it is & Why it's there:** `internal static class CacheStorage` explicitly documented as storing process-local session state for legacy execution flows before `SessionDatabaseStore`. Has **0 code callers** anywhere in the repository.
- **Proposed Action:** Delete `DataVo.Core/Cache/CacheStorage.cs` and the empty `DataVo.Core/Cache/` directory; clean up the comment in `AuditFixTests.cs:20`.
- **Risk / Dependencies:** None (`internal`, zero references).
- **Effort Estimate:** `XS`

#### [DONE] `BC-07` — Obsolete `netstandard2.1` Compat Seam `TaskCancellationWaiter`
- **Exact Location(s):**
  - `DataVo.Core/Compat/TaskCancellationWaiter.Modern.cs:1-8`
  - `DataVo.Core/Compat/TaskCancellationWaiter.Portable.cs:1-32`
  - `DataVo.Tests/Compat/TaskCancellationWaiterTests.cs`
  - `tools/DataVo.PortablePackageSmoke/Program.cs:259,265`
- **What it is & Why it's there:** Compatibility wrapper for `Task.WaitAsync(CancellationToken)` originally used by the removed `GroupCommitter.cs` (deleted with `StorageMode.Disk`). Now has **0 production callers** across `DataVo.Core` and all other libraries.
- **Proposed Action:** Delete both `TaskCancellationWaiter.*.cs` files, `TaskCancellationWaiterTests.cs`, and the smoke calls in `tools/DataVo.PortablePackageSmoke/Program.cs:259,265`.
- **Risk / Dependencies:** Internal type; only touched by its own unit test and `PortablePackageSmoke`.
- **Effort Estimate:** `XS`

#### `BC-08` — Ambient Static Singleton `DataVoEngine.Current()` & `StorageContext.Instance` Fallback Chain
- **Exact Location(s):**
  - `DataVo.Core/Runtime/DataVoEngine.cs:36-38,267,276,292-333,582-596`
  - `DataVo.Core/StorageEngine/StorageContext.cs:25-26,40,47-67,629,1254,1421,1829,1839`
  - `DataVo.Core/Parser/Actions/BaseDbAction.cs:27-36`, `Parser/QueryEngine.cs:27,42`, `Parser/Evaluator.cs:13`
  - `DataVo.Core/Models/Statement/Utils/TableDetails.cs:82,109,134,163,196`, `Column.cs:35`, `Services/TableService.cs:40`
  - ~35 additional call sites in `Parser/DQL/Select*.cs`, `Parser/DML/Update.cs`, `Parser/Statements/Where.cs`, `Parser/Statements/Mechanism/StatementEvaluator*.cs`, `Parser/Transactions/Commit.cs`
  - `DataVo.Tests/AuditFixes/AuditFixTests.cs:325`, `LsmWriteUnitAtomicityTests.cs:76`, `IndexKeyCultureTests.cs:58`
- **What it is & Why it's there:** Legacy global singleton architecture (`StorageContext.Instance`, `StorageContext.Initialize`, `DataVoEngine._fallbackCurrent`) kept alongside per-instance `DataVoEngine`/`DataVoContext` (`StorageContext.cs:48`: *"For legacy singleton usage during benchmarks, defaults to InMemory if unconfigured"*). Multi-engine isolation requires callers to remember `using DataVoEngine.PushCurrent(engine)`.
- **Proposed Action:** Thread `DataVoEngine` explicitly into `BaseDbAction`, `TableService`, `TableDetail`, `Column`, `StatementEvaluator*`, `Select`, `Update`, `Commit`, and `RowSerializer`; then delete `StorageContext.Instance`, `StorageContext.Initialize`, `DataVoEngine._fallbackCurrent`, `Current()`, and `ResetCurrent()`.
- **Risk / Dependencies:** Medium-high refactor touching ~45 internal call sites and 3 test files; public API cleanup on `StorageContext` and `DataVoEngine`.
- **Effort Estimate:** `L`

#### `BC-09` — Dual SQL Execution Architecture (`Legacy*` Dictionary Pipelines vs. Volcano & Typed Scan/Join)
- **Exact Location(s):**
  - `DataVo.Core/Parser/DQL/Select.Planner.cs:18-26,73-272` (`LegacyWhereJoin`, `LegacyWhereExpression`, `LegacyJoinOnly`, `LegacyNoJoinScan`)
  - `DataVo.Core/Parser/DQL/Select.cs:162-206,484-500,531-544`
  - `DataVo.Core/Parser/Statements/Where.cs`, `Join.cs`, `GroupBy.cs`, `Aggregate.cs`, `JoinStrategies/*`, `Mechanism/StatementEvaluator*.cs`
  - `DataVo.Core/StorageEngine/StorageContext.cs:1575,1727,1776-1814` (`CopyStoredRowToLegacyDictionary`, `ConvertCellForLegacyDictionary`)
  - `DataVo.Core/ValueConversion/DataVoBoundValue.cs:6`, `DataVoValueConverterRegistry.cs:136-146` (`BindForLegacyWrite`)
- **What it is & Why it's there:** Four legacy dictionary-based execution plans coexist with Volcano (`EnableVolcanoExecution = false` default) and typed scan/join fast paths. Even `BuildLegacyNoJoinScan` wraps `ExecutionRow` in `TableScanOperator` only to convert back to `JoinedRow` dictionaries. However, outer/cross joins, `HAVING`, `DISTINCT` aggregates, subqueries, and window functions still require the legacy dictionary pipeline today.
- **Proposed Action:** Implement outer/cross joins, `HAVING`, `DISTINCT` aggregates, subqueries, and window operators in the typed/Volcano pipeline first, then retire the 4 `Legacy*` plans and `StatementEvaluator*`; rename `ConvertCellForLegacyDictionary` to reflect its role as the SQL/object-boxing boundary.
- **Risk / Dependencies:** High — retiring prematurely breaks outer joins, `HAVING`, subqueries, and window functions.
- **Effort Estimate:** `L`

#### [DONE] `BC-10` — Transition Bridge `StoredRowDictionaryView` & `StoredRow.AsDictionary()`
- **Exact Location(s):**
  - `DataVo.Core/StorageEngine/StoredRowDictionaryView.cs:1-81`
  - `DataVo.Core/StorageEngine/StoredRow.cs:89-91`
  - `DataVo.Core/Runtime/Reactive/ReactiveRegistry.cs:547` (sole production caller)
  - `DataVo.Tests/Storage/StoredRowDictionaryViewTests.cs`, `TypedReadParityTests.cs`
- **What it is & Why it's there:** Explicitly documented (`StoredRowDictionaryView.cs:7-15`) as a *"transition bridge, not an endpoint"* for legacy consumers during typed-storage migration. Only 1 production caller remains (`ReactiveRegistry.cs:547`).
- **Proposed Action:** Migrate `ReactiveRegistry.cs:547` to consume `StoredRow` / `StoredRowView` directly, then delete `StoredRowDictionaryView.cs`, `StoredRow.AsDictionary()`, and `StoredRowDictionaryViewTests.cs`.
- **Risk / Dependencies:** Low (`internal` class; `StoredRow.AsDictionary()` is public only on `StoredRow` if exposed).
- **Effort Estimate:** `S`
- **Resolution (Phase 7):** Reactive operators now seed from typed rows: `IReactiveQuery.Seed` takes `IEnumerable<TypedRow>` and `ReactiveRegistry` wraps each `StoredRow`'s cells without copying. `StoredRowDictionaryView.cs`, `StoredRow.AsDictionary()` and `StoredRowDictionaryViewTests.cs` are deleted; `TypedReadParityTests` compares `StoredRow` cells directly.

#### [DONE] `BC-11` — Legacy Disk B+Tree 32-Byte Binary Key Encoding, Dead JSON Attributes & O(n log n) `Delete` in `BTree/`
- **Exact Location(s):**
  - `DataVo.Core/BTree/IndexKeyEncoder.cs:29,39,46-64,204-221,330-410`
  - `DataVo.Core/BTree/BTreeNode.cs:1,13,26-59`
  - `DataVo.Core/BTree/Core/InMemoryBTreeIndex.cs:1-5,21-28,148-185`
  - `DataVo.Core/Indexing/BTree/BTreeIndexFactory.cs:30`
  - `DataVo.Tests/BTree/InMemoryBTreeIndexTests.cs:60,73`
- **What it is & Why it's there:** Remnants of the removed paged-disk binary B+Tree and old JSON-serialized B-Tree:
  - `IndexKeyEncoder.KeySize`, `EmptyKey`, `IsEmptyKey` have **0 callers**; `Encode*` / `CompareKeys` (32-byte fixed binary keys) have **0 production callers**.
  - `NormalizeCell` (`IndexKeyEncoder.cs:204-206`) formats numbers as `"[42]"` solely for *"dictionary-path key compatibility"*, forcing `LsmIndexKeyCodec`, `IndexManager`, and `KeyLockSet` to parse bracketed integers.
  - `BTreeNode` and `InMemoryBTreeIndex` retain `[JsonPropertyName]` attributes, unused `System.Text.Json`/`Exceptions`/`Serialization` usings, and comments about JSON serialization that no longer exists (banned under AOT anyway).
  - `InMemoryBTreeIndex.Delete(string, RowKey)` (`:148-185`) is an O(n log n) full-tree rebuild superseded by O(log n) `RemoveValue` (`:220`), with 0 production callers.
- **Proposed Action:** Delete dead 32-byte binary encoding methods in `IndexKeyEncoder`, strip `[JsonPropertyName]` and stale comments/usings from `BTreeNode`/`InMemoryBTreeIndex`/`BTreeIndexFactory`, delegate or delete `InMemoryBTreeIndex.Delete`, and plan removing the `"[n]"` bracket convention across `IndexKeyEncoder`/`LsmIndexKeyCodec`/`KeyLockSet`.
- **Risk / Dependencies:** Low for dead encoding/JSON attributes; Medium if removing `"[n]"` string key formatting (touches `IndexKeyEncoder`, `LsmIndexKeyCodec`, `IndexManager`, `KeyLockSet`, and BTree tests).
- **Effort Estimate:** `S` (cleanup) / `M` (if unifying numeric key format)
- **Resolution (Phase 2A):** Removed the 32-byte binary encoding, JSON attributes/usings/comments, and `InMemoryBTreeIndex.Delete` (tests now use O(log n) `RemoveValue`). The `"[n]"` bracket key convention is intentionally kept (still shared with `LsmIndexKeyCodec`/`KeyLockSet`).

#### [DONE] `BC-12` — Dead Vector Index `ExportEntries` / `ImportEntries` Pre-Snapshot Persistence Remnants
- **Exact Location(s):**
  - `DataVo.Core/Indexing/Flat/FlatVectorIndex.cs:503-518,538-550`
  - `DataVo.Core/Indexing/HNSW/BrowserFallbackVectorIndex.cs:63-77`
- **What it is & Why it's there:** `internal ExportEntries()` and `ImportEntries(...)` methods left over from the pre-`HnswSnapshotFile` vector persistence mechanism. Zero callers anywhere in the repository.
- **Proposed Action:** Delete both pairs of `internal` methods.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`

#### [DONE] `BC-13` — Legacy Reflection `File.Replace` Workaround in `AtomicFileOperations`
- **Exact Location(s):**
  - `DataVo.Core/Utils/AtomicFileOperations.cs:15-17,79-96`
- **What it is & Why it's there:** Uses `typeof(File).GetMethod(nameof(File.Replace), ...)` and `MethodInfo.Invoke` even though `File.Replace(string, string, string?)` exists in `netstandard2.1` and `.NET 8/9/10` (already called directly in `LsmDurableFileOperations.Portable.cs:94`) and browser runtimes are already short-circuited at line 73.
- **Proposed Action:** Replace reflection with a direct `File.Replace(tempPath, destinationPath, null)` call (keeping `PlatformNotSupportedException` fallback if needed).
- **Risk / Dependencies:** Very low. Called by `WalFileStore`, `TransactionIdStateStore`, and catalog persistence.
- **Effort Estimate:** `XS`

#### [DONE] `BC-14` — Dead/Alias Shims in `LockManager`, `TransactionManager`, `TransactionIdStateStore`, `MvccCoordinator`, `RowKeyConversions`, & `FileHandlePool`
- **Exact Location(s):**
  - `DataVo.Core/Transactions/LockManager.cs:535-540,559-564` (`AcquireRetainedWriteLock`/`ReleaseRetainedWriteLock` 1-line aliases; callers: `DataVoContext.cs:758,788`, `LockManagerRowLevelTests.cs:211-217`)
  - `DataVo.Core/Transactions/TransactionManager.cs:46-50,93-102` (`Begin(Guid, TransactionIdAllocator)` & `Commit(Guid)` — 0 production callers; only 2 test files)
  - `DataVo.Core/Transactions/TransactionIdStateStore.cs:16-19,52-93` (`PersistHighWaterMark` & throttling fields — 0 callers anywhere)
  - `DataVo.Core/Transactions/FileHandlePool.cs:56-92` (`FlushToDisk()` — 0 production callers; only `FileHandlePoolTests.cs:134`)
  - `DataVo.Core/MVCC/MvccCoordinator.cs:11,42-51` (`EnsureRowVersionExists` — 0 callers anywhere)
  - `DataVo.Core/StorageEngine/RowKeyConversions.cs:21-85` (6 of 10 `RowKey`↔`long` conversion overloads have 0 callers anywhere)
- **What it is & Why it's there:** Leftover migration shims and superseded overloads from the `long`→`RowKey`, `CommitClock`, block-reserved TXID, and retained-lock refactors.
- **Proposed Action:** Inline/delete the `LockManager` aliases (update 2 lines in `DataVoContext.cs`), delete `TransactionIdStateStore.PersistHighWaterMark`, `FileHandlePool.FlushToDisk`, `MvccCoordinator.EnsureRowVersionExists`, the 6 unused `RowKeyConversions` overloads, and the 2 unused `TransactionManager` overloads.
- **Risk / Dependencies:** Low (update the 3–4 referencing unit tests).
- **Effort Estimate:** `S`
- **Resolution (Phase 2A):** `TransactionIdStateStore.ForcePersistHighWaterMark` was renamed to `PersistHighWaterMark` once the throttled variant was removed; tests now open transactions through `CommitClock` and commit via `BeginCommit`/`EndCommit`.

#### [DONE] `BC-15` — Pre-AST Aggregate String-Parsing Fallback & T-SQL `GO` No-Op Shim
- **Exact Location(s):**
  - `DataVo.Core/Models/Statement/AggregateModel.cs:55-91`
  - `DataVo.Core/Services/AggregationService.cs:10-19` (`CreateInstance(string, Column)`)
  - `DataVo.Core/Parser/Commands/Go.cs:7-11`, `Parser/Parser.cs:159-160`, `Parser/Evaluator.cs:110-113`
- **What it is & Why it's there:** `AggregateModel` retains a manual `token.IndexOf('(')` string-slicing fallback for `SelectColumnNode.RawExpression` from before `AggregateExpressionNode` existed (sole caller of `AggregationService.CreateInstance(string, Column)`). Separately, `Go` is an `IDbAction` no-op shim that does not inherit `BaseDbAction` (bypassing auth) and does not split batches.
- **Proposed Action:** Remove the raw-string fallback in `AggregateModel.cs:55-91` and `AggregationService.CreateInstance(string, Column)`; decide whether `GO` should be removed or properly split batches in `Evaluator.ToRunnables()`.
- **Risk / Dependencies:** Low. Verify no tests construct `SelectColumnNode` without `Expression`.
- **Effort Estimate:** `XS`
- **Resolution (Phase 2A):** Removed the raw-string aggregate fallback and `AggregationService.CreateInstance(string, Column)`. `GO` is kept as the documented no-op batch separator (`docs/manual/sql/statements/databases-and-security.md`) and is now locked by `GoStatementTests`.

#### [DONE] `BC-16` — Legacy `"Error:"` String-Prefix Error Detection in `BaseDbAction` & `DataVoErrorClassifier`
- **Exact Location(s):**
  - `DataVo.Core/Parser/Actions/BaseDbAction.cs:127,146-163`
  - `DataVo.Core/Contracts/Results/DataVoErrorClassifier.cs:11,46-48,101-106`
- **What it is & Why it's there:** `BaseDbAction.Perform` still checks `HasErrors || Messages.Any(m => m.StartsWith("Error:", ...))` as a legacy error-signaling bridge. Because `DataVoErrorClassifier.Clean` strips the `"Error:"` prefix, this split contract directly causes the `UnionSelect` bug (`LE-03`).
- **Proposed Action:** Convert all remaining `Messages.Add("Error: ...")` call sites to `AddError(...)`, fix `UnionSelect.cs` (`LE-03`), and remove the `"Error:"` string-prefix heuristic from `BaseDbAction` and `DataVoErrorClassifier`.
- **Risk / Dependencies:** Medium — must audit every `BaseDbAction` subclass to ensure none signals failure solely via `Messages.Add("Error:...")`.
- **Effort Estimate:** `S`
- **Resolution (Phase 2A):** `BaseDbAction` records a structured first error (`AddError(Exception)` / `AddError(QueryResult)`); the `"Error:"` scan and classifier prefix are gone; `CreateTable` was the last `Messages.Add("Error: ...")` site.

#### [DONE] `BC-17` — Legacy `StorageMode.Disk` Naming & Stale Comment Remnants in `DataVo.Core`
- **Exact Location(s):**
  - `DataVo.Core/StorageEngine/Config/DataVoConfig.cs:68,303,357` (`DiskStoragePath` property name — 235 references repo-wide)
  - `DataVo.Core/StorageEngine/StorageContext.cs:146,313,321`
  - `DataVo.Core/StorageEngine/IStorageEngine.cs:5`, `Backends/Abstractions/IStorageBackend.cs:8,13`
  - `DataVo.Core/Runtime/CatalogStore.cs:25`, `Runtime/DeferredPublication.cs:13`, `Exceptions/RowDeletedException.cs:5`
- **What it is & Why it's there:** `StorageMode.Disk` and `DiskStorageEngine` were deleted in v0.5.0, but the config property is still named `DiskStoragePath` and 7 core files still reference `Disk` mode in comments.
- **Proposed Action:** Clean up all stale `Disk`-mode comments immediately; optionally rename `DiskStoragePath` → `StoragePath` (or `DataDirectory`) across the repo in a coordinated breaking cleanup since zero BC is required.
- **Risk / Dependencies:** Trivial for comments; Medium-wide mechanical rename across 235 references (core, providers, tests, examples, benchmarks, Unity, docs) if renaming `DiskStoragePath`.
- **Effort Estimate:** `XS` (comments only) / `S` (full `DiskStoragePath` rename)
- **Resolution (Phase 7):** `DataVoConfig.DiskStoragePath` is renamed `DataDirectory` (matching `ResolveDataDirectory()`) across Core, providers, tests, benchmarks, demos, examples, tools, Unity smoke and docs (the prebuilt `docs/public/datavo-wasm` binary is left for the next bundle rebuild). The stale `Disk`-mode comments in `StorageContext`, `IStorageEngine`, `IStorageBackend`, `CatalogStore`, `DeferredPublication`, `RowDeletedException`, `IndexManager`, `InsertInto` and `InsertRowService` now describe the InMemory/LSM engines.

#### [DONE] `BC-18` — Legacy Dictionary Bridges in `DataVoCompiledQuery` & Reactive Operators (`_legacyBuilder`)
- **Exact Location(s):**
  - `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs:34-74,653,1395` (`SelectSingle`/`SelectMany` dictionary overloads)
  - `DataVo.Core/Runtime/Reactive/{Aggregate,Join,TopK,RecursiveCte,VipExposure}ReactiveQuery.cs` (`_legacyBuilder` field + duplicated `Apply` overrides)
  - `DataVo.Core/Runtime/Reactive/RowRef.cs:66` (`ToDictionary()` compatibility method)
  - `DataVo.Generators/DataVoQueryGenerator.cs:215-216`
- **What it is & Why it's there:** Dictionary-materializing compiled query paths remain solely because `DataVoQueryGenerator` falls back to them for shapes without typed getters (see `LE-35`). Every `IBorrowedReactiveQuery` operator also duplicates a `_legacyBuilder` field for `Subscribe(..., Action<QueryChange>)`.
- **Proposed Action:** Migrate remaining `DataVoQueryGenerator` shapes to typed readers and delete dictionary `SelectSingle`/`SelectMany` overloads; centralize `IBorrowedReactiveQuery`→`QueryChange` adaptation inside `ReactiveRegistry` so individual operators don't carry `_legacyBuilder`.
- **Risk / Dependencies:** Touches `DataVo.Generators`, `CompiledQueryRuntimeTests`, and reactive tests.
- **Effort Estimate:** `M`
- **Resolution (Phase 7):** Every SQL-compiled operator implements `IBorrowedReactiveQuery`; `IReactiveQuery` no longer has `Apply`, and `ReactiveRegistry.DispatchCompiled` is the single place a borrowed delta is materialized (`QueryChangeRef.Materialize`) for owned subscribers. All `_legacyBuilder` fields and owned `Apply` overrides are gone. The dictionary `DataVoCompiledQuery.SelectSingle`/`SelectMany` overloads and their `ExecuteSelect` path are deleted; `DataVoQueryGenerator` always emits a typed `CompiledRowMapper` (`SelectSingleTyped`/`SelectManyTyped`), reading a constructor parameter without a typed getter through the new `CompiledRowReader.GetValue(column)` cast to its type, so `QueryMethodModel.DictionaryArguments` is gone. `RowRef.ToOwnedDictionary()` stays as the one owned boundary behind `Materialize` and the LINQ dictionary-model projector. Tests that used the dictionary path as a parity oracle compare the runtime-resolved and tagged typed paths instead; the materialization-reclaim test now asserts typed per-row cost does not grow with unprojected columns (measured 80 B/row narrow and wide, ceiling 128 B/row).

---

### Subsystem: `DataVo.Data`

#### [DONE] `BC-19` — Hardcoded `ServerVersion => "DataVo 1.0"` & Phantom WAL Connection-String XML Docs
- **Exact Location(s):**
  - `DataVo.Data/DataVoConnection.cs:14,58`
- **What it is & Why it's there:** `ServerVersion` hardcodes `"DataVo 1.0"` (package version is `0.5.1`), and XML doc at line 14 claims the connection string configures *"and WAL settings"* (not supported by `DataVoConnectionStringBuilder`).
- **Proposed Action:** Derive `ServerVersion` from `AssemblyInformationalVersionAttribute` (or `0.5.1`) and remove the phantom WAL mention from line 14.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `XS`
- **Resolution (Phase 4A):** `ServerVersion` reads `AssemblyInformationalVersionAttribute` (build metadata stripped, `0.5.1` fallback); the WAL mention is gone.

#### [DONE] `BC-20` — Stale Gitignored Spec Path Comment in `DataVo.Data.csproj`
- **Exact Location(s):**
  - `DataVo.Data/DataVo.Data.csproj:15`
- **What it is & Why it's there:** Comment references `docs/superpowers/specs`, which is gitignored (`.gitignore:432`) and absent on clean clones.
- **Proposed Action:** Point comment to `docs/manual/client-interfaces/native-aot.md` or remove the dead path.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`
- **Resolution (Phase 4A):** The csproj comment points to `docs/manual/client-interfaces/native-aot.md`.

---

### Subsystem: `DataVo.EntityFrameworkCore`

#### [DONE] `BC-21` — No-Op Preview Bootstrap Scaffolding (`DataVoProviderBootstrapMarker`, `BootstrapDiagnosticsEnabled`, `ProviderIdentityPreview`)
- **Exact Location(s):**
  - `DataVo.EntityFrameworkCore/Infrastructure/Internal/DataVoProviderBootstrapMarker.cs:1-5`
  - `DataVo.EntityFrameworkCore/Extensions/DataVoServiceCollectionExtensions.cs:24`
  - `DataVo.EntityFrameworkCore/Infrastructure/DataVoDbContextOptionsBuilder.cs:67-81`
  - `DataVo.EntityFrameworkCore/Infrastructure/Internal/DataVoOptionsExtension.cs:26,28,93-103,136`
  - `DataVo.Tests/EntityFramework/DataVoProviderBootstrapTests.cs:30,34,188`
- **What it is & Why it's there:** Empty marker class registered in DI and never resolved anywhere in production; `BootstrapDiagnosticsEnabled` only prints to `LogFragment`/`DebugInfo`; `EnableProviderIdentityPreview()` does not set `IsDatabaseProvider = true` or register `IDatabaseProvider` — it only gates `EnableNativeQueryTranslationPreview()`.
- **Proposed Action:** Collapse or delete the no-op preview flags/marker unless implementing a real `IDatabaseProvider`, and update `DataVoProviderBootstrapTests.cs`.
- **Risk / Dependencies:** Low (`DataVoProviderBootstrapTests.cs` asserts the marker by type-name reflection).
- **Effort Estimate:** `S`
- **Resolution (Phase 4B):** `DataVoProviderBootstrapMarker` and its DI registration are deleted; `EnableBootstrapDiagnostics` and `EnableProviderIdentityPreview` (with the `ProviderIdentityPreview` mode and the `ProviderIdentityPreviewEnabled` fields of the status records) are removed, leaving two modes: `BridgeOnly` and `NativeTranslationPreview`. `DataVoProviderBootstrapTests` assert the creator and mode resolver registrations instead of the marker. *(Breaking change to the preview API surface.)*

#### [DONE] `BC-22` — Stale `dynamic` Value Comment in `DataVoEntityMaterializer`
- **Exact Location(s):**
  - `DataVo.EntityFrameworkCore/Infrastructure/Internal/DataVoEntityMaterializer.cs:233`
- **What it is & Why it's there:** XML doc still refers to *"a DataVo dynamic value"* from before `DataVo.Core` removed `dynamic`.
- **Proposed Action:** Update comment to `object`.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`
- **Resolution (Phase 4B):** The comment now describes an `object` value and lists the `DATETIME` and `VECTOR` mappings.

---

### Subsystem: `DataVo.VectorData`

#### [DONE] `BC-23` — Fragile Non-Public Reflection Shim on `VectorStoreKeyAttribute`
- **Exact Location(s):**
  - `DataVo.VectorData/DataVoCollectionModel.cs:224-227`
- **What it is & Why it's there:** `ExplicitAutoGenerated` reflects private internal property `IsAutoGeneratedNullable` on `Microsoft.Extensions.VectorData.Abstractions` `VectorStoreKeyAttribute` (`BindingFlags.Instance | BindingFlags.NonPublic`). Under Native AOT trimming or minor upstream NuGet updates, that private property can vanish, silently making `[VectorStoreKey(IsAutoGenerated = false)]` auto-generate `Guid` keys.
- **Proposed Action:** Verify behavior under `DataVo.AotSmoke` (`LE-33`), add `[DynamicDependency]` if keeping the workaround, or replace with public API as soon as `Microsoft.Extensions.VectorData.Abstractions` exposes it.
- **Risk / Dependencies:** Medium (silent AOT behavioral regression on `Guid` keys).
- **Effort Estimate:** `XS`
- **Resolution (Phase 4A):** `[DynamicDependency]` keeps `IsAutoGeneratedNullable` under trimming, with a fallback to the attribute's recorded `IsAutoGenerated` named argument (public API); a canary test and the AOT smoke verify `IsAutoGenerated = false` under Native AOT.

---

### Subsystem: `DataVo.Generators`

#### [DONE] `BC-24` — Legacy Hardcoded Game/Telemetry Column Heuristic `InferCastType` in `DataVoQueryGenerator`
- **Exact Location(s):**
  - `DataVo.Generators/DataVoQueryGenerator.cs:246,349-356`
  - `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs:294-317`
- **What it is & Why it's there:** When a `[DataVoQuery]` DTO constructor's parameter names don't match selected column names, `InferCastType` casts columns to `int` ONLY if named `*Id`, `Level`, or `Frame` (leftover from an early game/telemetry demo), and `string` for everything else — emitting invalid casts for `long`/`double`/`bool`/`DateTime`/`Score`/etc.
- **Proposed Action:** Use positional constructor parameter types (`constructor.Parameters[i].Type`) instead of guessing from column names, or emit a diagnostic on signature mismatch.
- **Risk / Dependencies:** Low-Medium; update `DataVoQueryGeneratorTests.cs:294-317`.
- **Effort Estimate:** `S`
- **Resolution (Phase 4A):** `InferCastType` is removed: constructor arguments map by name, else by position using the parameter types; an arity/ambiguity mismatch reports `DATAVOQ004`.

#### [DONE] `BC-25` — Hardcoded `[GeneratedCode(..., "1.0")]` & Duplicated NuGet Metadata in `DataVo.Generators`
- **Exact Location(s):**
  - `DataVo.Generators/Tables/DataVoTableEmitter.cs:31`
  - `DataVo.Generators/Linq/DataVoLinqEmitter.cs:41`
  - `DataVo.Generators/DataVo.Generators.csproj:12-27`
- **What it is & Why it's there:** Generated code emits hardcoded version `"1.0"` while package version is `0.5.1`; `.csproj` duplicates `<Version>`, `<Authors>`, `<Company>`, `<PackageLicenseExpression>`, `<RepositoryUrl>` instead of sharing via `Directory.Build.props`, and lacks `<PackageReleaseNotes>`.
- **Proposed Action:** Centralize shared NuGet metadata & `<Version>` in `Directory.Build.props` and align `[GeneratedCode]` version.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `XS`
- **Resolution (Phase 4A):** Shared NuGet metadata and `<Version>` live once in `Directory.Build.props` (Generators gains release notes); `[GeneratedCode]` uses the generator's package version.

---

### Subsystem: `DataVo.Browser`

#### [DONE] `BC-26` — Legacy `Newtonsoft.Json`, `Microsoft.CSharp` (`dynamic?`), Dormant `XmlSerializer` Trimmer Roots & Stale `0.1.0-preview.1` Version
- **Exact Location(s):**
  - `DataVo.Browser/DataVo.Browser.csproj:7,13,21-40`
  - `DataVo.Browser/Program.cs:4,15,284`
- **What it is & Why it's there:** `DataVo.Browser.csproj` retains `Newtonsoft.Json` 13.0.4 and 11 `<TrimmerRootAssembly>` items (including `System.Xml.XmlSerializer` and `Microsoft.CSharp`, left over from before `DataVo.Core` dropped `XmlSerializer`/`Newtonsoft.Json`/DLR), while `<PublishTrimmed>false</PublishTrimmed>` (`:13`) renders all trimmer roots dormant and ships ~5.7 MB of XML/Newtonsoft/VB/DLR `.wasm` files. `Program.cs:284` still uses `out dynamic? rawValue`. Package `<Version>` is stuck at `0.1.0-preview.1`.
- **Proposed Action:** Replace `dynamic?` with `object?` at `Program.cs:284`, migrate `Program.cs` serialization to `System.Text.Json` source-gen, remove `Newtonsoft.Json` and obsolete `XmlSerializer`/`Microsoft.CSharp` trimmer roots, align version, and evaluate enabling trimming.
- **Risk / Dependencies:** Medium — requires `wasm-tools` workload and Playwright browser suite verification.
- **Effort Estimate:** `M`
- **Resolution (Phase 5):** `Program.cs` uses `System.Text.Json` through the source-generated `BrowserJsonContext` (`BrowserJson.cs`); result cells (`object?`) are written by `BrowserCellJsonConverter` by runtime type, so the `dynamic?` and `JObject` paths are gone. `Newtonsoft.Json` and the `Microsoft.CSharp`/`System.Xml.XmlSerializer`/framework trimmer roots are removed. Trimming is now enabled (`PublishTrimmed=true`, roots: `DataVo.Core` and `DataVo.Browser` only): the docs bundle went from 16.6 MB to 7.7 MB with no trim warnings. `<Version>` comes from the new shared `$(DataVoVersion)` (0.5.1) in `Directory.Build.props`.

#### [DONE] `BC-27` — `localStorage` & Non-Persistent `worker-memory-fallback` Compatibility Shims in `datavo.interop.js`
- **Exact Location(s):**
  - `DataVo.Browser/datavo.interop.js:1-7,69-82,122-313,452-490`
- **What it is & Why it's there:** OPFS worker transport requires `Atomics.wait` + `SharedArrayBuffer`, which always fails on the browser main thread or without COOP/COEP headers — forcing fallback to synchronous `localStorage` (~5 MB cap) or silent non-persistent `worker-memory-fallback` (loses all data on reload).
- **Proposed Action:** Keep `localStorage` only if main-thread/non-COOP hosting is still desired; surface an explicit warning/flag when `worker-memory-fallback` activates; consider direct `FileSystemSyncAccessHandle` inside a single Web Worker (eliminating `SharedArrayBuffer` requirement).
- **Risk / Dependencies:** Medium (affects docs playground hosting on GitHub Pages, which lacks COOP/COEP headers by default).
- **Effort Estimate:** `M`
- **Resolution (Phase 5):** `localStorage` stays as the main-thread/non-isolated fallback. `worker-memory-fallback` now logs a warning, sets `globalThis.__datavoStorageNonPersistent` and reports `persistent: false` plus a `warning` in the capabilities (every backend reports `persistent`). **Open follow-up:** the OPFS worker transport still never engages. `DataVoRuntimeWorker.ts` calls `withApplicationArgumentsFromQuery()` (throws without `window`), and the nested storage worker never starts while its parent blocks in `Atomics.wait`. Making OPFS work needs the single-worker `FileSystemSyncAccessHandle` redesign.

---

### Subsystem: `integrations/unity`

#### [DONE] `BC-28` — Stale `StorageMode.Disk` Claim in `integrations/unity/com.datavo.core/README.md`
- **Exact Location(s):**
  - `integrations/unity/com.datavo.core/README.md:15`
  - `scripts/check-unity-support-status.sh:88-89`
- **What it is & Why it's there:** UPM package README status table still lists ``| `StorageMode.Disk`, `StorageMode.Lsm` | Save, close and reopen pass...`` even though `StorageMode.Disk` was removed in v0.5.0. `check-unity-support-status.sh` only rejects `StorageMode.Disk` in `docs/features/unity-and-godot.md`, missing this UPM README.
- **Proposed Action:** Remove `StorageMode.Disk` from `integrations/unity/com.datavo.core/README.md:15` and add this file to `scripts/check-unity-support-status.sh`.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `XS`
- **Resolution (Phase 5):** `StorageMode.Disk` removed from the UPM README status table; `check-unity-support-status.sh` rejects `StorageMode.Disk` in that README.

#### [DONE] `BC-29` — Stale `0.4.0-rc.1` Version in Unity `CHANGELOG.md` & `package-unity-upm.sh`
- **Exact Location(s):**
  - `integrations/unity/com.datavo.core/CHANGELOG.md:3,31`
  - `scripts/package-unity-upm.sh:8`
- **What it is & Why it's there:** `CHANGELOG.md` stops at `0.4.0-rc.1` (missing v0.5.0 breaking `Disk` removal and v0.5.1) and `scripts/package-unity-upm.sh:8` hardcodes default `version="0.4.0-rc.1"`.
- **Proposed Action:** Add `0.5.x` changelog entry and read the default version dynamically from `Directory.Build.props` (or require `--version`).
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `XS`
- **Resolution (Phase 5):** UPM `CHANGELOG.md` has `0.5.0` (Disk removal) and `0.5.1` entries; `package-unity-upm.sh` defaults to `<DataVoVersion>` from `Directory.Build.props` (verified: builds `com.datavo.core-0.5.1.tgz`).

---

### Subsystem: `docs/`

#### [DONE] `BC-30` — Entire Superseded `docs/features/` Directory (22 of 23 Files, ~3,617 Lines of v0.2 Docs)
- **Exact Location(s):**
  - `docs/features/*.md` (22 legacy files; **exception:** `docs/features/insert-intent-row-locking.md` is an active v0.5.1 engineering note!)
  - `docs/.vitepress/config.mts:134`, `docs/scripts/generate-og-images.cjs:24`, `docs/mcp/datavo-docs-server.cjs:18`
- **What it is & Why it's there:** Pre-reorg March 2026 (`v0.2.0-rc.3`) docs superseded by `docs/manual/` and already excluded from VitePress and MCP. Contain obsolete `v0.2.0-rc.3` prerelease instructions, `"Active v0.3 Roadmap"` checklists, `Disk` storage mode disclaimers, and broken links to deleted dirs (`docs/features/index.md:164,188-189` → `../DataVo.Core/index.md`, `../architecture/index-manager.md`).
- **Proposed Action:** Relocate `docs/features/insert-intent-row-locking.md` to `docs/manual/internals/` (or an ADR directory), delete the 22 obsolete files under `docs/features/`, and remove `"features/**"` exclusion entries (also update `scripts/check-unity-support-status.sh` if it checks `docs/features/unity-and-godot.md`).
- **Risk / Dependencies:** Low (already excluded from VitePress & MCP; check `scripts/check-doc-api-samples.sh` & `scripts/check-unity-support-status.sh` references).
- **Effort Estimate:** `S`
- **Resolution (Phase 6):** `insert-intent-row-locking.md` moved to `docs/manual/internals/` (frontmatter, sidebar, `public-manual.cjs`); the other 22 `docs/features/` files deleted; `features/**`/`superpowers/**` dropped from `config.mts` `srcExclude` and `generate-og-images.cjs`; `check-unity-support-status.sh` now checks `roadmap.md`, `installation.md` (TFM row) and `README.md` instead.

#### [DONE] `BC-31` — `CLAUDE.md` & `README.md` Claim Removed `StorageMode.Disk` / `DiskStorageEngine` Still Exists as "Deprecated/Legacy"
- **Exact Location(s):**
  - `CLAUDE.md:8`
  - `README.md:169`
- **What it is & Why it's there:** `CLAUDE.md:8` instructs agents to keep `StorageMode.Disk` / `DiskStorageEngine` *"compiling and unbroken"* as deprecated/legacy; `README.md:169` says ``(`Disk`, an earlier paged engine, is legacy.)``. Both contradict v0.5.0, which completely deleted `StorageMode.Disk` and `DiskStorageEngine`.
- **Proposed Action:** Update `CLAUDE.md:8` (and line 12 to include `DataVo.VectorData` in AOT-locked projects) and `README.md:169` to reflect that `Disk` was removed in v0.5.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`
- **Resolution (Phase 6):** `CLAUDE.md` states `Disk`/`DiskStorageEngine` were removed in v0.5.0 and lists `DataVo.VectorData` among the AOT-locked projects; `README.md` says the paged engine was removed in v0.5.

#### [DONE] `BC-32` — `README.md` Self-Contradiction on Multi-Target Packages ("Planned for v0.5" vs. Already Shipped)
- **Exact Location(s):**
  - `README.md:8,81,176,178`
- **What it is & Why it's there:** Line 8 says `.NET 10`; line 81 says current release is `0.5.1`; line 176 claims *"The published DataVo.Core package currently ships a net10.0 asset; multi-target packages (net8.0, net9.0, net10.0, netstandard2.1) are planned for v0.5"*, directly contradicting line 178 (*"the package ships a netstandard2.1 asset (alongside net8.0, net9.0 and net10.0)"*, which also has lowercase typo `runtime. the package`).
- **Proposed Action:** Reconcile `README.md:8,176,178` with actual shipped v0.5.1 TFMs per package and fix the capitalization typo.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`
- **Resolution (Phase 6):** `README.md` tagline says .NET 8, 9 and 10; the Platforms section gives the real per-package TFMs (Core/Data incl. `netstandard2.1`, VectorData `net8`–`net10`, EF `net10.0`, Generators `netstandard2.0`); the lowercase typo is gone.

#### [DONE] `BC-33` — Legacy VitePress URL Redirects & Duplicated Trailing-Punctuation Stripping Hacks
- **Exact Location(s):**
  - `docs/.vitepress/legacy-redirects.ts:6-37` (30 pre-reorg routes + 2 typo routes `/manual/getting-started/roadmap)` and `roadmap))` at `:11-12`)
  - `docs/.vitepress/config.mts:6,57-60,129-131`
  - `docs/.vitepress/theme/index.ts:15,19-54,87-106`
- **What it is & Why it's there:** Static HTML redirect generator plus duplicated client-side and `<head>` scripts to preserve pre-reorg `/manual/preface/*`, `/manual/tutorial/*`, `/manual/sql-language/*` URLs and strip trailing `)`/`:` punctuation.
- **Proposed Action:** Delete `legacy-redirects.ts`, its `buildEnd` hook in `config.mts`, and the redirect/punctuation interceptors in `theme/index.ts` (or keep only if external backlink analytics still show significant traffic).
- **Risk / Dependencies:** Medium if external links on Reddit/HN/blogs still point to pre-reorg URLs (will 404 on GitHub Pages); zero code risk.
- **Effort Estimate:** `XS`
- **Resolution (Phase 6):** The client-side redirect interceptor in `theme/index.ts` and the trailing-punctuation `<head>` script in `config.mts` are deleted. `legacy-redirects.ts` + its `buildEnd` hook are kept as the single mechanism (static redirect pages, so external pre-reorg links still land on a page; verified in Playwright: `/manual/preface/roadmap` → `/manual/getting-started/roadmap`).

#### [DONE] `BC-34` — Stale `v0.4` / `v0.4.0-rc.1` Version Strings Across AI Generator, Generated Exports & 15+ Manual Pages
- **Exact Location(s):**
  - `docs/scripts/generate-ai-docs.cjs:121,150,174` (propagated into tracked `docs/public/llms.txt:3`, `docs/public/mcp/datavo-docs.json:3`, `docs/public/mcp/remote-docs.json:3`)
  - `docs/manual/getting-started/introduction.md:3,78`, `docs/manual/reference/limits.md:3,8,12,44`, `docs/manual/reference/ai-access.md:3,8,67`
  - `docs/manual/sql/compatibility.md:3,25,27`, `data-types.md:74`, `functions.md:3,8,82,112-113,118,243,246,248-249`
  - `docs/manual/sql/statements/{select,insert,update,delete,create-table,alter-and-drop,indexes,transactions,databases-and-security}.md`
- **What it is & Why it's there:** Hardcoded `v0.4` / `v0.4.0-rc.1` strings left behind after releasing `0.5.0`/`0.5.1`.
- **Proposed Action:** Bump `generate-ai-docs.cjs` and manual pages to `v0.5` (or remove version tags from evergreen SQL docs) and regenerate `npm run docs:ai`.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `S`
- **Resolution (Phase 6):** `generate-ai-docs.cjs` reads the release label from `<DataVoVersion>` in `Directory.Build.props` (exports now say `v0.5.1`); the SQL statement/function/type pages, `limits.md`, `ai-access.md` and `introduction.md` say `v0.5`/`0.5.1`. Each "not supported" claim was re-checked against `Parser.cs` first. Historical benchmark rows keep their v0.4 run labels. `npm run docs:ai` regenerated.

#### [DONE] `BC-35` — Stale `--prerelease` Install Flags & `.NET 10`-Only Phrasing Across v0.5 Docs
- **Exact Location(s):**
  - `--prerelease`: `docs/manual/getting-started/quickstart.md:43`, `docs/manual/client-interfaces/dapper.md:32`, `docs/manual/client-interfaces/vector-data.md:13`
  - `.NET 10`-only phrasing: `docs/index.md:34`, `docs/scripts/generate-og-images.cjs:20`, `docs/manual/getting-started/{introduction.md:3,8,78, quickstart.md:8, choosing-an-interface.md:3,31-32}`, `docs/manual/client-interfaces/{native-aot.md:3,8, source-generators.md:89}`, `docs/manual/sql/data-types.md:46`, `docs/blog/part-2-packed-buffers-aot-and-roslyn-codegen.md:424`
- **What it is & Why it's there:** NuGet packages are now stable `0.5.1` (no `--prerelease`), and `DataVo.Core`, `DataVo.Data`, and `DataVo.VectorData` multi-target `.NET 8+` (`DateOnly` adapters use `#if NET8_0_OR_GREATER`, not `.NET 10`-only).
- **Proposed Action:** Remove `--prerelease` from the 3 install snippets and update `.NET 10`-only copy to `.NET 8, 9, and 10` (keeping `.NET 10` only for EF Core, Browser, and benchmark environments).
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`
- **Resolution (Phase 6):** `--prerelease` removed from `quickstart.md`, `dapper.md`, `vector-data.md`; `.NET 10`-only copy now says .NET 8, 9 and 10 (`index.md`, `introduction.md`, `quickstart.md`, `choosing-an-interface.md`, `native-aot.md`, `data-types.md` and `source-generators.md` `DateOnly` adapters on `net8.0`+, OG default description, `blog/index.md`); Part 2 has an update note on the multi-target AOT fence.

#### [DONE] `BC-36` — Unused Vue Components (`BenchmarkChart.vue`, `SqlEditor.vue`), 17 MB of Stale March 2026 WASM Binaries in Git, & Dead Playwright Spec Stub
- **Exact Location(s):**
  - `docs/src/components/BenchmarkChart.vue` (0 usages; registered in `docs/.vitepress/theme/index.ts:8,81`, stripped in `docs/scripts/generate-ai-docs.cjs:21-23,50`)
  - `docs/src/components/SqlEditor.vue` (0 usages across all `.md` files; registered in `docs/.vitepress/theme/index.ts:6,80` — contradicts `README.md:179` & `roadmap.md:31` claiming a live docs playground!)
  - `docs/public/datavo-wasm/` (**17 MB**, 186+ compiled `.wasm`/`.js`/`.json` files tracked in git, dated **2026-03-30**, pre-v0.3/v0.4/v0.5!)
  - `docs/tests/browser/datavo-wasm-needs-specific.spec.ts:1-5` (`test.skip(true, "Replaced by sharded needs-specific specs.")`)
- **What it is & Why it's there:** Leftover playground/charting assets from March 2026 that are no longer mounted on any docs page.
- **Proposed Action:** Delete `BenchmarkChart.vue` (and its theme/AI-script hooks) and `datavo-wasm-needs-specific.spec.ts`. Decide whether to (a) wire `SqlEditor.vue` onto a real playground page + stop committing compiled `datavo-wasm/` binaries to git (build in `deploy-docs.yml` instead), or (b) remove `SqlEditor.vue` and `docs/public/datavo-wasm/` and update `README.md:179`/`roadmap.md:31`.
- **Risk / Dependencies:** Medium if local Playwright browser tests expect prebuilt `docs/public/datavo-wasm/` without running `scripts/deploy-browser-wasm.sh`.
- **Effort Estimate:** `S`
- **Resolution (Phase 6):** Option (a): `SqlEditor.vue` is mounted on a new `/manual/getting-started/playground` page (nav + sidebar), so the README/roadmap claim is true. `docs/public/datavo-wasm/` stays committed because the Playwright browser suites use it, and it was rebuilt from current source (`DataDirectory` present, `DiskStoragePath` gone). `BenchmarkChart.vue` (+ theme registration and AI-script hook) and `datavo-wasm-needs-specific.spec.ts` are deleted. Playground verified in headless Chromium (script runs, rows returned).

#### [DONE] `BC-37` — Obsolete Pre-Reorg Paths & Nonexistent Directories in MCP Server & VitePress Config
- **Exact Location(s):**
  - `docs/mcp/datavo-docs-server.cjs:14-24,119` (`deniedPathFragments` lists nonexistent `"architecture/"`, `"audit/"`, `"datavo.core/"`, `"superpowers/"`; `get_doc` schema gives broken pre-reorg examples `/quickstart`, `quickstart.md`, `datavo-docs://page/quickstart`)
  - `docs/manual/reference/ai-access.md:37` (cites deleted `architecture/index-manager.md`)
  - `docs/.vitepress/config.mts:135` & `docs/scripts/generate-og-images.cjs:32` (exclude gitignored `"superpowers/**"`)
- **What it is & Why it's there:** Pre-reorg directory names left behind after consolidating docs into `docs/manual/`.
- **Proposed Action:** Update `datavo-docs-server.cjs:119` to match `worker.js:101` (`/manual/getting-started/quickstart`), prune nonexistent directories from `deniedPathFragments`/`config.mts`/`generate-og-images.cjs`, and update `ai-access.md:37`.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`
- **Resolution (Phase 6):** MCP `deniedPathFragments` pruned to existing dirs (`research/`, `.vitepress/`, `node_modules/`, `public/`, `src/`); `get_doc` schema examples use `/manual/getting-started/quickstart`; `ai-access.md` denial example uses `research/feature-bibliography.md`; `superpowers/**` removed from `config.mts` and `generate-og-images.cjs`.

---

### Subsystem: `CI/scripts` *(incl. Tests, Tools, Demos, Examples)*

#### [DONE] `BC-38` — Broken `tools/DataVo.PortablePackageSmoke` Importing Deleted `DataVo.Core.StorageEngine.Disk` Namespace
- **Exact Location(s):**
  - `tools/DataVo.PortablePackageSmoke/Program.cs:4,15,107-108,521-525`
  - `tools/DataVo.PortablePackageSmoke/DataVo.PortablePackageSmoke.csproj:16` (`System.Text.Json` `10.0.0` vs `DataVo.Core` `netstandard2.1` `8.0.6`)
- **What it is & Why it's there:** Line 15 still has `using DataVo.Core.StorageEngine.Disk;` (deleted in v0.5.0), **causing a compile error**. Lines 4 and 521-525 retain 5 unused `SafeFileHandle` delegates (`PortableGetLength`, `PortableWrite`, `PortableRead`, `PortableSetLength`, `PortableFlushToDisk`) from paged-disk tests. Also `:107-108` prints `"passed"` *before* calling `VerifyPreparedStatementsAndNameCase()`. Never caught because CI does not build `tools/`.
- **Proposed Action:** Delete `using DataVo.Core.StorageEngine.Disk;`, remove unused `SafeFileHandle` using/delegates, move `"passed"` log after `VerifyPreparedStatementsAndNameCase()`, and wire into CI (`LE-53`).
- **Risk / Dependencies:** High urgency (currently broken build); low risk to fix.
- **Effort Estimate:** `XS`

#### [DONE] `BC-39` — Broken Pre-0.5 Two-TFM Assertions in `prepare-unity-candidate.sh` & `verify-portable-candidate.sh`
- **Exact Location(s):**
  - `scripts/prepare-unity-candidate.sh:88-93`
  - `scripts/verify-portable-candidate.sh:7,48-53`
- **What it is & Why it's there:** Both scripts hardcode `expected_dlls=$'lib/net10.0/DataVo.Core.dll\nlib/netstandard2.1/DataVo.Core.dll'`. In v0.5.0+, `DataVo.Core.nupkg` ships 4 TFMs (`net8.0`, `net9.0`, `net10.0`, `netstandard2.1`), causing `prepare-unity-candidate.sh` — and transitively `package-unity-upm.sh`, `prepare-unity-smoke-candidate.sh`, `prepare-unity-arena-candidate.sh`, all `run-unity-*.sh`, and `verify-unity-upm-package.sh` — to fail immediately. `verify-portable-candidate.sh` is also unreferenced and still uses `0.0.0-stage2a.$RUN_ID`.
- **Proposed Action:** Delegate `prepare-unity-candidate.sh:88-93` to `scripts/assert-core-package-targets.sh "$core_package"`; either delete `verify-portable-candidate.sh` or update it similarly and wire into CI.
- **Risk / Dependencies:** High impact (unblocks every Unity script); low risk.
- **Effort Estimate:** `XS`

#### [DONE] `BC-40` — Orphaned & Broken Stage 2B Script `scripts/verify-stage2b-platform-seams.sh`
- **Exact Location(s):**
  - `scripts/verify-stage2b-platform-seams.sh:1-164` (`:68,73,136-161`)
- **What it is & Why it's there:** Unreferenced milestone script from before v0.5 multi-targeting. Asserts `#if NET10_0_OR_GREATER` (now `NET8_0_OR_GREATER`), checks moved path `DataVo.Core/Compat/NetStandard21Polyfills.cs`, and explicitly fails if `DataVo.Core.nupkg` contains `lib/netstandard2.1/DataVo.Core.dll` (*"quarantined netstandard2.1 asset"*).
- **Proposed Action:** Delete `scripts/verify-stage2b-platform-seams.sh`.
- **Risk / Dependencies:** None (0 references).
- **Effort Estimate:** `XS`

#### [DONE] `BC-41` — Broken `test-relational-hardening.sh` (Deleted `DiskIndexConcurrencyTests`) & Legacy Milestone Closeout Script Cluster
- **Exact Location(s):**
  - `scripts/test-relational-hardening.sh:8-9`
  - `scripts/phase-closeout.sh`, `scripts/final-mile-validate.sh`, `scripts/test-hnsw-fast.sh`, `scripts/test-hnsw-perf.sh`, `scripts/test-browser-strict-stress.sh:12-27` (misnumbered `[1/4]..[3/4],[4/5],[5/5]`)
- **What it is & Why it's there:** `test-relational-hardening.sh:9` filters for deleted `DiskIndexConcurrencyTests` (renamed to `LsmIndexConcurrencyTests`), executing **0 tests**. The surrounding `phase-closeout.sh`/`final-mile-validate.sh` cluster is only referenced by each other and historical `docs/features/sql-engine-vector-wasm-phase-status.md`, never by CI.
- **Proposed Action:** Fix filter to `LsmIndexConcurrencyTests` (if kept) or retire the obsolete `phase-closeout.sh` / `final-mile-validate.sh` / `test-relational-hardening.sh` cluster; fix step numbering in `test-browser-strict-stress.sh` if retained.
- **Risk / Dependencies:** Low (not called by CI).
- **Effort Estimate:** `XS`
- **Resolution (Phase 2A):** Retired `test-relational-hardening.sh`, `phase-closeout.sh`, `final-mile-validate.sh`, `test-hnsw-fast.sh`, `test-hnsw-perf.sh`; kept `test-browser-strict-stress.sh` with step numbering fixed to `[1/5]..[5/5]`.

#### [DONE] `BC-42` — Dead Manual WASM Scenarios for Deleted Legacy B-Tree Tests
- **Exact Location(s):**
  - `scripts/wasm-scenarios/manual-scenarios.cjs:98-242`
- **What it is & Why it's there:** Contains 9 cases for deleted `JsonBTreeIndexTests.*` (`:98-228`) and 1 case for deleted `BinaryBPlusTreeIndexTests.InsertAndSearch_RoundTrip_ReturnsInsertedRowId` (`:230-241`). Neither test class exists in `DataVo.Tests/`.
- **Proposed Action:** Delete lines 98–242 from `scripts/wasm-scenarios/manual-scenarios.cjs`.
- **Risk / Dependencies:** None (unreachable switch cases).
- **Effort Estimate:** `XS`

#### [DONE] `BC-43` — Dead v1 SSTable Compaction Branch & Dead `TestSsTableWriter` Class in `DataVo.Tests`
- **Exact Location(s):**
  - `DataVo.Tests/Lsm/ReferenceCompaction.cs:46-55`
  - `DataVo.Tests/Lsm/TestSsTable.cs:66-88` (`TestSsTableWriter`)
  - `DataVo.Tests/Lsm/LsmStreamingCompactionTests.cs:16`
- **What it is & Why it's there:** `LsmSstableFormat` only has `RowKeyed = 2` and `Keyed = 3`, making `ReferenceCompaction.cs:46-55`'s `else` branch (v1 SSTable writer) unreachable — and `ReferenceCompaction.cs:48` is the sole caller of `TestSsTableWriter`. `LsmStreamingCompactionTests.cs:16` still mentions *"legacy v1 row files"*.
- **Proposed Action:** Replace `else` in `ReferenceCompaction.cs:46-55` with `throw new ArgumentOutOfRangeException(nameof(format))`, delete dead `TestSsTableWriter`, and update `LsmStreamingCompactionTests.cs:16`.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`

#### [DONE] `BC-44` — Obsolete Stage 2B Probe Test & Stale `Disk`-Mode Naming/Comments Across `DataVo.Tests/`
- **Exact Location(s):**
  - `DataVo.Tests/Performance/Stage2BHotPathProbeTests.cs:1-296` (permanently skipped via unreferenced `DATAVO_RUN_STAGE2B_PROBE=1`, probes `ScalarKeyFormatter` via reflection even though `ScalarKeyFormattingAllocationTests.cs` tests it directly)
  - `DataVo.Tests/E2E/DiskIntegerPrimaryKeyPersistenceTests.cs` (filename says `Disk`, class at `:15` is `LsmIntegerPrimaryKeyPersistenceTests`)
  - `DataVo.Tests/E2E/SqlBenchmarkTests.cs:25,55,59,88-89,98,145` (uses `StorageMode.Lsm`, named `*_InMemory_vs_Disk`, logs `[Disk]`)
  - `DataVo.Tests/E2E/SqlExecutionTestsBase.cs:11`, `LsmIndexConcurrencyTests.cs:10`, `BigIntAndDoubleColumnTests.cs:319`
  - `DataVo.Tests/EntityFramework/DataVoProviderBootstrapTests.cs:61,65-66` (`UseDataVo_DiskTypedOptions_*` calling `UseLsmStorage()`)
  - `DataVo.Tests/Performance/LinqProjectionMemoryTests.cs:69`, `DataVo.Tests/Streaming/RowRefStreamTests.cs:168`, `DataVo.Tests/BTree/IndexManagerTests.cs:23-24` (stale singleton comment)
- **What it is & Why it's there:** One-off portability milestone probe plus leftover `Disk` file/method/comment names after v0.5.0 removal. *(Keep `DataVo.Tests/ADO/AdoNetTests.cs:315` `[InlineData("Disk")]`, which intentionally verifies `StorageMode=Disk` is rejected.)*
- **Proposed Action:** Delete `Stage2BHotPathProbeTests.cs`, rename `DiskIntegerPrimaryKeyPersistenceTests.cs` → `LsmIntegerPrimaryKeyPersistenceTests.cs`, and clean up stale `Disk` test method names/comments.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `S`

#### [DONE] `BC-45` — Abandoned Pure-SQLite `examples/semantic-inventory/` Project in `DataVo.sln`
- **Exact Location(s):**
  - `examples/semantic-inventory/` (entire subtree, including `backend/SemanticInventory.Backend.csproj:9-13`, `backend/Program.cs:12-14,40`, `backend/Migrations/*`, `backend/semantic-inventory.db` (**22.6 MB**), `seed/seed.sql`)
  - `DataVo.sln:18-23,120-131,269-271`
  - `scripts/start-semantic-inventory.sh`, `scripts/stress-semantic-inventory.sh`
- **What it is & Why it's there:** References **zero DataVo projects or packages** — it is a pure EF Core + SQLite demo (`Microsoft.EntityFrameworkCore.Sqlite` 10.0.0) superseded by `examples/MicrosoftAiLocalRag` and `examples/HybridVectorRagAot`. Not built in CI, bloats the repository by >22.6 MB, and misleads contributors opening `DataVo.sln`.
- **Proposed Action:** Remove `SemanticInventory.Backend` from `DataVo.sln`, delete `examples/semantic-inventory/` (and `scripts/{start,stress}-semantic-inventory.sh`), or explicitly archive/port it to `DataVo.VectorData`.
- **Risk / Dependencies:** Low (not built by CI or linked from `README.md` or `docs/manual/`).
- **Effort Estimate:** `XS` (delete) / `L` (if porting)

#### [DONE] `BC-46` — Legacy `"disk"` CLI Alias & Stale Disk/WinForms References in `demos/`
- **Exact Location(s):**
  - `demos/realtime-benchmarks/src/DataVo.Benchmarks.Common/DataVoBenchmarkContext.cs:11` (accepts `"disk"` and silently maps to `StorageMode.Lsm`)
  - `demos/realtime-benchmarks/README.md:78-82` (documents `--storage disk`)
  - `demos/realtime-benchmarks/browser-wasm-showcase/README.md:21` (references deleted `Frontend` `net6.0-windows` WinForms project)
  - `demos/Research.Benchmark/src/Research.Benchmark.Host/Program.cs:114-115` (stale comment citing `DataVo (Disk)` / `DataVo (Disk+fsync)`)
  - `demos/Research.Benchmark/src/Research.Benchmark.Host/DiskCrudEngineMatrix.cs:16-21` & `WhitepaperEngineMatrix.cs:14` (`datavo`/`datavo-lsm` aliases)
- **What it is & Why it's there:** Leftover CLI flags, comments, and documentation from before `StorageMode.Disk` removal.
- **Proposed Action:** Remove the `"disk"` alias and update `realtime-benchmarks/README.md` + `Research.Benchmark.Host/Program.cs:114-115`; remove stale WinForms reference (or retire `browser-wasm-showcase/`). Keep `disk-crud-wal` scenario slug only if synchronized with `.github/workflows/benchmark.yml:109` and docs.
- **Risk / Dependencies:** Low (check `benchmark.yml:109` before renaming `disk-crud-wal`).
- **Effort Estimate:** `XS`

#### [DONE] `BC-47` — Stale `0.5.0` Release Notes in `Directory.Build.props` & Orphaned `python`-Broken Scripts
- **Exact Location(s):**
  - `Directory.Build.props:19,40,47,52,70,75` (`<Version>0.5.1</Version>` while `<PackageReleaseNotes>` still describe `0.5.0`)
  - `scripts/ci-suite-log-guard.sh:10,19,31` (0 references; hardcodes only 11 old test namespaces missing 17 newer ones; invokes `python` instead of `python3`)
  - `scripts/test-overlap-stress.sh:42` (0 references; invokes `python` instead of `python3`)
- **What it is & Why it's there:** Version bumped to `0.5.1` without updating release notes, plus 2 orphaned helper scripts broken on modern macOS/Ubuntu (`python` missing).
- **Proposed Action:** Update `<PackageReleaseNotes>` in `Directory.Build.props` for `0.5.1`; delete `ci-suite-log-guard.sh` (superseded by CI TRX logging) and either fix `python`→`python3` in `test-overlap-stress.sh` or delete it.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`

---

## 3. Section B — Unfinished Follow-Ups & Loose Ends (`LE-*`)

### Subsystem: `DataVo.Core`

#### [DONE] `LE-01` — Hardcoded Benchmark-Specific `VipExposureReactiveQuery` Shipped in `DataVo.Core`
- **Exact Location(s):**
  - `DataVo.Core/Runtime/Reactive/VipExposureReactiveQuery.cs:7-297` (`:40-52` loose `IsSupported`; `:264` `row[3].AsInt32()` bug on `decimal Stake`)
  - `DataVo.Core/Runtime/Reactive/ReactiveRegistry.cs:514-517`
- **What it is & Why it's there:** A narrow, hardcoded reactive operator written for `demos/Research.Benchmark` (`Orders ⋈ Accounts ⋈ Markets`, hardcoded column names `Id`/`AccountId`/`MarketId`/`Stake`/`IsVip`/`Category` and output `Category`/`TotalExposure`). `IsSupported` only checks table names, join count, `GROUP BY` ending in `Category`, 2 columns, and any `SUM(...)` — never validating the `WHERE` predicate (`IsVip = true`), join predicates, or `SUM(Stake)`. Any user query with similarly named tables silently returns wrong results or crashes. Line 264 also truncates `decimal Stake` via `AsInt32()`.
- **Proposed Action:** Replace with composable multi-table join + aggregate reactive operators (or remove from `DataVo.Core` and tighten `IsSupported` + fix line 264 immediately if retained short-term for the benchmark).
- **Risk / Dependencies:** `demos/Research.Benchmark` (`arena-live-views` / betting risk runner) currently depends on this query shape running reactively.
- **Effort Estimate:** `S` (fix & isolate) / `L` (general 3-way join+aggregate IVM)
- **Resolution (Phase 7):** `VipExposureReactiveQuery` and its tests are deleted. The new `JoinAggregateReactiveQuery` maintains any N-way INNER equi-join with an optional WHERE (qualified or unambiguous columns), `GROUP BY` and `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`: each table's rows are kept typed and bucketed by every join column (no primary key needed; rows are a bag), and each committed row change is joined against the other tables along a precomputed spanning-tree probe plan and fed ±1 into the shared `ReactiveGroupAggregator` (extracted from `AggregateReactiveQuery`, now ordinal-based with typed MIN/MAX state). Self-joins, outer joins, HAVING and ORDER BY/LIMIT are rejected with `NotSupportedException`. `SUM` over an INT column is now a BIGINT, as in SQL, so the benchmark engine and tests read it numerically. Tests: scenario coverage (dimension flips, category moves, deletes, bag duplicates, seeding, zero-alloc delivery), a randomized differential against an in-test oracle on InMemory and LSM, and the rejected shapes; the VIP allocation guard (≤ 2,450 B/insert) and the DataVo/DuckDB/SQLite VIP benchmark test still pass.

#### [DONE] `LE-02` — `BTreeIndexFactory` Defaults `minDegree = 3` Instead of `InMemoryBTreeIndex.DefaultMinDegree` (`50`)
- **Exact Location(s):**
  - `DataVo.Core/Indexing/BTree/BTreeIndexFactory.cs:25`
  - `DataVo.Core/BTree/Core/InMemoryBTreeIndex.cs:15` (`DefaultMinDegree = 50`)
  - `DataVo.Core/Indexing/IndexManager.cs:326` (passes empty `@params` dict)
- **What it is & Why it's there:** `BTreeIndexFactory.CreateIndex` hardcodes `int minDegree = 3;` (max 5 keys/node), ignoring `InMemoryBTreeIndex.DefaultMinDegree = 50` (max 99 keys/node). Because `IndexManager` passes an empty `@params` dictionary, **every production in-memory B-Tree index runs at degree 3** with ~3–4× deeper trees and excessive node allocations.
- **Proposed Action:** Change `BTreeIndexFactory.cs:25` default from `3` to `InMemoryBTreeIndex.DefaultMinDegree` (`50`).
- **Risk / Dependencies:** Very low — check whether any B-Tree split test assumes `BTreeIndexFactory` returns degree 3.
- **Effort Estimate:** `XS`
- **Resolution (Phase 3):** `BTreeIndexFactory` defaults to `InMemoryBTreeIndex.DefaultMinDegree` (now `internal const`, 50); an explicit `minDegree` parameter is still honored. Covered by `Phase3AuditFixTests`.

#### [DONE] `LE-03` — `UnionSelect` Error Propagation Bug (Failed `UNION` Silently Returns `Success`)
- **Exact Location(s):**
  - `DataVo.Core/Parser/DQL/UnionSelect.cs:18-24,32-38`
  - `DataVo.Core/Parser/Actions/BaseDbAction.cs:127`
- **What it is & Why it's there:** When `leftResult.IsError` or `branchResult.IsError` is true, `UnionSelect` copies `Messages`/`Data`/`Fields` and returns without setting `HasErrors = true` or calling `AddError(...)`. Because `DataVoErrorClassifier.Clean` strips `"Error:"` from `leftResult.Messages`, `BaseDbAction.Perform` sees `HasErrors == false` and no `"Error:"` prefix — turning a failed `UNION` query into `QueryResult.Success`!
- **Proposed Action:** Set `HasErrors = true` and propagate `_firstError` / `ErrorCode` on both branch error paths in `UnionSelect.cs`, and add a regression unit test.
- **Risk / Dependencies:** Low — pure correctness fix.
- **Effort Estimate:** `XS`
- **Resolution (Phase 2A):** Branch failures go through `AddError(QueryResult)` (sets `HasErrors`, keeps code/position); regression cases added to `ErrorContractTests.DetectedFailures`.

#### [DONE] `LE-04` — Security & Authorization Subsystem Bugs & API Bypasses (`Runtime/Security`)
- **Exact Location(s):**
  - `DataVo.Core/Runtime/Security/SessionSecurityStore.cs:143-263,383-400,504-519`
  - `DataVo.Core/DataVoContext.cs:498-896,957-1090`, `CompiledQueries/DataVoCompiledQuery.cs`, `Streaming/RowRefStream.cs`
  - `DataVo.Core/Parser/Actions/BaseDbAction.cs:77-111`
  - `DataVo.Core/Parser/Commands/Grant.cs:22`, `Revoke.cs:22`
- **What it is & Why it's there:** Multiple incomplete wiring and security bugs:
  1. **Privilege escalation (`SessionSecurityStore.cs:391-399,504-519`):** `InferPrimaryRole` maps a user with only `ManageSchema` or `ManageSecurity` to `DatabaseRole.Admin`, and `HasPermission` unconditionally returns `true` for `DatabaseRole.Admin` — escalating to full admin!
  2. **Non-SQL API bypass:** `SessionSecurity.Authorize` is never called in `DataVoContext.BulkInsert`, `InsertTyped`, `InsertTypedBatch`, `Insert<T>`, `ApplyBatch`, `SearchNearest`, `StreamRef`, `Subscribe`/`SubscribeZeroAlloc`, `CreateSnapshot`/`RestoreSnapshot`, or `DataVoCompiledQuery.*`.
  3. **Missing `EnsureInitialized(config)`:** All 8 role/grant mutators (`CreateRole`..`RevokePermissionsFromRole`, lines 143–263) omit `EnsureInitialized`, operating on unseeded state if called first.
  4. **Stale sessions & in-memory-only grants:** `DropUser` does not evict active sessions; `Grant*`/`Revoke*` do not refresh cached `EffectivePermissions`; all roles/grants vanish on LSM restart; `CreateUser`/`DropUser` mutate `DataVoConfig.AuthorizationUsers` in place.
  5. **Fragile namespace inference & missing SQL:** `BaseDbAction.InferRequiredPermission` infers rights from namespace strings (`Vacuum` is in `.Parser.DML`, requiring only `WriteData`); `GRANT/REVOKE ROLE ... TO/FROM ROLE` throws `InvalidOperationException`.
- **Proposed Action:** Fix the `HasPermission` escalation bug and missing `EnsureInitialized` calls immediately; enforce authorization on direct `DataVoContext`/`CompiledQuery`/`StreamRef`/`Reactive` APIs; refresh/invalidate sessions on security mutations; persist security metadata in LSM mode if auth is a supported production feature.
- **Risk / Dependencies:** Medium — audit existing `DataVo.Tests/Security/` tests when changing `HasPermission` and session eviction.
- **Effort Estimate:** `M`–`L`
- **Resolution (Phase 3):** `HasPermission` now checks resolved grants only (`Admin` grant or the exact permission) and `InferPrimaryRole` returns `Admin` only for the explicit `Admin` grant, so `ManageSchema`/`ManageSecurity` no longer escalate. All 8 role/grant mutators take the config and call `EnsureInitialized`. `CreateUser`/`DropUser` no longer mutate `DataVoConfig.AuthorizationUsers`; every security mutation re-resolves logged-in sessions (`RefreshSessionsLocked`) and evicts sessions of dropped users. Direct APIs authorize through `DataVoEngine.AuthorizeDirectAccess` (no-op unless `EnableAuthorization`): `BulkInsert`, `InsertTyped`, `InsertTypedBatch`, `Insert<T>`, `ApplyBatch`, `RestoreSnapshot` (ManageSchema + WriteData), `SearchNearest`, `StreamRef` (creation and enumeration/in-place scan), `CreateSnapshot`, every reactive `Subscribe*` (via `ReactiveRegistry.ResolveDatabase`), and `DataVoCompiledQuery` select/prepare/update entry points plus prepared typed handles. **Still open (out of Phase 3 scope):** LSM persistence of roles/grants, namespace-based `InferRequiredPermission`, and `GRANT/REVOKE ROLE … TO/FROM ROLE`.

#### [DONE] `LE-05` — `ALTER TABLE` & `VACUUM` Corrupt HNSW Vector Indexes, Ignore Columnar Indexes, & Skip MVCC Cleanup
- **Exact Location(s):**
  - `DataVo.Core/Parser/DDL/AlterTableAddColumn.cs:76,81-104`
  - `DataVo.Core/Parser/DDL/AlterTableDropColumn.cs:66,76-119`
  - `DataVo.Core/Parser/DDL/AlterTableModifyColumn.cs:88,93-120`
  - `DataVo.Core/Parser/DML/Vacuum.cs:64-85`
- **What it is & Why it's there:** `RewriteTable` (copy-pasted identically in all 3 `AlterTable*` classes) and `Vacuum.cs:64-85` rebuild every catalog index as a scalar B-Tree (`new ScalarIndexContents()` → `Indexes.CreateIndex(...)`), corrupting/failing on **HNSW/FLAT vector indexes**. None of the four rebuild or invalidate **columnar indexes** (`Engine.Columnar`) or clear MVCC version metadata after renumbering row IDs. `AlterTableDropColumn`/`ModifyColumn` also check `GetTableIndexedColumns` (which excludes columnar indexes), allowing columnar-indexed columns to be dropped/modified silently, and use case-sensitive `==` on column names (while `CatalogStore` uses `OrdinalIgnoreCase`). Also, explicit *"in this version"* blocks reject altering PK/UNIQUE/FK/indexed columns.
- **Proposed Action:** Extract a shared `TableRewriteHelper`, preserve index type (`HNSW`/`FLAT`/`COLUMNAR`/`BTREE`) during rebuild, invalidate/rebuild columnar indexes & MVCC version tables on row-ID renumbering, and switch column-name comparisons to `StringComparison.OrdinalIgnoreCase`.
- **Risk / Dependencies:** High correctness value for any table combining `ALTER TABLE` / `VACUUM` with vectors or columnar indexes.
- **Effort Estimate:** `M`
- **Resolution (Phase 3):** New `Parser/DDL/TableRewrite` holds the shared rewrite: `ReplaceRows` (ALTER) and `RebuildAfterRenumber` (VACUUM) clear the table's MVCC versions/retained images, drop every index (which also deletes HNSW graph snapshots) and rebuild each as its own kind through the extracted `DataVoEngine.RebuildIndexesFromRows` (HNSW/FLAT with metric and options, scalar key indexes), then request a columnar rebuild. `AlterTableDropColumn`/`ModifyColumn` treat columnar-indexed columns as indexed (`TableRewrite.IsIndexedColumn`) and match column, key and FK names with `OrdinalIgnoreCase`, resolving the stored spelling first. The "in this version" PK/UNIQUE/FK/indexed-column rejections are unchanged.

#### [DONE] `LE-06` — Volcano Join `NULL = NULL` Bug, Missing `DISTINCT` Aggregate, Static Cardinality State & `EXPLAIN` Cost Disconnect
- **Exact Location(s):**
  - `DataVo.Core/Execution/Volcano/InnerJoinOperator.cs:67,151-156`
  - `DataVo.Core/Execution/Volcano/NestedLoopJoinOperator.cs:100,149-154`
  - `DataVo.Core/Execution/Volcano/HashAggregateOperator.cs:84-131`
  - `DataVo.Core/Parser/DQL/Select.cs:98-101` (process-wide `static _joinCardinalityFeedback*`)
  - `DataVo.Core/Parser/DQL/Select.Planner.cs:64,371-376` (`UnknownRowCountEstimate = 1`)
- **What it is & Why it's there:**
  1. Volcano `InnerJoinOperator` and `NestedLoopJoinOperator` map `null` join keys to `"<NULL>"`, violating SQL three-valued logic (`NULL = NULL` wrongly matches!).
  2. `HashAggregateOperator` lacks `DISTINCT` aggregate support (and Volcano lacks outer/cross joins, `HAVING`, window operators).
  3. Join cardinality feedback is stored in process-wide `static` fields on `Select`, leaking statistics across independent `DataVoEngine` instances.
  4. `BuildPlannerDiagnostics` (`EXPLAIN`) passes `useRowMaterializationForEstimates: false`, forcing `EstimateFromTableRowCount` to return `1`, which makes `EXPLAIN` almost always report `Legacy` cost even when runtime chooses `Volcano`.
- **Proposed Action:** Skip `null` keys in Volcano inner/nested-loop joins; move join cardinality feedback onto `DataVoEngine`; wire `TryEstimateRowCount` (metadata-only, no materialization) into `BuildPlannerDiagnostics`.
- **Risk / Dependencies:** Low-Medium; verify `DataVo.Tests/Execution/` Volcano tests.
- **Effort Estimate:** `M`
- **Resolution (Phase 3):** Shared `Volcano/JoinKeys.TryNormalize` gives `NULL` no key, so inner and nested-loop joins never match `NULL = NULL`. Join cardinality feedback moved from `Select`'s statics to the per-engine `JoinCardinalityFeedbackStore` (`DataVoEngine.JoinCardinalityFeedback`), including load/trim/persist. `EXPLAIN` estimates use `TableDetail.TryEstimateRowCountFromMetadata` (cached rows or `IRowCountEstimator`, never materialization); `ExplainTests` now assert cost grows with stored rows. **Still open:** `DISTINCT` aggregates, outer/cross joins, `HAVING` and window operators in Volcano.

#### [DONE] `LE-07` — `InMemoryStorageEngine.CompactTable` Throws on Typed Rows & `WasmStorageBackend` Strips Capability Interfaces
- **Exact Location(s):**
  - `DataVo.Core/StorageEngine/Memory/InMemoryStorageEngine.cs:762-776`
  - `DataVo.Core/StorageEngine/Backends/WasmStorageBackend.cs:13-32`
- **What it is & Why it's there:** `InMemoryStorageEngine.CompactTable` throws `InvalidOperationException("In-memory compaction is not supported for tables containing typed rows.")`, so `VACUUM` crashes on any in-memory table populated via `DataVoTable<T>` / `InsertTyped`. Meanwhile, `WasmStorageBackend` only implements `IStorageBackend` (not `ITypedRowStorageEngine`, `IPagedRowScan`, `IPackedRowSource`, `IInMemoryStorageSnapshotProvider`, `IRowCountEstimator`, or `IColumnarRowStore`), silently stripping every fast path when `StorageMode.Wasm` falls back to in-memory.
- **Proposed Action:** Support compacting surviving typed rows in `InMemoryStorageEngine.CompactTable`; delegate optional capability interfaces in `WasmStorageBackend` (or instantiate `InMemoryStorageBackend` when `wasmEngine == null`).
- **Risk / Dependencies:** Low-Medium.
- **Effort Estimate:** `S`
- **Resolution (Phase 3):** `TableRows.Compact` renumbers serialized and typed (including kept-apart) rows in order; typed survivors report an empty payload since VACUUM now rebuilds indexes from stored rows. `StorageMode.Wasm` without an OPFS engine resolves to `InMemoryStorageBackend("Wasm")` via `WasmStorageBackend.Create`, keeping every capability interface; a supplied OPFS engine is still wrapped as before. `InMemoryTypedStorageTests` now asserts typed compaction instead of the old throw.

#### [DONE] `LE-08` — Incomplete Transactional / Snapshot Support & Unsafe `UpdateFixedWidthByPrimaryKeyBatch`
- **Exact Location(s):**
  - `DataVo.Core/Runtime/DataVoEngine.cs:530-566`, `StorageEngine/StorageContext.cs:1492-1511` (`CreateSnapshot`/`RestoreSnapshot` throw `NotSupportedException` outside `InMemory`)
  - `DataVo.Core/DataVoContext.cs:543-548,631-636,730-735,860-863` (`BulkInsert`, `InsertTyped`, `InsertTypedBatch`, `Insert<T>`, `ApplyBatch` reject active transactions)
  - `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs:381,514,556,580-640` (compiled updates throw `NotSupportedException` in active transactions; `UpdateFixedWidthByPrimaryKeyBatch` omits both active-tx check and `LockManager` write lock!)
- **What it is & Why it's there:** Direct/compiled write fast paths bypass `TransactionContext`, and snapshots are `InMemory`-only. Most urgently, `UpdateFixedWidthByPrimaryKeyBatch` (added to amortize benchmark WAL fsync) mutates storage without checking `HasActiveTransaction` or acquiring write locks — creating a concurrency/isolation hazard.
- **Proposed Action:** Immediately add active-transaction validation and table/row write-lock acquisition to `UpdateFixedWidthByPrimaryKeyBatch`; track transactional support for `InsertTyped`/`ApplyBatch`/compiled updates and LSM snapshots as explicit roadmap items (or document boundaries clearly).
- **Risk / Dependencies:** High safety value for `UpdateFixedWidthByPrimaryKeyBatch`; Medium effort to wire transactions into direct APIs.
- **Effort Estimate:** `XS` (safety fix) / `L` (full tx + LSM snapshots)
- **Resolution (Phase 3, safety fix):** `UpdateFixedWidthByPrimaryKeyBatch` rejects an active session transaction (shared `ThrowIfActiveTransaction`, also used by the other compiled updates), authorizes `WriteData`, and resolves/validates/patches the whole batch under the table write lock (taken after the commit-clock unit, as `BulkInsert` does). **Reserved for Phase 7:** transactional `InsertTyped`/`ApplyBatch`/compiled updates and LSM snapshots.
- **Resolution (Phase 7):** Direct writes no longer reject or bypass an open transaction. `BulkInsert`, `InsertTyped`, `InsertTypedBatch` and `Insert<T>` validate and buffer their rows in it like SQL `INSERT` (keys checked against committed and buffered rows; provisional keys returned; diagnostics record failures). `ApplyBatch` folds the batch as before and writes it into the transaction (`BatchApplier.ExecuteInTransaction`): rows found through the transaction's view, deletes/updates as parameterized statements (row locks, MVCC, constraints, cascades), inserts buffered, the whole batch under a savepoint, same insert/update/delete/unchanged/missing counts. Compiled `Update`, `UpdateFixedWidth`, `UpdateFixedWidthByPrimaryKey` and `UpdateFixedWidthByPrimaryKeyBatch` run as a cached parameterized `UPDATE` of the transaction (the batch under a savepoint) instead of patching storage. `CreateSnapshot`/`RestoreSnapshot` support `StorageMode.Lsm`: the snapshot holds the catalog and every table's rows; a restore requires an unchanged schema, rewrites all tables in one LSM write batch, rebuilds scalar and vector indexes, and is durable across reopen. Tests: `DirectApiTransactionTests`, `ApplyBatchTests.OpenTransaction_*`, `CompiledQueryRuntimeTests.CompiledUpdate*InsideATransaction*`, `LsmSnapshotTests`.

#### [DONE] `LE-09` — Reactive IVM Follow-Ups (`SubscribeZeroAlloc` Gaps, `RowChange.TypedBefore`, Buffer Leak, Test Seam)
- **Exact Location(s):**
  - `DataVo.Core/Runtime/Reactive/ReactiveRegistry.cs:151-152,195-199,422-450,492` (`AddCompiledForTest` seam; `SubscribeZeroAlloc` throws `NotSupportedException` for 4 of 9 operators; `_incomingBuffer` leak after unsubscribe; unused `sql` param at `:492`)
  - `DataVo.Core/Runtime/Reactive/{ReactiveSubscription,DistinctReactiveQuery,SubqueryReactiveQuery,UnionReactiveQuery}.cs` (missing `IBorrowedReactiveQuery`)
  - `DataVo.Core/Runtime/Changes/RowChange.cs:88-99` (no `TypedBefore`, forcing dictionary boxing on `Delete`/`Update`)
  - `DataVo.Core/Runtime/Reactive/AggregateReactiveQuery.cs:170-213,689-735` (only `Insert` uses `TypedAfter`; no `HAVING`/`ORDER BY`/`LIMIT`/`DISTINCT`)
  - `DataVo.Core/Runtime/Reactive/RecursiveCteReactiveQuery.cs:158,432` (explicitly defers incremental closure maintenance and deep typed state)
  - `DataVo.Core/Runtime/Reactive/{JoinReactiveQuery,TopKReactiveQuery}.cs` (2-table join limit, no `GROUP BY`/`ORDER BY`/`LIMIT` on joins)
- **What it is & Why it's there:** Half-completed zero-allocation IVM migration: even basic L1 `SELECT ... WHERE` (`ReactiveSubscription`) throws `NotSupportedException` in `SubscribeZeroAlloc`. In addition, `ReactiveRegistry.Remove()` never unhooks `_engine.Changes` when `_subscriptions.Count == 0`, causing `_incomingBuffer` to grow unboundedly on engines where a subscription was ever created and disposed!
- **Proposed Action:** Immediately fix the `_incomingBuffer` leak by unhooking/gating `Enqueue` when `_subscriptions.Count == 0` and remove unused `sql` param / `AddCompiledForTest`; next, migrate `ReactiveSubscription`, `Distinct`, `Subquery`, `Union` to `IBorrowedReactiveQuery` and add `TypedBefore` to `RowChange`.
- **Risk / Dependencies:** High memory-leak impact for long-lived engines using reactive views.
- **Effort Estimate:** `XS` (buffer leak fix) / `L` (full zero-alloc IVM completion)
- **Resolution (Phase 3, leak fix):** `ReactiveRegistry` unhooks `Changes.Captured` and clears its buffer when the last subscription is removed (switching capture off only if it turned it on and no other subscriber listens), `Enqueue` drops sets while no subscription exists, and subscribing again re-hooks. The unused `sql` parameter of `CreateQuery` is gone. **Reserved for Phase 7:** `AddCompiledForTest` removal, borrowed emit for the remaining operators, `RowChange.TypedBefore`.
- **Resolution (Phase 7):** `AddCompiledForTest` is removed (the bridge test uses `AddCompiled`). `ReactiveSubscription` (now internal), `DistinctReactiveQuery`, `UnionReactiveQuery` and `SubqueryReactiveQuery` emit borrowed deltas with typed state (`PackedKey`-keyed, shared `ReactiveTupleCounter` for DISTINCT/UNION), and `RecursiveCteReactiveQuery` keeps its base relations and CTE tuples typed, so `SubscribeZeroAlloc` serves every shape `Subscribe` accepts. `RowChange` gains `TypedBefore` and `RowChange.FromTyped`; each image is available typed and as a dictionary, the missing form built lazily and cached. `ChangeRecorder` captures insert/update/delete images as typed rows (per-table cached schema; dictionary snapshot only for values without a typed form), so operators read typed images only and owned readers get dictionaries on demand. `AggregateReactiveQuery` uses typed images for every change kind; MIN/MAX state is a typed multiset (no boxing per change). Out of scope and unchanged: HAVING/ORDER BY/LIMIT on aggregates and the two-table limit of non-aggregate joins.

#### [DONE] `LE-10` — SQL Dialect Gaps & Legacy `TableColumnService` (`DATETIME`) / `GroupBy` Hash-Collision Bugs
- **Exact Location(s):**
  - `DataVo.Core/Services/TableColumnService.cs:12-45` & `Parser/Aggregations/Aggregation.cs:323-336` (`DATETIME` missing from `IsDate`, breaking legacy `MIN`/`MAX` on `DATETIME`!)
  - `DataVo.Core/Parser/Statements/GroupBy.cs:70-83` (groups keyed by 32-bit `GetHashCode().ToString()` — hash collisions silently merge distinct groups!)
  - `DataVo.Core/Runtime/Reactive/RecursiveCteParser.cs:46-48` & `Parser/DQL/Select.Cte.cs` (main parser lacks `WITH RECURSIVE` / recursive CTEs)
  - `DataVo.Core/Parser/Parser.cs:138-147,353,1145,1797,2069-2116,2573` & `Select.WindowHaving.cs:36-39` (window functions hardcoded to `RANK()` with single-column `ORDER BY`; `(` followed by non-`SELECT` silently skipped at `:138-147`; no multi-col `UNIQUE`, parenthesized `UNION`, or `DECIMAL`)
- **What it is & Why it's there:** Two latent correctness bugs (`TableColumnService` omitting `DATETIME` and `GroupBy` using 32-bit `GetHashCode` as group key) alongside intentional/deferred SQL dialect omissions.
- **Proposed Action:** Immediately add `"datetime"` to `TableColumnService.IsDate`, replace `GetHashCode()` group keys in `GroupBy.cs` with canonical value keys, and throw a parse error instead of silently skipping unexpected `(` at `Parser.cs:138-147`. Track `WITH RECURSIVE`, `ROW_NUMBER`/`DENSE_RANK`/`LAG`/`LEAD`, and `DECIMAL` on the SQL roadmap.
- **Risk / Dependencies:** Low risk for the 3 bug fixes; high correctness improvement.
- **Effort Estimate:** `S` (bug fixes) / `L` (SQL dialect expansion)
- **Resolution (Phase 3):** `TableColumnService.IsDate` accepts `datetime`; `GroupBy.BuildGroupKey` builds length-prefixed value keys (NULL has its own marker) instead of 32-bit hash codes; `Parser.ParseStatements` throws `Unexpected token` on a stray `(`. **Still open (roadmap):** `WITH RECURSIVE`, more window functions, multi-column `UNIQUE`, parenthesized `UNION`, `DECIMAL`.

#### [DONE] `LE-11` — HNSW & Vector Index Follow-Ups (`dimension` Stub, Missing `FlatVectorIndex` Snapshots, Browser Check)
- **Exact Location(s):**
  - `DataVo.Core/Indexing/HNSW/HNSWIndexFactory.cs:35-39,98-101` (empty `if (@params.TryGetValue("dimension", ...))` stub; `IsBrowserRuntime()` checks only `BrowserRuntimeFlags.ForceVectorFallback`, not `FileCompat.IsBrowser()`)
  - `DataVo.Core/Runtime/DataVoEngine.VectorSnapshots.cs:29-38,84-103` (snapshots only persist `HNSWIndex`, not `FlatVectorIndex`, and only write on clean `Dispose`)
- **What it is & Why it's there:** Unfinished dimension-validation stub in `HNSWIndexFactory`, `FlatVectorIndex` rebuilds from full table scan on every LSM reopen, and unclean crashes trigger full HNSW rebuilds once WAL catch-up exceeds 50%.
- **Proposed Action:** Delete or wire `"dimension"` validation in `HNSWIndexFactory.cs:35-39`; check `FileCompat.IsBrowser()` in `IsBrowserRuntime()`; evaluate snapshot persistence for `FlatVectorIndex` and periodic HNSW checkpointing.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `S` (cleanup) / `M` (Flat snapshots + periodic HNSW checkpoints)
- **Resolution (Phase 3):** `HNSWIndexFactory` wires `dimension` like the flat factory (with `expectedCount` it reserves capacity and pins the dimension, after `M` is applied) and `IsBrowserRuntime()` also checks `FileCompat.IsBrowser()`. **Still open:** `FlatVectorIndex` snapshots and periodic HNSW checkpoints.

#### [DONE — fsync; FK locks deferred] `LE-12` — WAL/TXID Durability Edge Case, 2-Assignment Batch Patch Cap, & Tracked FK Concurrency Race
- **Exact Location(s):**
  - `DataVo.Core/Transactions/WalFileStore.cs:233-240` (`TruncateThroughLsn`) & `TransactionIdStateStore.cs:106-117` (`ForcePersistHighWaterMark`) — both use `AtomicFileOperations.ReplaceFromTemp` without parent-directory `fsync` (unlike `LsmDurableFileOperations.WriteFileAtomically`)
  - `DataVo.Core/StorageEngine/IFixedWidthPatchStorageEngine.cs:6-17,36-39` & `LsmStorageEngine.cs:632-635` (hard-capped at 2 assignments, `NotSupportedException` for >2 — guarded by callers)
  - `docs/features/insert-intent-row-locking.md:76` (explicitly tracked v0.5.1 follow-up: FK parent-delete vs. child-insert concurrency race needs shared key locks on referenced parent values)
- **What it is & Why it's there:** Remaining durability and concurrency edge cases documented or discovered during LSM/locking hardening.
- **Proposed Action:** Route `WalFileStore.TruncateThroughLsn` and `TransactionIdStateStore` through parent-directory `fsync` on POSIX; implement shared parent-key locks for FK validation; keep 2-assignment patch cap as intentional fast-path boundary unless >2-column updates become hot.
- **Risk / Dependencies:** Low for directory fsync; Medium for FK shared locking (lock-ordering deadlock analysis required).
- **Effort Estimate:** `S` (fsync) + `M` (FK shared locks)
- **Resolution (Phase 3):** New `AtomicFileOperations.ReplaceFromTempDurably`/`FsyncParentDirectory` (POSIX via `LsmDurableFileOperations.FsyncDirectory`, skipped on Windows and in browsers) are used by `WalFileStore.TruncateThroughLsn` (including the delete-whole-log branch) and `TransactionIdStateStore.PersistHighWaterMark`. **Deferred:** FK shared parent-key locks. Lock-order safety could not be shown within this phase: it needs shared key locks (the key-lock API is exclusive-only), shared locks on every child-insert path (SQL, typed, bulk, `ApplyBatch`, `COMMIT`), exclusive old-key locks on every parent delete/key-change path (`RowDeleter`, `Update`, prepared/compiled updates, batches), and a deadlock analysis against transaction-lifetime row locks; tracked for a dedicated change. Also noted: `CatalogStore` saves its temp file without fsync before the rename.

#### [DONE] `LE-13` — Dead/Buggy MVCC & Unmaintained Index/LSM Metadata
- **Exact Location(s):**
  - `DataVo.Core/MVCC/VersionStorageManager.cs:218-267,449-498` (`VacuumTable` has 0 callers; `GetVersionChain` has 0 production callers because visibility uses `RetainedRowImage`)
  - `DataVo.Core/MVCC/SnapshotVisibilityEvaluator.cs:30-40,67-105` (`IsRowVisible`/`CanDeleteRow`/`FilterVisibleRows` have 0 production callers; **`FilterVisibleRows:97-98` treats `null` version as invisible**, contradicting live `MvccCoordinator.GetVersionOrBase` where `null` = base version `xmin=0`!)
  - `DataVo.Core/MVCC/TransactionIdAllocator.cs:49-64` (`AllocateRange` — 0 prod callers) & `TransactionSnapshot.cs:39` (`CanSee` — 0 prod callers)
  - `DataVo.Core/Indexing/IndexMetadata.cs:4,36,48-73` & `IIndexFactory.cs:8,19` (`CreatedAtUtc`, `ModifiedAtUtc`, `EntryCount`, `IsUnique`, `IsPrimaryKey`, `CacheKey` never read/updated; phantom `"B25"` index type in comments)
  - `DataVo.Core/StorageEngine/Lsm/LsmCompactor.cs:341,346,392` (ignored `bool rowKeyed` param) & `LsmStorageEngine.ColumnarScan.cs:608` (ignored `in ColumnarSnapshot snapshot` param)
- **What it is & Why it's there:** Superseded MVCC helpers (one with a latent visibility inversion bug), unmaintained metadata fields, and dead parameters left after refactors.
- **Proposed Action:** Fix or delete `SnapshotVisibilityEvaluator.FilterVisibleRows`/`IsRowVisible`, delete dead `VersionStorageManager.VacuumTable`, prune unused `IndexMetadata` fields / `"B25"` comments, and remove unused LSM parameters.
- **Risk / Dependencies:** Low (update `SnapshotVisibilityTests.cs` / `MVCCIntegrationTests.cs` if removing unused MVCC helpers).
- **Effort Estimate:** `S`

#### [DONE] `LE-14` — Dead 5-File University CLI Compiler Island (`"abkr"`), Dead `TopologicalSort`, & Dead `Utils/` Helpers
- **Exact Location(s):**
  - `DataVo.Core/Parser/Utils/ParserConfig.cs:3-10` (`FILE_EXTENSION = "abkr"`)
  - `DataVo.Core/Utils/FileHandler.cs:1-27`, `DataVo.Core/Utils/ConsoleInputHandler.cs:1-20`
  - `DataVo.Core/Exceptions/FileExtensionNotSupported.cs:1-9`, `DataVo.Core/Exceptions/NoSourceFileProvided.cs:1-15`
  - `DataVo.Core/Services/TopologicalSort.cs:1-58` (0 callers)
  - `DataVo.Core/Utils/StringExtensions.cs:1-40` (4 of 6 methods have 0 callers; `TruncateLeftRight:24` ignores its `charsToTruncate` param; `EqualsSerialized` has 1 caller in `Enums/Operators.cs:183`)
  - `DataVo.Core/Utils/DictionaryExtensions.cs:35-38` (`ToHashedTable` — 0 callers)
- **What it is & Why it's there:** Completely disconnected internal code from early university coursework (`"Only *.abkr files can be compiled!"`) plus unused extension helpers.
- **Proposed Action:** Delete all 7 dead files (`ParserConfig.cs`, `FileHandler.cs`, `ConsoleInputHandler.cs`, `FileExtensionNotSupported.cs`, `NoSourceFileProvided.cs`, `TopologicalSort.cs`, `StringExtensions.cs` after replacing `Operators.cs:183` with `string.Equals(..., OrdinalIgnoreCase)`) and remove `DictionaryExtensions.ToHashedTable`.
- **Risk / Dependencies:** None (all `internal` except `ToHashedTable`).
- **Effort Estimate:** `XS`

#### [DONE] `LE-15` — Source File Corruption (Raw `0x00` NUL Byte in `EngineCatalog.cs`) & File Hygiene Issues
- **Exact Location(s):**
  - `DataVo.Core/Runtime/EngineCatalog.cs:24-49,130-136,229` (**literal binary `0x00` NUL byte** inside `"..."` at line 229, causing `file` to classify the `.cs` file as binary; spliced XML docs at `:24-49,130-136`)
  - `DataVo.Core/DataVoContext.cs:2` & `DataVo.Core/Parser/Statements/Join.cs:2` (stray mid-file UTF-8 BOM `\uFEFF` on line 2)
  - `DataVo.Core/Parser/DQL/Select.cs:449` & `Select.Planner.cs:57` (stray trailing apostrophe in `"Invalid columns specified'"`)
  - `DataVo.Core/Runtime/Changes/RowChange.cs:94` (stale XML `<see cref="RowChange(string, long, TypedRow)"/>` — ctor takes `RowKey`)
- **What it is & Why it's there:** Accidental raw binary character insertion, copy-paste splice errors, and minor text defects.
- **Proposed Action:** Replace raw `0x00` with `"\0"`, repair XML docs, strip mid-file BOMs, and fix the error message typo.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`

#### [DONE] `LE-16` — Minor `ValueConversion`, `DataVoDateTime`, & `ExecuteUpdate` Runtime Gaps
- **Exact Location(s):**
  - `DataVo.Core/ValueConversion/DataVoValueConverterRegistry.cs:69,108` (`ConvertMaterialized` does not unwrap `Nullable.GetUnderlyingType` — throws `NotSupportedException` reading non-null values into `T?`)
  - `DataVo.Core/DataVoDateTime.cs:104-123` (`TryConvert` does not accept `DateOnly` via `DateObjectAdapter` on .NET 8+)
  - `DataVo.Core/Tables/DataVoTable.Composition.cs:98-110`, `DataVoQuery.Composition.cs:127-146`, `DataVoReactiveLinq.cs:30-35` (`ExecuteUpdate` and uncompiled ordered/paged reactive LINQ always throw `NotSupportedException` if Roslyn interceptors are disabled — intentional design, document prominently)
- **What it is & Why it's there:** Small conversion edge cases when mixing nullable custom types or `DateOnly`→`DATETIME`, plus interceptor-only LINQ surfaces.
- **Proposed Action:** Unwrap `Nullable.GetUnderlyingType` in `DataVoValueConverterRegistry.ConvertMaterialized` and add `DateOnly` handling to `DataVoDateTime.TryConvert`.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `XS`
- **Resolution (Phase 3):** Converter lookups in both `ConvertMaterialized` overloads unwrap `Nullable.GetUnderlyingType`, so non-NULL values materialize into `T?`; `DataVoDateTime.TryConvert` accepts `DateOnly` (via `DateObjectAdapter`) as midnight UTC. The interceptor-only `ExecuteUpdate`/reactive LINQ surfaces remain by design.

---

### Subsystem: `DataVo.Data`

#### [DONE] `LE-17` — `DataVoConnection` Bugs (`ConnectionString` Setter, Parameterless Ctor, `ChangeDatabase`, Path `DataSource`)
- **Exact Location(s):**
  - `DataVo.Data/DataVoConnection.cs:20,28,44-49,52,70-74,88-89,128-132`
  - `DataVo.Data/DataVoConnectionStringBuilder.cs:14`
- **What it is & Why it's there:**
  1. `_builder` is `readonly` — setting `connection.ConnectionString = ...` updates `_connectionString` only, so `Open()`/`Database`/`DataSource` silently keep old values.
  2. No parameterless `public DataVoConnection()` constructor (required by `DbProviderFactory`/DI/ORMs).
  3. `ChangeDatabase(databaseName)` executes `USE {databaseName};` without updating `connection.Database`.
  4. `Open()` executes `CREATE DATABASE IF NOT EXISTS {_builder.DataSource}; USE {_builder.DataSource};`, which fails SQL identifier parsing whenever `DataSource` is a filesystem path like `./mydb` (contradicting its own XML doc examples at `DataVoConnection.cs:20` and `DataVoConnectionStringBuilder.cs:14`!).
- **Proposed Action:** Add parameterless ctor; reparse `_builder` in `ConnectionString` setter (rejecting mutation while open); track `_currentDatabase` in `ChangeDatabase`; decouple `Database` name from filesystem `DataSource` path (or sanitize/derive DB name).
- **Risk / Dependencies:** Low-Medium (verify `DataVo.Tests/ADO/AdoNetTests.cs`).
- **Effort Estimate:** `S`
- **Resolution (Phase 4A):** Parameterless ctor; the `ConnectionString` setter reparses (rejected while open); `ChangeDatabase` updates `Database`; the SQL database comes from an optional `Database` key or is derived from the last `DataSource` path segment.

#### [DONE] `LE-18` — Missing `DbProviderFactory` & Non-Inheriting Read-Only `DataVoConnectionStringBuilder`
- **Exact Location(s):**
  - `DataVo.Data/DataVoConnectionStringBuilder.cs:19-85`
  - `DataVo.Data/DataVoConnection.cs` (missing `DbProviderFactory` override)
  - `docs/manual/client-interfaces/ado-net.md:47,50` (falsely claims inheritance from `DbConnectionStringBuilder` — see `LE-47`)
- **What it is & Why it's there:** No `DataVoFactory : DbProviderFactory` exists, and `DataVoConnectionStringBuilder` is a plain read-only class instead of deriving from `System.Data.Common.DbConnectionStringBuilder` with writable properties and `ConnectionString` output.
- **Proposed Action:** Inherit `DataVoConnectionStringBuilder` from `DbConnectionStringBuilder` with writable `StorageMode`/`DataSource` properties, implement `DataVoFactory : DbProviderFactory`, and override `DataVoConnection.DbProviderFactory`.
- **Risk / Dependencies:** Low (check `netstandard2.1` + AOT compatibility of `DbConnectionStringBuilder`).
- **Effort Estimate:** `S`
- **Resolution (Phase 4A):** `DataVoConnectionStringBuilder : DbConnectionStringBuilder` with writable `StorageMode`/`DataSource`/`Database`; new `DataVoFactory`, returned by `DataVoConnection.DbProviderFactory`.

#### [DONE] `LE-19` — `DataVoException` Inherits `Exception` Instead of `DbException` & `ExecuteInternal` Drops Error Metadata
- **Exact Location(s):**
  - `DataVo.Data/DataVoException.cs:6`
  - `DataVo.Data/DataVoConnection.cs:168` (vs `DataVoCommand.cs:211-216`)
- **What it is & Why it's there:** `public class DataVoException : Exception` breaks `catch (DbException)` across ADO.NET/Dapper consumers. Additionally, `DataVoConnection.ExecuteInternal` throws `new DataVoException(...)` without setting `ErrorCode`/`Line`/`Column`.
- **Proposed Action:** Change base class to `System.Data.Common.DbException` and populate `ErrorCode`/`Line`/`Column` in `DataVoConnection.ExecuteInternal`.
- **Risk / Dependencies:** Low (`DbException` inherits `Exception`, so source-compatible for `catch (Exception)` and fixes `catch (DbException)`).
- **Effort Estimate:** `XS`
- **Resolution (Phase 4A):** `DataVoException : DbException`; `ExecuteInternal` carries `ErrorCode`/`Line`/`Column` via a shared `DataVoException.FromResult`.

#### [DONE] `LE-20` — `DataVoTransaction` Ignores `IsolationLevel`, Omits `SupportsSavepoints = true`, & Leaks Open Transactions on `Dispose`
- **Exact Location(s):**
  - `DataVo.Data/DataVoConnection.cs:135-140`
  - `DataVo.Data/DataVoTransaction.cs:16-97`
- **What it is & Why it's there:** `BeginDbTransaction` ignores `isolationLevel` and hardcodes `IsolationLevel.Serializable`; `SupportsSavepoints` is not overridden (defaults to `false` on .NET 8+ despite implementing savepoints!); `Dispose(bool)` is not overridden, so throwing inside `using var tx = conn.BeginTransaction()` leaks an active transaction until connection close.
- **Proposed Action:** Override `SupportsSavepoints => true` (`#if NET8_0_OR_GREATER`), override `Dispose(bool)` to auto-rollback uncommitted transactions, and validate/document `IsolationLevel` mapping (Snapshot vs Serializable).
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `XS`
- **Resolution (Phase 4A):** `SupportsSavepoints => true` (net8+); `Dispose` rolls back uncommitted transactions; isolation levels up to `Snapshot` run as `Snapshot`, `Serializable`/`Chaos` throw `NotSupportedException`.

#### [DONE] `LE-21` — `DataVoCommand` Stubs (`Cancel`, `CommandTimeout`, `CommandType`, `CommandBehavior`, `DbTransaction`) & Fragile `ExecuteNonQuery` Scraping
- **Exact Location(s):**
  - `DataVo.Data/DataVoCommand.cs:54,57,88-92,97-101,107-125,153,252-270`
- **What it is & Why it's there:** `Cancel()` is `{ }`; `CommandTimeout`, `CommandType`, `_transaction`, and `CommandBehavior` (including `CloseConnection`, `SingleRow`, `SingleResult`, `SchemaOnly`) are ignored. `ExecuteNonQuery()` scrapes `"Rows affected:"` strings from `QueryResult.Messages` instead of reading a structured integer count.
- **Proposed Action:** Enforce `CommandBehavior.CloseConnection`/`SingleRow`/`SingleResult`, reject unsupported `CommandType` with `NotSupportedException`, validate `_transaction` matches `_connection`, and expose structured `RowsAffected` on `QueryResult`.
- **Risk / Dependencies:** Low-Medium (adds `RowsAffected` property to `DataVo.Core.Contracts.Results.QueryResult`).
- **Effort Estimate:** `S`
- **Resolution (Phase 4A):** `QueryResult.RowsAffected` (structured, `-1` for non-DML) feeds `ExecuteNonQuery`, metrics and `DataVoSql`; `CloseConnection`/`SingleRow`/`SingleResult` are enforced; non-`Text` `CommandType` and foreign/completed transactions are rejected. `Cancel`/`CommandTimeout` remain no-ops.

#### [DONE] `LE-22` — `DataVoDataReader` & `DataVoParameter` Bugs/Stubs (`IsClosed`, `GetBytes`/`GetChars`, `NextResult`, `DBNull`, Schema, Async)
- **Exact Location(s):**
  - `DataVo.Data/DataVoDataReader.cs:50,53,76-81,84-90,120-128,165`
  - `DataVo.Data/DataVoParameter.cs:19-42,107,139-150`, `DataVo.Data/DataVoCommand.cs:246`
- **What it is & Why it's there:**
  - `IsClosed` (`:50`) has no setter and `Close()`/`Dispose(bool)` aren't overridden → **always `false`**.
  - `NextResult()` (`:76-81`) increments `_resultIndex` past `_results.Count` without clamping → subsequent `FieldCount`/`HasRows` throw `ArgumentOutOfRangeException`.
  - `GetString(ordinal)` (`:165`) returns `""` on `DBNull.Value` instead of throwing `InvalidCastException`.
  - `GetOrdinal` (`:86`) is strictly case-sensitive (ADO.NET spec requires case-insensitive fallback).
  - `GetBytes` and `GetChars` (`:120-128`) throw `NotSupportedException`; `RecordsAffected` hardcoded `-1`; missing `GetSchemaTable()`, `GetColumnSchema()`, `GetProviderSpecificValue()`, and true `*Async` overrides.
  - `DataVoParameter` ignores `DbType`/`Direction`/`Size`; `DataVoParameterCollection.Contains(object)` throws `InvalidCastException` on non-`DataVoParameter`; `GetParameter(string)` throws `InvalidOperationException` instead of `IndexOutOfRangeException`.
- **Proposed Action:** Fix `IsClosed`, `NextResult()` clamping, `DBNull` guard in `GetString`, case-insensitive `GetOrdinal`, `DataVoParameterCollection` edge cases, and implement `GetSchemaTable()`/`GetBytes`/`GetChars`.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `S`
- **Resolution (Phase 4A):** Real `IsClosed` via `Close`/`Dispose`, clamped `NextResult`, `InvalidCastException` on NULL `GetString`, case-insensitive `GetOrdinal` fallback, `GetSchemaTable`, `GetBytes`/`GetChars`, `RecordsAffected`; the parameter collection follows ADO.NET exception contracts. `DbType`/`Direction`/`Size` and true async overrides remain out of scope.

---

### Subsystem: `DataVo.EntityFrameworkCore`

#### [DONE] `LE-23` — Preview Bridge Architecture (Not a Real EF Core Provider)
- **Exact Location(s):**
  - `DataVo.EntityFrameworkCore/Infrastructure/Internal/DataVoOptionsExtension.cs:51-63,136` (`IsDatabaseProvider => false`; `Validate()` throws unless another EF provider like `UseInMemoryDatabase` is registered)
  - `DataVo.EntityFrameworkCore/DataVo.EntityFrameworkCore.csproj:10,14` (`net10.0`-only, not AOT-compatible)
- **What it is & Why it's there:** `DataVo.EntityFrameworkCore` is a sync/DDL/DML bridge layered on top of a host EF provider (`UseInMemoryDatabase`), implementing zero EF relational provider services (`IDatabaseProvider`, `IRelationalConnection`, `IQuerySqlGenerator`, `IMigrationsSqlGenerator`, `IHistoryRepository`, etc.).
- **Proposed Action:** Strategic decision: either keep explicitly scoped as a lightweight sync bridge (and fix bugs `LE-24`–`LE-28`), or invest in a full `IDatabaseProvider` + relational provider built on `DataVo.Data`.
- **Risk / Dependencies:** Architectural scope decision.
- **Effort Estimate:** `L` (full provider; multi-week)
- **Resolution (Phase 4B):** **Decision: keep `DataVo.EntityFrameworkCore` scoped as a lightweight sync/DDL/DML bridge over a host EF provider.** No `IDatabaseProvider` / relational SQL generator stack is planned; `LE-24`–`LE-28` were fixed within that scope. Applications that need a full EF provider should use `DataVo.Data` (ADO.NET) directly.

#### [DONE] `LE-24` — Critical Bug: `UseInMemoryStorage()` / `DataVoStorageMode.InMemory` Is Broken Across Bridge Operations
- **Exact Location(s):**
  - `DataVo.EntityFrameworkCore/DataVoDbContextExtensions.cs:74`, `DataVoChangeTrackerExtensions.cs:72`, `DataVoEntityMaterializer.cs:68`, `DataVoNativeQueryExecutor.cs:24,80`, `DataVoDbContext.cs:596`, `DataVoDatabaseFacadeExtensions.cs:105`
  - `DataVo.EntityFrameworkCore/Extensions/DataVoDbContextOptionsExtensions.cs:162` (XML doc example demonstrating broken `UseInMemoryStorage()`)
- **What it is & Why it's there:** Every bridge method opens and disposes a brand-new `DataVoConnection`. Because `DataVoEngineRegistry` returns `root = null` for `StorageMode.InMemory`, every call spins up a fresh empty in-memory engine and discards it — so `EnsureDataVoCreated()` tables vanish immediately, `SaveChanges()` fails, and `LoadFromDataVo()` reads empty tables! (All working EF tests use `StorageMode=Lsm`.)
- **Proposed Action:** Share a scoped/named in-memory `DataVoConnection` (or named `InMemory` registry key in `DataVoEngineRegistry`) across bridge operations, or reject `InMemory` in bridge mode with a clear error until shared.
- **Risk / Dependencies:** High user impact (documented example currently fails).
- **Effort Estimate:** `S`
- **Resolution (Phase 4B):** New internal `DataVoBridgeStorage` opens every bridge connection. Lsm keeps one connection per operation (the engine registry shares the directory); `InMemory` keeps one process-wide connection per `DataSource`+`Database` that bridge operations share, serialized by a per-store lock, until `EnsureDeleted()` drops it. `EnsureCreated()` now returns whether any table was created, in both modes.

#### [DONE] `LE-25` — Data Integrity Bug: `SaveChanges` Bridge Executes DML Without a DataVo Transaction
- **Exact Location(s):**
  - `DataVo.EntityFrameworkCore/DataVoChangeTrackerExtensions.cs:72-129`
- **What it is & Why it's there:** `ExecuteDataVoCrud` loops over `Added`/`Modified`/`Deleted` entities executing separate autocommitted `ExecuteNonQuery()` calls without `BeginTransaction()`/`Commit()`. A failure on entity #3 leaves #1–#2 permanently committed in DataVo while EF rolls back. Also line 129 mislabels every outer error as `DataVoEfOperation.Insert`.
- **Proposed Action:** Wrap the DML loop in `using var tx = connection.BeginTransaction(); ... tx.Commit();` and fix the outer catch operation tag.
- **Risk / Dependencies:** High data-integrity value; low implementation risk.
- **Effort Estimate:** `XS`
- **Resolution (Phase 4B):** `ExecuteDataVoCrud` runs all statements in one `BeginTransaction()`/`Commit()`; any failure rolls back everything, and the outer error reports the operation that was running (`Connection`, `Insert`, `Update` or `Delete`). Enabling it exposed an engine bug, fixed in `InsertRowService`: an INSERT buffered in a transaction failed its FK check when the parent row was inserted earlier in the same transaction (COMMIT already re-checks FKs after applying parents). Covered by `TransactionForeignKeyTests`.

#### [DONE] `LE-26` — Unparameterized SQL Interpolation & `DateTime`/`DateTimeOffset` Truncated to SQL `DATE` (`yyyy-MM-dd`)
- **Exact Location(s):**
  - `DataVo.EntityFrameworkCore/DataVoDbContextExtensions.cs:247-250,287-288`
  - `DataVo.EntityFrameworkCore/DataVoChangeTrackerExtensions.cs:198,214,246,253-268`
  - `DataVo.EntityFrameworkCore/Infrastructure/Internal/DataVoNativeQueryExecutor.cs:460,670-674,1015-1039`
- **What it is & Why it's there:** All CRUD and native queries interpolate raw SQL literals instead of binding `DataVoParameter`. Worse, `DateTime`, `DateTimeOffset`, and `TimeOnly` map to SQL `DATE` formatted as `'yyyy-MM-dd'`, **silently discarding time-of-day** even though DataVo 0.5.0 added native `DATETIME`. `BuildLikePredicate` (`:670-674`) also fails to escape `%`/`_` wildcards.
- **Proposed Action:** Migrate CRUD and native query execution to `DataVoParameter` bindings and map `DateTime`/`DateTimeOffset` to SQL `DATETIME` preserving full timestamps.
- **Risk / Dependencies:** High correctness/security impact; verify existing EF bridge tests expecting `DATE`.
- **Effort Estimate:** `M`
- **Resolution (Phase 4B):** CRUD (`INSERT`/`UPDATE`/`DELETE` values and keys), native queries (constants, captured values, LIKE patterns, vectors) and `ExecuteSqlOnDataVo` bind `DataVoParameter`s via `DataVoSqlParameterList`; only `DEFAULT` literals in DDL are still formatted (`DataVoBridgeValues.FormatLiteral`, full ISO 8601 UTC for instants). `DateTime`/`DateTimeOffset`/`TimeOnly` map to `DATETIME` (`TimeOnly` as an instant on `0001-01-01`); enums, `char` and unsigned integers are converted for binding (64-bit enums → `BIGINT`). LIKE arguments escape `\`, `%` and `_`.

#### [DONE] `LE-27` — Schema Generation Breaks on `float[]`/`ReadOnlyMemory<float>` Vectors & Composite PKs
- **Exact Location(s):**
  - `DataVo.EntityFrameworkCore/DataVoDbContextExtensions.cs:186-216,222-277`
  - `DataVo.EntityFrameworkCore/DataVoVectorDbFunctions.cs:34,47` (messages cite `COSINE_DISTANCE`/`L2_DISTANCE` instead of `<=>`/`<->`)
  - `DataVo.EntityFrameworkCore/DataVoDbContext.cs:54-69` (`SupportedGuardedOperators` lists `CosineDistance` but omits `L2Distance`)
- **What it is & Why it's there:** `GetDataVoType` (`:276`) throws `NotSupportedException` for `float[]` and `ReadOnlyMemory<float>`, contradicting `DataVoVectorDbFunctions` and `DataVoNativeQueryExecutor` (entities with vectors cannot run `EnsureCreated()`!). Composite PKs emit invalid per-column `PRIMARY KEY` clauses (`:190-193`); `NOT NULL`, `UNIQUE`, secondary indexes, and owned entities are never emitted.
- **Proposed Action:** Add `VECTOR(n)` mapping (via max-length or custom annotation), emit table-level composite PKs (or reject cleanly), emit `NOT NULL`/`UNIQUE`/secondary indexes, and fix the `DataVoVectorDbFunctions`/`SupportedGuardedOperators` text mismatches.
- **Risk / Dependencies:** Medium.
- **Effort Estimate:** `M`
- **Resolution (Phase 4B):** `float[]`/`ReadOnlyMemory<float>` map to `VECTOR(n)` from `HasDataVoVectorDimensions(n)`, `[VectorDimensions(n)]`, `HasMaxLength(n)` or an explicit `HasColumnType(...)` (missing dimension → clear `NotSupportedException`). Composite keys emit a table-level `PRIMARY KEY (...)`; `NOT NULL` follows column nullability; single-column unique indexes/alternate keys emit `UNIQUE` (multi-column unique is rejected with a clear error); non-unique indexes emit `CREATE INDEX` for newly created tables (idempotent). Only explicitly configured defaults are emitted. `DataVoVectorDbFunctions` messages name `<=>`/`<->`; `SupportedGuardedOperators` lists `L2Distance`. Owned types remain unsupported.

#### [DONE] `LE-28` — Analyzer vs. Translator Mismatches, In-Place `OptionsExtension` Mutation, Dead Code & Triplicated Topological Sort
- **Exact Location(s):**
  - `DataVo.EntityFrameworkCore/Infrastructure/Internal/DataVoQueryTranslationAnalyzer.cs:101,121,169,253-256,271-276` vs `DataVoNativeQueryExecutor.cs:183-198,317-330,339-355,437,833-845`
  - `DataVo.EntityFrameworkCore/DataVoDbContext.cs:296-306,340-346,368-462,521-527` (`Any`/`Count`/`FirstOrDefault` always full-table dump; `asNoTracking: false` ignored on native path; `CancellationToken` ignored)
  - `DataVo.EntityFrameworkCore/Infrastructure/Internal/DataVoOptionsExtension.cs:75-109` & `DataVoDbContextOptionsExtensions.cs:207-218` (mutates extension in-place instead of cloning!)
  - Triplicated topological sort: `DataVoDbContextExtensions.cs:128-165`, `DataVoChangeTrackerExtensions.cs:270-306`, `DataVoEntityMaterializer.cs:335-374` (divergent TPH/cycle behavior)
  - Duplicated `EnsureDeleted`: `DataVoDatabaseFacadeExtensions.cs:64-80` vs `DataVoDatabaseCreator.cs:124-153` (divergent path resolution)
- **What it is & Why it's there:** Analyzer classifies `!(x.Age > 10)`, `Where(x => true)`, and `x.Flag ?? false` as native-translatable, but `DataVoNativeQueryExecutor` throws `NotSupportedException` on all three. `DataVoOptionsExtension.With*` violates EF's immutability contract. Dead methods (`IsSimpleSelectExpression`, `ResolveProperty`, `FallbackOperators`) and 3 diverging topological sort copies remain.
- **Proposed Action:** Make `DataVoOptionsExtension` immutable (clone on `With*`); align analyzer & translator; honor `asNoTracking: false` and `CancellationToken`; delete dead methods; unify topological sort and `EnsureDeleted` into shared internal helpers.
- **Risk / Dependencies:** Medium.
- **Effort Estimate:** `M`
- **Resolution (Phase 4B):** `DataVoOptionsExtension` is immutable (`With*` returns a copy; the options builder re-registers it). Analyzer and translator share `DataVoExpressionHelpers` and the analyzer checks members against the EF model: `!(x.Age > 10)` → `NOT (...)`, constant/captured booleans → `1 = 1`/`1 = 0`, `x.Flag ?? c` and `predicate == bool` translate natively, unmapped members fall back. Tracking queries (`asNoTracking: false`) are reported as guarded fallback and return tracked entities through the mirror; async paths honor `CancellationToken`; `Any`/`FirstOrDefault` use `LIMIT 1` and `Count` uses `COUNT(*)` natively. Dead code removed; one topological sort (`DataVoModelOrdering`) and one `EnsureDeleted` (`DataVoBridgeStorage`) remain.

---

### Subsystem: `DataVo.VectorData`

#### [DONE] `LE-29` — Bug: Configured `IEmbeddingGenerator` Ignored for Text Searches on Pre-Embedded Vector Properties
- **Exact Location(s):**
  - `DataVo.VectorData/DataVoCollectionModel.cs:289-295`
  - `DataVo.VectorData/DataVoCollectionEngine.cs:334-341`
  - `DataVo.VectorData/DataVoVectorStore.cs:18-19`
- **What it is & Why it's there:** `CreateVector` only assigns `embeddingGenerator = generator ?? defaultGenerator` inside `if (kind == DataVoVectorKind.Generated)`. For `ReadOnlyMemory<float>`, `float[]`, and `Embedding<float>` vector properties, `EmbeddingGenerator` stays `null` — causing `collection.SearchAsync("query text", top: 5)` to throw `NotSupportedException` even when a store/collection `IEmbeddingGenerator` is configured! Directly contradicts `DataVoVectorStoreOptions.EmbeddingGenerator` XML docs.
- **Proposed Action:** Assign `embeddingGenerator = generator ?? defaultGenerator` unconditionally for all vector kinds (throwing only when `kind == DataVoVectorKind.Generated && embeddingGenerator is null`), and add a unit test.
- **Risk / Dependencies:** Very low — clean bug fix with high RAG impact.
- **Effort Estimate:** `XS`
- **Resolution (Phase 4A):** The configured generator is kept for every vector kind; text search over pre-embedded vectors works.

#### [DONE] `LE-30` — Unsupported `Microsoft.Extensions.VectorData` Surface (`IsFullTextIndexed` Silently Ignored, Distance/Index/Type Gaps)
- **Exact Location(s):**
  - `DataVo.VectorData/DataVoCollectionModel.cs:165,206,231-240,261-262,283-311`
  - `DataVo.VectorData/DataVoCollectionEngine.cs:274` (dynamic `null` `Guid` key throws `ArgumentException` instead of auto-generating)
  - `DataVo.VectorData/DataVoValueMapper.cs:16-69`, `DataVoEmbeddings.cs:31-37`
- **What it is & Why it's there:**
  - `IsFullTextIndexed` on attributes/definitions is silently ignored (no `IKeywordHybridSearchable<TRecord>`).
  - Unsupported distance functions (`DotProductSimilarity`, `NegativeDotProductSimilarity`, `ManhattanDistance`, `HammingDistance`), index kinds (`IvfFlat`, `DiskAnn`, `QuantizedFlat`, `Dynamic`), vector types (`Half`, `double`, `BinaryEmbedding`, `DataContent`), and data types (`byte[]`, `Enum`, `ulong`, `TimeOnly`, `int[]`/`float[]`; `decimal` loses precision via `DOUBLE`).
  - Dynamic upsert with `null` `Guid` key fails instead of auto-generating a `Guid`.
- **Proposed Action:** Throw `NotSupportedException` when `IsFullTextIndexed = true` (don't silently ignore); auto-generate `Guid` keys when `null` in dynamic upsert; prioritize `DotProductSimilarity` (requires `<#>` in `DataVo.Core`) if needed.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `S` (guards + dynamic Guid) / `M` (DotProduct/Half)
- **Resolution (Phase 4A):** `IsFullTextIndexed = true` throws `NotSupportedException`; null dynamic Guid keys are generated. Extra distance functions/index kinds/vector types remain unsupported (they already throw).

#### [DONE] `LE-31` — `DataVoFilterTranslator` Rejects Inline Constructors/Arithmetic, Collection `Contains`, & `LIKE` Wildcards
- **Exact Location(s):**
  - `DataVo.VectorData/DataVoFilterTranslator.cs:95,119,135-139,248-254,266-307,345-348`
  - `DataVo.VectorData/DataVoValueMapper.cs:69` (`IsFilterable = false` for `string[]`/`List<string>`)
- **What it is & Why it's there:** `Evaluate()` (`:266-307`) does not handle `NewExpression`, `BinaryExpression`, or `UnaryExpression` — so common LINQ filters like `r => r.UpdatedAt > new DateTime(2026, 1, 1)`, `r => r.Rank > min + 1`, or `r => r.Rank > -x` throw `NotSupportedException`. `EndsWith`/`Contains` with `%`/`_` throw `NotSupportedException` (no SQL `LIKE ... ESCAPE`); `StringComparison` overloads and `r.Tags.Contains("x")` are unsupported.
- **Proposed Action:** Add AOT-safe `NewExpression` (for `DateTime`, `DateTimeOffset`, `DateOnly`, `Guid`, `TimeSpan`), `BinaryExpression`, and `UnaryExpression` evaluation to `DataVoFilterTranslator.Evaluate()`.
- **Risk / Dependencies:** Low (keep AOT-clean — no `Expression.Compile()`!).
- **Effort Estimate:** `S`
- **Resolution (Phase 4A):** `Evaluate()` handles constructors, binary/unary/conditional/coalesce expressions without `Expression.Compile()`; string filters escape `%`, `_` and `\` through the new backslash `LIKE` escape (`DataVoLikePattern.Escape`).

#### [DONE] `LE-32` — DI, `GetService`, & `CancellationToken` Gaps in `DataVo.VectorData`
- **Exact Location(s):**
  - `DataVo.VectorData/DataVoVectorStoreCollection.cs:61-76,109-122,147-154`
  - `DataVo.VectorData/DataVoDynamicVectorStoreCollection.cs:31-45,78-91,116-123`
  - `DataVo.VectorData/DataVoVectorStoreServiceCollectionExtensions.cs:52-79`
- **What it is & Why it's there:** `GetService` does not expose `DataVoContext` or `IEmbeddingGenerator`; `AddKeyedDataVoVectorStore` has no matching `AddKeyedDataVoCollection` or `AddDataVoDynamicCollection`; `CollectionExistsAsync`, `EnsureCollectionExistsAsync`, `EnsureCollectionDeletedAsync`, and `DeleteAsync` never check `cancellationToken.ThrowIfCancellationRequested()`.
- **Proposed Action:** Add `ThrowIfCancellationRequested()` to all async methods, expose `DataVoContext`/`IEmbeddingGenerator` via `GetService`, and add keyed/dynamic DI extensions.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`
- **Resolution (Phase 4A):** All async collection/store methods check the token; `GetService` exposes `DataVoContext` and the embedding generator; `AddKeyedDataVoCollection`, `AddDataVoDynamicCollection` and `AddKeyedDataVoDynamicCollection` added.

#### [DONE] `LE-33` — Untested Code Paths in `DataVo.VectorData.Tests` & Complete Absence from `DataVo.AotSmoke`
- **Exact Location(s):**
  - `DataVo.VectorData.Tests/DataVoVectorStoreTests.cs`
  - `DataVo.AotSmoke/DataVo.AotSmoke.csproj:20-24`, `DataVo.AotSmoke/Program.cs`
- **What it is & Why it's there:** `DataVo.VectorData` is advertised as Native AOT compatible (`Directory.Build.props:80`) yet is **not referenced or tested in `DataVo.AotSmoke`** (critical given `BC-23` non-public reflection and `Activator.CreateInstance<TRecord>()`). Unit tests also omit multi-vector records, custom `StorageName`, typed `VectorStoreCollectionDefinition`, `[VectorStoreKey(IsAutoGenerated = false)]`, dynamic `typeof(object)` keys, `IN` with `null`/empty, `DataVoSessionPool` eviction, and several mapped CLR types (`DateOnly`, `DateTimeOffset`, `decimal`, `short`, `byte`, `float`, `List<string>`).
- **Proposed Action:** Add `DataVo.VectorData` end-to-end smoke scenario to `DataVo.AotSmoke` and fill the unit-test coverage gaps in `DataVo.VectorData.Tests`.
- **Risk / Dependencies:** High value (actually verifies published AOT claim in CI).
- **Effort Estimate:** `S`
- **Resolution (Phase 4A):** `DataVo.AotSmoke` runs an end-to-end VectorData scenario; `DataVoVectorStoreCoverageTests` covers the listed gaps (and found/fixed a `DateOnly` read-back bug and a session-pool eviction leak).

---

### Subsystem: `DataVo.Generators`

#### [DONE] `LE-34` — Half-Wired `CompileTimeCatalog.IsPrimaryKey` & Overloaded `DATAVOQ001` Diagnostic
- **Exact Location(s):**
  - `DataVo.Generators/Sql/CompileTimeCatalog.cs:36-37`, `DataVoDdlManifestParser.cs:24-30,60-68,77-87`
  - `DataVo.Generators/DataVoQueryGenerator.cs:93,191-202`
  - `DataVo.Generators/Diagnostics/DataVoGeneratorDiagnostics.cs:7-80`
- **What it is & Why it's there:** `DataVoDdlManifestParser` parses `PRIMARY KEY` constraints into `CompileTimeCatalog.IsPrimaryKey`, but `DataVoQueryGenerator.GenerateSelectPlan` only calls `TryResolveSingleColumnIndex` and **never calls `IsPrimaryKey`** — causing manifest PK lookups to fall back to `RuntimeResolve`. Also, `DataVoQueryGenerator.cs:93` emits `DATAVOQ001` (*"SQL is not supported"*) even when the SQL is valid and only the C# method signature is invalid. *(Note: all 17 diagnostics in `DataVoGeneratorDiagnostics.cs` are emitted; zero orphaned.)*
- **Proposed Action:** Wire `catalog.IsPrimaryKey` into `GenerateSelectPlan` (or delete unused PK parsing) and add `DATAVOQ003` for invalid `[DataVoQuery]` method signatures.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `S`
- **Resolution (Phase 4A):** Manifest primary keys emit `CompiledAccessPath.PrimaryKey`, which the runtime now honours (fail-safe); invalid signatures report `DATAVOQ003`.

#### [DONE] `LE-35` — `DataVoQueryGenerator` Bugs, Incremental Caching Regression, & Narrow SQL Subset
- **Exact Location(s):**
  - `DataVo.Generators/DataVoQueryGenerator.cs:22-40,98,103-111,127,167,318-330,450-472`
  - `DataVo.Generators/DataVoQueryShapeParser.cs:7-17,98`
- **What it is & Why it's there:**
  1. Combines raw `Compilation` into `RegisterSourceOutput` (`:28,39`) instead of using `ForAttributeWithMetadataName` + value-equatable models, defeating Roslyn incremental caching on every keystroke.
  2. Hardcodes `public static partial` (`:167`), causing CS0262 compile errors on `internal static partial` methods.
  3. HintName (`:98`) collides across namespaces and method overloads; nested classes unsupported (`:127`).
  4. Typed reader mapping (`:318-330`) omits `float`, `ReadOnlyMemory<float>`, `Vector3`, `byte`, `short` (forcing legacy dictionary fallback `BC-18`).
  5. `DataVoQueryShapeParser` is regex-only and restricted to single-equality `SELECT`/`UPDATE` and single-row `INSERT` (no `DELETE`, `AND`/`OR`, `<`/`>`/`LIKE`/`IN`, `ORDER BY`, `LIMIT`, `SELECT *`).
- **Proposed Action:** Fix incremental pipeline (`ForAttributeWithMetadataName`), preserve method accessibility, disambiguate hint names, add `float`/`ReadOnlyMemory<float>`/`Vector3` typed getters, and document or expand supported SQL shapes.
- **Risk / Dependencies:** Low-Medium; update `DataVoQueryGeneratorTests.cs`.
- **Effort Estimate:** `M`
- **Resolution (Phase 4A):** `ForAttributeWithMetadataName` + value-equatable `QueryMethodModel` (no `Compilation` in outputs; cache test added); declared accessibility, nested types and overloads preserved; unique hint names; typed getters for `float`, `short`, `byte`, `Vector3`, `ReadOnlyMemory<float>`. The regex SQL subset (item 5) is unchanged.

#### [DONE] `LE-36` — Missing `column.Type.IsVector` Handling in `DataVoLinqShapeAnalyzer` & `LinqUpdateTranslator`
- **Exact Location(s):**
  - `DataVo.Generators/Linq/DataVoLinqShapeAnalyzer.cs:85-88`
  - `DataVo.Generators/Linq/LinqUpdateTranslator.cs:155-168,191`
- **What it is & Why it's there:** `InspectOrder` (`:85`) rejects `column.Type.IsVector3` as unorderable, **but forgets `column.Type.IsVector`** (`float[]`/`ReadOnlyMemory<float>`), allowing invalid `.OrderBy(d => d.Embedding)`. Similarly, `LinqUpdateTranslator.Value` (`:155-168`) omits `column.Type.IsVector` (unlike `DataVoTableEmitter.cs:296-299` which calls `VectorParameter`), emitting broken parameter construction for vector `ExecuteUpdate`. Also `LinqUpdateTranslator.Operand` (`:191`) supports only `+`, `-`, `*`.
- **Proposed Action:** Reject `column.Type.IsVector` in `InspectOrder`, route `column.Type.IsVector` through `VectorParameter` in `LinqUpdateTranslator.Value`, and add unit tests.
- **Risk / Dependencies:** Low risk; prevents broken generated code on vector tables.
- **Effort Estimate:** `XS`
- **Resolution (Phase 4A):** `InspectOrder` rejects vector keys (`DATAVOL004`); vector `ExecuteUpdate` setters bind through `VectorParameter`.

#### [DONE] `LE-37` — Unsupported LINQ Operators/Types, Global Mutable Test Hook & `SyntaxTree` Pinning in `DataVo.Generators`
- **Exact Location(s):**
  - `DataVo.Generators/Tables/DataVoTableInspector.cs:244-247,275-309`, `DataVoTableEmitter.cs:14-52`
  - `DataVo.Generators/Linq/DataVoLinqAnalyzer.cs:29,31-46,148`, `LinqConditionTranslator.cs:102-111,204-240`, `LinqModel.cs:115`
- **What it is & Why it's there:**
  - `[DataVoTable]` lacks `byte`/`short`/`decimal`/`enum`/composite PK and generated `DeleteById`.
  - Interceptors don't cover chained `.Where().Where()`, `.Where().FirstOrDefault(pred)`, `Sum` on nullable numeric columns (`:148`), general `!expr` / string `StartsWith`/`Contains` / `IN` predicates, or `DATE`/`DATETIME`/`VARCHAR` scan kernels (`:102-111`).
  - `DataVoLinqAnalyzer.UsePathLocations` (`:29`) is a mutable process-wide static boolean forcing serial test collections.
  - `LinqInspection.Tree` (`LinqModel.cs:115`) retains a Roslyn `SyntaxTree` reference in the incremental pipeline output model (IDE memory pinning).
- **Proposed Action:** Replace `UsePathLocations` with an analyzer config option; remove `SyntaxTree` from `LinqInspection` model; prioritize chained `.Where()` and nullable `Sum` next.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `M`
- **Resolution (Phase 4A):** `build_property.DataVoUsePathLocations` replaces the static hook (generator tests no longer serialize); `LinqInspection` holds a file path and diagnostics are mapped to source trees in a separate cached step. The unsupported LINQ operator/type list is unchanged.

---

### Subsystem: `DataVo.Browser`

#### [DONE] `LE-38` — Severe JS Interop Memory/Perf Defects & Dead Code in `datavo.interop.js` / `datavo.storage.worker.js`
- **Exact Location(s):**
  - `DataVo.Browser/datavo.interop.js:13,46-53,329,349-351`
  - `DataVo.Browser/datavo.storage.worker.js:20-35,44-50,131-146,169,180,192-197,306-318`
- **What it is & Why it's there:**
  - `invokeSync` (`datavo.interop.js:349-351`) allocates a brand-new **8 MB `SharedArrayBuffer` on every single call**, and `datavo.storage.worker.js:309,318` calls `responseBytes.fill(0)` across all 8 MB on every response.
  - Hard 8 MB table-read cap (`readAllRows` serializes the entire table into one JSON string).
  - Single `insertRow` performs **10 OPFS file operations** (5 for `getNextRowId` + 5 for row durability).
  - `getRootDirectory()` (`:20-35`) permanently caches a rejected promise on transient failure; `bytesToBase64` (`:46-53`) does O(N²) char concatenation.
  - Dead code: unused `const encoder = new TextEncoder();` (`datavo.interop.js:329`) and redundant prefix checks (`datavo.storage.worker.js:169,180`).
- **Proposed Action:** Reuse a single `SharedArrayBuffer` without zero-filling 8 MB per call; clear `rootDirectoryPromise` on rejection; fix `bytesToBase64`; delete dead `encoder` and redundant checks; chunk/stream `readAllRows` and batch `insertRows`.
- **Risk / Dependencies:** Medium (requires browser Playwright verification).
- **Effort Estimate:** `M`
- **Resolution (Phase 5):** `invokeSync` reuses one control buffer and one 8 MB `SharedArrayBuffer` for the backend's lifetime (sent once in an `attach` message); the worker writes with `TextEncoder.encodeInto` (no per-call allocation or zero-fill) and the reader copies only the payload. `rootDirectoryPromise` is cleared on rejection; `bytesToBase64` converts in 32 KB chunks; the unused `encoder` and redundant prefix checks are gone. `readAllRows` chunking and batched `insertRows` remain open.

#### [DONE] `LE-39` — `BrowserStorageEngine` & `DataVoInterop` Persistence, API & Error-Contract Loose Ends
- **Exact Location(s):**
  - `DataVo.Browser/BrowserStorageEngine.cs:9-20,46-54,59,68-116,133-147`
  - `DataVo.Browser/Program.cs:73,100-158,212,245-329`
- **What it is & Why it's there:**
  - `ExecuteSql` (`Program.cs:117-120`) calls `PersistBrowserState()` (full catalog XML export + write) in `finally` on **every call**, including read-only `SELECT`s and errors!
  - `CompactTable` (`BrowserStorageEngine.cs:133-147`) drops the table then re-inserts rows one by one — non-atomic (tab close mid-compaction loses all data).
  - `InsertRows` loops calling `InsertRowJS` one row at a time; `RebuildIndexesFromStorage` (`:310-313`) skips `NULL` keys (diverging from Core 0.5.0); `ForceVectorFallback = true` by default (`:73,212`).
  - Inconsistent JSON return shapes (`List<QueryResult>` vs `{ "error": ... }` vs `{ "Error": ..., "Type": ... }`); debug `DiagnoseLexer` exported via `[JSExport]`; stale IndexedDB comments (`BrowserStorageEngine.cs:9-20`); missing `[SupportedOSPlatform("browser")]` on `BrowserStorageEngine`; no parameterized `ExecuteSql` or `Close()` without wiping storage.
- **Proposed Action:** Persist catalog only on DDL/DB changes; make `CompactTable` atomic; unify JSON error envelope; align NULL index rebuild; remove/gate `DiagnoseLexer`; fix stale IndexedDB comments.
- **Risk / Dependencies:** Medium (touches `docs/src/DataVoClient.ts` if JSON envelope changes).
- **Effort Estimate:** `M`
- **Resolution (Phase 5):** `ExecuteSql` persists the catalog only when `Catalog.Generation` changed and the selected database only when it changed (verified in Chromium: read-only and failing statements leave `writeCatalog` untouched). Every SQL response is a `QueryResult[]` (failures via `QueryResult.FromException`, with `ErrorCode`); `DiagnoseLexer` and the client's diagnostic fallback are removed. `CompactTable` no longer drops and re-inserts: rows are independent entries, so it returns them under their ids and writes nothing (atomic). Index rebuild on startup calls Core's new `DataVoEngine.RebuildIndexesFromStorage()` (same NULL-key and vector rules as engine open). `BrowserStorageEngine` has `[SupportedOSPlatform("browser")]` and OPFS docs.

#### [DONE] `LE-40` — Broken `/DataVo-DBMS/` Base-Path URLs in Browser Client/Playwright & 189 Placeholder-Stubbed "Parity" Tests
- **Exact Location(s):**
  - `docs/src/DataVoClient.ts:146-153`, `docs/src/DataVoRuntimeWorker.ts:52-57` (hardcode `/datavo-wasm/...` without `/DataVo-DBMS/` base)
  - `docs/playwright.config.ts:15,21`, `docs/tests/browser/datavo-wasm.spec.ts:340,365,382,463,466,536,539`, `docs/tests/browser/support/wasm-parity.ts:113,144,157`, `needs-specific-suite.ts:26` (hardcode `page.goto("/")` and `import("/src/DataVoClient.ts")`)
  - `docs/tests/browser/support/needs-specific.ts:17-187` (maps all 189 `needsSpecificCode` test IDs — including stale `/Disk/i` at `:182` — onto 6 generic smoke queries, e.g., `makeMvccPlan` runs `INSERT`/`UPDATE`/`SELECT` with no `BEGIN`/`COMMIT`!)
  - `docs/tests/browser/generated/*.json` (stale March 2026 gitignored artifacts claiming `"434/434 passed"`)
- **What it is & Why it's there:** `docs/.vitepress/config.mts:77` sets `base: "/DataVo-DBMS/"`, breaking the hardcoded root URLs in `DataVoClient.ts`, `DataVoRuntimeWorker.ts`, and Playwright. Meanwhile the 189 "needs-specific" browser parity tests are generic placeholders, creating false confidence.
- **Proposed Action:** Use `import.meta.env.BASE_URL` in TS client/worker and prefix `/DataVo-DBMS/` in Playwright; replace the 6 placeholder plans with real assertions or honestly relabel the browser test suite as smoke-only.
- **Risk / Dependencies:** High for browser playground functionality.
- **Effort Estimate:** `M`
- **Resolution (Phase 5):** `DataVoClient.ts`/`DataVoRuntimeWorker.ts` resolve the bundle via `import.meta.env.BASE_URL` (`src/datavoWasmPaths.ts`); Playwright uses `baseURL .../DataVo-DBMS/` and `tests/browser/support/base-path.ts` for `page.goto` and the client module URL; the stale `/Disk/i` match is removed. `datavo-wasm.spec.ts` passes (strict parity included). The placeholder needs-specific plans are unchanged.

---

### Subsystem: `integrations/unity`

#### [DONE] `LE-41` — `com.datavo.core/Samples~/QuickStart/` Is Gitignored (`*~`), Breaking UPM Packaging on Clean Clones
- **Exact Location(s):**
  - `integrations/unity/com.datavo.core/Samples~/QuickStart/` (`DataVoQuickStart.cs`, `DataVo.QuickStart.asmdef` — untracked!)
  - `.gitignore:266` (`*~` without negation rule, unlike `Documentation~` at `:269-270`)
  - `scripts/package-unity-upm.sh:45`, `scripts/write-unity-upm-metadata.mjs:135-141`, `integrations/unity/com.datavo.core/README.md:24`
- **What it is & Why it's there:** `.gitignore:266` matches `Samples~`, so `Samples~/QuickStart/` was never committed to git. On any fresh clone or CI runner, `scripts/package-unity-upm.sh:45` fails trying to copy `Samples~/QuickStart/*`.
- **Proposed Action:** Add `!integrations/unity/com.datavo.core/Samples~/` and `!integrations/unity/com.datavo.core/Samples~/**` to `.gitignore` and commit `Samples~/QuickStart/`.
- **Risk / Dependencies:** High impact (fixes broken UPM packaging); zero risk.
- **Effort Estimate:** `XS`

#### [DONE] `LE-42` — All `.meta` Files Under `com.datavo.unity.mathematics/` Are Gitignored
- **Exact Location(s):**
  - `integrations/unity/com.datavo.unity.mathematics/`
  - `.gitignore:108` (`*.meta`, missing negation unlike `UnitySmoke`/`UnityArenaDemo` at `:289-290`)
- **What it is & Why it's there:** Unity requires `.meta` files for read-only/git-URL UPM packages; without them, Unity ignores or fails to bind assets/asmdefs when installed as an immutable package.
- **Proposed Action:** Add `!integrations/unity/com.datavo.unity.mathematics/**/*.meta` (and for `com.datavo.core` if applicable) to `.gitignore` and commit the `.meta` files.
- **Risk / Dependencies:** Medium for UPM package consumers; zero risk to .NET builds.
- **Effort Estimate:** `XS`

#### [DONE] `LE-43` — `com.datavo.unity.mathematics` Stuck at `0.1.0-preview.1`, Missing `com.datavo.core` Dependency, & Contradictory Docs Referencing Unimplemented `com.datavo.unity.jobs`
- **Exact Location(s):**
  - `integrations/unity/com.datavo.unity.mathematics/package.json:4,6-9`
  - `integrations/unity/com.datavo.unity.mathematics/Documentation~/README.md:45,53-56` vs `integrations/unity/com.datavo.unity.mathematics/README.md:46-53`
- **What it is & Why it's there:** `package.json` is stuck at `0.1.0-preview.1` ("candidate fixture") without declaring `com.datavo.core` in `dependencies`. `Documentation~/README.md:53` claims Editor/Mono tests *"have not run yet"*, contradicting `README.md:46` (which reports Unity 6000.5.4f1 macOS Editor & IL2CPP smoke passing). Both files reference an unimplemented `com.datavo.unity.jobs` package that does not exist in the repo.
- **Proposed Action:** Reconcile `Documentation~/README.md` with `README.md`, mark `com.datavo.unity.jobs` clearly as planned/unimplemented, and align `package.json` version & dependency on `com.datavo.core`.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `XS`
- **Resolution (Phase 5):** `com.datavo.unity.mathematics` is `0.5.1-preview.1` and depends on `com.datavo.core` 0.5.1. The generated dev candidate is now named `com.datavo.core` so the smoke/arena projects satisfy it (manifests, locks and checks updated; Unity EditMode 7/7 and IL2CPP smoke pass). Both READMEs agree on status and mark `com.datavo.unity.jobs` as planned and unimplemented.

#### `LE-44` — Windows x64 IL2CPP Player Unverified & macOS-Only Player Builders / SQLite P/Invoke
- **Exact Location(s):**
  - `tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Editor/PlayerSmokeBuild.cs:18-65` (`BuildTarget.StandaloneOSX` only)
  - `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaPlayerBuild.cs:18-68` (`BuildTarget.StandaloneOSX` only)
  - `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/Sqlite/ArenaSqliteNative.cs:19-21` (hardcodes macOS `sqlite3` P/Invoke)
  - `docs/manual/getting-started/roadmap.md:78`
- **What it is & Why it's there:** Unity evaluation has only been verified on macOS Editor + macOS IL2CPP; no Windows x64 IL2CPP build script or Player proof exists yet, and macOS smoke has not been re-verified post-v0.5 `Disk` removal (blocked by `BC-39`).
- **Proposed Action:** After fixing `BC-39`, re-run macOS smoke on v0.5.1 and add a `BuildWindowsX64Il2Cpp` builder + runner script if moving Unity from evaluation to supported.
- **Risk / Dependencies:** Requires a Windows runner with Unity 6000.5.4f1 + IL2CPP workload.
- **Effort Estimate:** `M`

#### [DONE] `LE-45` — `PlayerSmokeBootstrap.cs` Hardcodes `scenarioCount = 3`
- **Exact Location(s):**
  - `tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Runtime/PlayerSmokeBootstrap.cs:326`
  - `scripts/run-unity-macos-il2cpp-player-smoke.sh:165`
- **What it is & Why it's there:** Hardcodes `scenarioCount = status == "passed" ? 3 : 0` even when `RunPackageScenarios()` (`PackageScenarios/GeneratedTableScenario.cs`) adds a 4th scenario — hiding whether the source-generated scenario actually executed in UPM verification.
- **Proposed Action:** Count executed scenarios dynamically and assert `>= 4` when `DATAVO_UPM_PACKAGE_SCENARIO` is active.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `XS`
- **Resolution (Phase 5):** `PlayerSmokeBootstrap` counts completed scenarios (the package scenario adds its own); `run-unity-macos-il2cpp-player-smoke.sh` requires `>= 4` when `DATAVO_UPM_PACKAGE_SCENARIO=1`, which `verify-unity-upm-package.sh` sets (verified: candidate Player reports 3, UPM tarball Player reports 4).

---

### Subsystem: `docs/`

#### [DONE] `LE-46` — `configuration.md` Documents Removed `CheckpointStateFilePath` & Misplaces `DataVoAuthUser` Properties on `DataVoConfig`
- **Exact Location(s):**
  - `docs/manual/reference/configuration.md:32,42,74-75`
  - `DataVo.Core/StorageEngine/Config/DataVoConfig.cs:8-40,341`
- **What it is & Why it's there:** Line 32 documents `CheckpointStateFilePath` (deleted in v0.5.0); lines 74–75 list `Username`, `Password`, `Role`, `Roles` in the `DataVoConfig` table when they belong to `DataVoAuthUser` — causing CS0117 compile errors for users copying the docs! Also line 42 (and `DataVoConfig.cs:341`) describes `EnableZeroAllocCompiledUpdate` as LSM-only in-place byte patching, omitting v0.5.1's `InMemory` zero-dict compiled update path.
- **Proposed Action:** Remove `CheckpointStateFilePath`, split `DataVoAuthUser` into its own subsection/table, and update `EnableZeroAllocCompiledUpdate` docs in both `configuration.md:42` and `DataVoConfig.cs:341`.
- **Risk / Dependencies:** High user friction; zero risk to fix.
- **Effort Estimate:** `XS`
- **Resolution (Phase 6):** `configuration.md` drops `CheckpointStateFilePath`, moves `Username`/`Password`/`PasswordHash`/`PasswordSalt`/`Role`/`Roles` into a separate `DataVoAuthUser` table with a sample, and documents the `InMemory` zero-dict path of `EnableZeroAllocCompiledUpdate` (doc comment in `DataVoConfig.cs` too). `check-doc-api-samples.sh` now rejects `CheckpointStateFilePath`.

#### [DONE] `LE-47` — Factual API Errors in `ado-net.md` & `installation.md`
- **Exact Location(s):**
  - `docs/manual/client-interfaces/ado-net.md:29,47` (claims "three supported keys" above a 2-row table; claims `DataVoConnectionStringBuilder` inherits `DbConnectionStringBuilder` — false per `DataVoConnectionStringBuilder.cs:19`)
  - `docs/manual/getting-started/installation.md:3,28-35,39-41` (omits `DataVo.VectorData` from combinations & AOT list; falsely claims all packages ship `net8.0/net9.0/net10.0/netstandard2.1` when `VectorData` lacks `netstandard2.1`, `EntityFrameworkCore` is `net10.0`-only, and `Generators` is `netstandard2.0`; claims `DataVo.Generators` ships `.props` when it ships `.targets`)
- **What it is & Why it's there:** Docs drifted from actual project files and `DataVo.Data` class hierarchy.
- **Proposed Action:** Fix `ado-net.md` (or inherit `DbConnectionStringBuilder` per `LE-18`) and update `installation.md` with an accurate per-package TFM/AOT table and `.targets` correction.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `XS`
- **Resolution (Phase 6):** `ado-net.md` documents all three keys (`StorageMode`, `DataSource`, `Database`), `ChangeDatabase`, `DataVoException : DbException` and `DataVoFactory`; its `DbConnectionStringBuilder` claim is now true (LE-18). `installation.md` has a per-package TFM/AOT table, a VectorData combination row, and correctly says `.targets` (`build/` + `buildTransitive/`). Stale "no `DbProviderFactory`" claims also fixed in `roadmap.md`, `choosing-an-interface.md` and `limits.md`.

#### [DONE] `LE-48` — `DataVo.VectorData` Omitted from `public-manual.cjs` (AI/MCP Exports) & 5 Core Overview Pages
- **Exact Location(s):**
  - `docs/scripts/public-manual.cjs:18-26` (missing `manual/client-interfaces/vector-data.md` → absent from `llms.txt`, `llms-full.txt`, `ai/index.json`, `mcp/datavo-docs.json`, `mcp/remote-docs.json`, local/remote MCP servers, and `CopyAiMarkdown.vue` button!)
  - `docs/manual/reference/ai-access.md:14` (hardcodes "45 public manual and blog pages")
  - `docs/manual/getting-started/introduction.md:67-75`, `choosing-an-interface.md:8,12,14-20,31-32`, `roadmap.md:18-32`
  - `docs/manual/client-interfaces/native-aot.md:22-30`, `docs/index.md:34,40`
- **What it is & Why it's there:** `vector-data.md` was added to VitePress sidebar in v0.5.1 without adding it to `public-manual.cjs` or updating the overview tables.
- **Proposed Action:** Add `manual/client-interfaces/vector-data.md` to `public-manual.cjs`, run `npm run docs:ai`, update page count to 46 in `ai-access.md:14`, and add `DataVo.VectorData` to the 5 overview tables.
- **Risk / Dependencies:** Low; high discoverability win for users and AI agents.
- **Effort Estimate:** `XS`
- **Resolution (Phase 6):** `vector-data.md` (and the relocated locking note) are in `public-manual.cjs`: 47 exported pages; `ai-access.md` names the count. `DataVo.VectorData` added to `introduction.md` packages, `choosing-an-interface.md`, the `roadmap.md` subsystem table, the `native-aot.md` compatibility table and `index.md`.

#### [DONE] `LE-49` — Unimplemented Roadmap Features (`roadmap.md:61` Track A Native Interop Unlabeled) & Planned-Feature Drift
- **Exact Location(s):**
  - `docs/manual/getting-started/roadmap.md:40,60-61,67`
  - `docs/blog/index.md:20-26` (`### Planned` Parts 3–7)
  - `docs/research/feature-bibliography.md` (unscheduled exploratory features)
- **What it is & Why it's there:** Under `### v0.5 ... (released as 0.5.0; current release 0.5.1)`, **Track A (Native Interop)** — Native AOT C ABI (`dv_open`, `dv_exec`, `dv_prepare`), zero-copy Apache Arrow C Data Interface export, and reference Python binding — has **zero files/code anywhere in the repo** yet lacks a `*Planned:*` label (unlike Track B's `*Done:*`/`*Planned:*` bullets!). Other explicitly planned/unimplemented items tracked in docs: 2D WGS84 geographic spatial points (`roadmap.md:67`), `DataVo.Server` PG-wire bridge (`roadmap.md:40`), full EF Core provider/migrations, ADO.NET `DbProviderFactory`, Browser npm package, Unity Windows IL2CPP, and Blog Parts 3–7.
- **Proposed Action:** Immediately label Track A as `*Planned (post-0.5):*` or move it into a `Future / v0.6+` section so readers don't think a C ABI / Arrow export / Python binding shipped in 0.5.1.
- **Risk / Dependencies:** High expectation mismatch if left unlabeled; zero risk to fix.
- **Effort Estimate:** `XS`
- **Resolution (Phase 6):** Track A (C ABI, Arrow export, Python binding) moved out of the v0.5 milestone into a new **Future (post-0.5, unscheduled)** section with `*Planned:*` labels, alongside WGS84 points, `DataVo.Server`, the full EF provider, the browser npm package and Windows IL2CPP. Blog "Planned" parts were already labeled.

#### [DONE] `LE-50` — Internal Contradictions Across 7 Documentation Pages
- **Exact Location(s):**
  - `docs/manual/performance/datavo-vs-sqlite.md:8,37` vs `:102` (line 37 claims nullable keys require null-flag pass on LSM; line 102 says LSM indexes now hold NULL keys)
  - `docs/manual/sql/statements/create-table.md:34,104` (omits `DATETIME`; claims scalar indexes omit NULL keys, contradicting `indexes.md:59`)
  - `docs/manual/sql/statements/indexes.md:43` vs `:112` (line 43 omits `DATETIME` from `COLUMNAR` types; line 112 includes it)
  - `docs/manual/sql/functions.md:33-34,64,170-173` (omits `DATETIME` from `MIN`/`MAX` and comparison tables, contradicting `data-types.md:57`)
  - `docs/manual/internals/compiled-scan-kernels.md:25,42` (omits `DateTime`/`DateTimeOffset`, though `DataVoLinqAnalyzer.cs:219-220` & `DataVoTableInspector.cs:300-301` support both)
  - `docs/blog/part-1-where-it-wins-and-where-it-loses.md:89,319` vs `:60` (still claims `EXPLAIN` operator reporting is "planned for 0.5.0" and Windows lock issue is deferred to Part 3 — both already shipped in v0.5!)
  - `README.md:56` & `choosing-an-interface.md:16` (`55 ns`/`216 ns` from 21 Nov) vs `benchmarks.md:18` (`44.1 ns`/`218.0 ns` from 25 Nov v0.5.0)
- **What it is & Why it's there:** Partial v0.5 edits updated some pages/sections while leaving adjacent tables or cross-references on v0.4 facts.
- **Proposed Action:** Reconcile all 7 pages with v0.5.1 implementation and latest benchmark numbers.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `S`
- **Resolution (Phase 6):** `datavo-vs-sqlite.md` NULL-key rule matches the v0.5.0 note; `create-table.md` lists `DATETIME` and no longer says indexes omit NULL keys; `indexes.md` COLUMNAR types include `DATETIME`; `functions.md` MIN/MAX and comparison tables include `DATETIME`; `compiled-scan-kernels.md` covers `DateTime`/`DateTimeOffset` and the v0.5.0 timings; Part 1 has update notes (EXPLAIN operator reporting since 0.5.0, writer-stall fixes); `README.md` and `choosing-an-interface.md` use the v0.5.0 `44.1 ns`/`218.0 ns` and `27.3–91.1 µs` figures.

#### [DONE] `LE-51` — `generate-ci-benchmark-plots.py` Fails on Fresh Clones Because `.log` Inputs Are Gitignored
- **Exact Location(s):**
  - `docs/scripts/generate-ci-benchmark-plots.py:4-5,94-95`
  - `docs/manual/performance/reproducing-benchmarks.md:111`
  - `.gitignore:135` (`*.log` ignores all 26 `docs/public/benchmarks/**/*.log` files!)
  - `README.md:75` (refers to "raw logs" in `2026-11-16-v04-final/`, though only `.md`/`.txt` are tracked)
- **What it is & Why it's there:** Global `*.log` ignore rule excluded `docs/public/benchmarks/2026-11-12/ci/{linux,windows}-x64/benchmark.log` and `2026-11-16-v04-final/*.log` from git, breaking `npm run benchmarks:ci-plots` on any clean checkout.
- **Proposed Action:** Either add `!docs/public/benchmarks/**/*.log` (or rename the CI input logs to `.txt`) and commit the required CI logs, and align `README.md:75` wording.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `XS`
- **Resolution (Phase 6):** `.gitignore` re-includes `docs/public/benchmarks/**/*.log`; all 25 benchmark logs are committed. `generate-ci-benchmark-plots.py` verified on the committed inputs; README wording now says "raw logs and reports".

#### [DONE] `LE-52` — Local vs. Remote MCP Server Drift, Wrong `docs/package.json` License, & Stray Local Files in `docs/`
- **Exact Location(s):**
  - `docs/mcp/datavo-docs-server.cjs` vs `docs/mcp-worker/worker.js:161,197-198` (local server lacks `ping` RPC handler and doesn't match `page.title` in `search_docs`)
  - `docs/package.json:23` (`"license": "ISC"` instead of `"MIT"`)
  - Local untracked/gitignored stray files on disk: `docs/Parser.cs.tmp` (58 KB Mar 2026 scratch copy), `docs/test-results/`, `docs/tests/browser/generated/*.json` (15 stale Mar 2026 files), `docs/.DS_Store`, `docs/public/.DS_Store`
- **What it is & Why it's there:** Local MCP server fell slightly behind the Cloudflare Worker implementation; `npm init` left `"ISC"` license; scratch/test artifacts accumulated locally.
- **Proposed Action:** Port `ping` and title-matching to `datavo-docs-server.cjs`, change `docs/package.json` license to `"MIT"`, and clean up local stray files (`docs/Parser.cs.tmp`, etc.).
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`
- **Resolution (Phase 6):** Local MCP server gained `ping` and title matching in `search_docs` (smoke-tested over stdio); `docs/package.json`/lockfile license is `MIT`; local strays (`Parser.cs.tmp`, `test-results/`, stale March `generated/*.json`, `.DS_Store`) deleted.

---

### Subsystem: `CI/scripts` *(incl. Tests, Tools, Demos, Examples)*

#### [DONE] `LE-53` — Uncommitted `VectorData` Path Fix Needed for `HEAD` CI & Multiple Solution/Repo Projects Never Built in `ci.yml`
- **Exact Location(s):**
  - Uncommitted local working-tree fixes (7 files fixing `src/DataVo.VectorData` → `DataVo.VectorData`): `.github/workflows/ci.yml:81,141`, `.github/workflows/publish-nuget.yml:52`, `scripts/check-aot-baseline.sh:18`, `DataVo.sln:44-48,274-278`, `DataVo.VectorData/DataVo.VectorData.csproj:15`, `DataVo.VectorData.Tests/DataVo.VectorData.Tests.csproj:29`, `examples/MicrosoftAiLocalRag/MicrosoftAiLocalRag.csproj:21`
  - Unbuilt in `.github/workflows/ci.yml`: `examples/HybridVectorRagAot` (in sln, AOT!), `examples/LiveReactiveLeaderboard` (in sln), `benchmarks/DataVo.Benchmarks` (in sln), `DataVo.Browser` (in sln), `tools/{DataVo.LinqBenchmark,DataVo.PortableConsumer,DataVo.PortablePackageSmoke,DataVo.UnityCandidateInspector}`, `demos/realtime-benchmarks/src/*`, `demos/Research.Benchmark/tools/VectorRecall`
- **What it is & Why it's there:** `HEAD` currently has broken `src/DataVo.VectorData` paths until the 7 modified files are committed. Meanwhile, `ci.yml` never builds `HybridVectorRagAot`, `LiveReactiveLeaderboard`, `DataVo.Benchmarks`, or `tools/*` — which directly allowed `BC-38` (`DataVo.PortablePackageSmoke` compile break) and `BC-39` (`prepare-unity-candidate.sh` breakage) to slip in unnoticed!
- **Proposed Action:** Commit the 7 `DataVo.VectorData` path fixes; add build/smoke checks in `ci.yml` for `HybridVectorRagAot` (incl. `dotnet publish` in `aot` job), `LiveReactiveLeaderboard`, `DataVo.Benchmarks`, `DataVo.PortablePackageSmoke`/`DataVo.PortableConsumer`/`DataVo.UnityCandidateInspector`, and decide whether to fold or archive `tools/DataVo.LinqBenchmark` and `demos/realtime-benchmarks/`.
- **Risk / Dependencies:** Low; huge regression-prevention payoff.
- **Effort Estimate:** `S`

#### [DONE] `LE-54` — Release & Aux Workflow Gaps (`publish-nuget.yml`, `randomized-tests.yml`, `benchmark.yml`, `deploy-docs.yml`)
- **Exact Location(s):**
  - `.github/workflows/publish-nuget.yml:17,20,61-65` (uses `@v4` actions vs `@v5` in `ci.yml`; validates package target layout ONLY for `DataVo.Core`, skipping `DataVo.Data`, `DataVo.VectorData`, `DataVo.EntityFrameworkCore`, `DataVo.Generators`; runs no AOT or smoke test before pushing to NuGet.org)
  - `.github/workflows/deploy-docs.yml:26` (`@v4` checkout)
  - `.github/workflows/randomized-tests.yml:6` (comment says `SEEDS=1`, but tests read `DATAVO_MODEL_SEEDS` / `DATAVO_DIFF_SEEDS`)
  - `.github/workflows/benchmark.yml:105-116` (`"all"` lists 10 scenarios, missing 4 supported by `Research.Benchmark.Host/Program.cs:341`: `arena-live-views`, `sql-workload`, `sql-matrix`, `hybrid-vector`)
- **What it is & Why it's there:** Incremental additions drifted out of sync across the 5 workflow files.
- **Proposed Action:** Upgrade actions to `@v5`, verify all 5 `.nupkg` layouts + run `check-aot-baseline.sh` in `publish-nuget.yml`, fix `randomized-tests.yml:6` comment, and sync `benchmark.yml`'s `"all"` scenario list.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `S`

#### [DONE] `LE-55` — AOT Baseline Fence & Doc-Sample Guard Gaps
- **Exact Location(s):**
  - `DataVo.AotSmoke/DataVo.AotSmoke.csproj:17,20-24` & `Program.cs:79-115` (omits `DataVo.VectorData`, `[DataVoQuery]`, parameterized ADO.NET + transactions/savepoints + LSM ADO.NET, and `WarningsAsErrors` for `IL2*`/`IL3*`)
  - `DataVo.Core/DataVo.Core.csproj:15` vs `DataVo.Data.csproj:17` vs `DataVo.VectorData.csproj:11` (`WarningsAsErrors` IL-code lists drifted: Core missing `IL2060;IL2067;IL2077;IL2095`; Data missing `IL2060;IL2077;IL2095`)
  - `scripts/check-aot-baseline.sh:8,21` (stale comment says `"both library projects"` when there are 3; redirects stderr/stdout to `/dev/null`, hiding which IL warning broke CI; `docs/manual/client-interfaces/native-aot.md:40` falsely claims it publishes `DataVo.AotSmoke`)
  - `scripts/check-doc-api-samples.sh:1-23` (orphaned — never called in CI; only scans `README.md` + `docs/features`, missing `docs/manual/`; doesn't guard against removed `StorageMode.Disk`/`DiskStorageEngine`/`UseDiskStorage`)
- **What it is & Why it's there:** Each package/script evolved independently without centralizing AOT error codes or wiring doc guards into `deploy-docs.yml`.
- **Proposed Action:** Centralize AOT `WarningsAsErrors` in `Directory.Build.props`, expand `DataVo.AotSmoke`, print build errors on failure in `check-aot-baseline.sh`, and modernize + wire `check-doc-api-samples.sh` into `deploy-docs.yml`.
- **Risk / Dependencies:** Low (check that Core/Data pass the 4 extra `IL*` codes cleanly).
- **Effort Estimate:** `S`

#### [DONE] `LE-56` — Script Bugs, Duplication & 15 Orphaned Scripts in `scripts/`
- **Exact Location(s):**
  - `scripts/test-unity-mathematics-package.sh:4-6` (**bug:** default `mode="${1:-all}"` fails `case` expecting `--all`, exiting with code 64 when run with no args!)
  - `scripts/profile-wasm-vector-mode.sh:7-24` (**bug & orphaned:** validates `$MODE` then never passes it to Playwright)
  - `scripts/stress-semantic-inventory.sh:1-16` (**stub & orphaned:** doesn't run a stress test and prints port `5000` while `start-semantic-inventory.sh:11` uses `5088`)
  - `scripts/generate-wasm-browser-scenarios.cjs:17-18,141-159` vs `scripts/wasm-scenarios/policy.cjs:1-22,31-36` (duplicates `ATTR_IGNORE`, `ATTR_NEEDS_SPECIFIC`, `parseReasonFromAttribute`)
  - `scripts/verify-local-packages.sh:21-30,102-115` (packs & smoke-tests only 2 of 5 packages: `Core` and `Data`, omitting `EntityFrameworkCore`, `Generators`, `VectorData`)
  - `scripts/xunit-serial.runsettings` (unreferenced)
  - 6 standalone Unity contract tests unreferenced by any runner (`test-check-unity-runtime-bcl.sh`, `test-prepare-unity-candidate.sh`, `test-resolve-unity-candidate-assets.mjs`, `test-unity-candidate-inspector.sh`, `test-unity-macos-il2cpp-player-smoke.sh`, `test-unity-mathematics-package.sh`) + 9 Unity `.sh` scripts lacking `+x`
- **What it is & Why it's there:** Accumulated scripts without a CI script-health check.
- **Proposed Action:** Fix `test-unity-mathematics-package.sh:4` (`--all`), deduplicate `generate-wasm-browser-scenarios.cjs` against `policy.cjs`, expand `verify-local-packages.sh` to all 5 packages, and delete `profile-wasm-vector-mode.sh`, `stress-semantic-inventory.sh`, and `xunit-serial.runsettings`.
- **Risk / Dependencies:** Low.
- **Effort Estimate:** `S`

#### [DONE] `LE-57` — Silently Passing (Early-Return) Opt-In Tests in `HNSWIndexTests` & `VectorSearchEngineTests`
- **Exact Location(s):**
  - `DataVo.Tests/Indexing/HNSWIndexTests.cs:356-362,421-427,450-456,491-497,536-542,623-629,730-736,793-799,898-904,955-961` (10 `[Fact]` perf tests that `return;` early unless `DATAVO_RUN_HNSW_PERF_TESTS=1`)
  - `demos/Research.Benchmark/tests/Research.Benchmark.Tests/VectorSearchEngineTests.cs:68-84` (catches `InvalidOperationException` and `return;`s when `sqlite-vec` is absent — always silently passes in `ci.yml`)
- **What it is & Why it's there:** Intentional opt-in/environment guards implemented via plain `return;`, causing xUnit to report **11 unexecuted tests as `Passed`** rather than `Skipped`.
- **Proposed Action:** Use a custom `[Fact]` attribute (or `Skip` property / `Xunit.SkippableFact`) so TRX reports honestly mark them `Skipped` when their precondition is unset, or move the 10 HNSW perf tests into `benchmarks/`.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`
- **Resolution (Phase 2A):** Added `[HnswPerformanceFact]` (skips unless `DATAVO_RUN_HNSW_PERF_TESTS=1`) and `[SqliteVecFact]` (skips unless `SQLITE_VEC_PATH` exists), so the 11 tests report `Skipped` instead of `Passed`.

#### [DONE] `LE-58` — Stubbed Benchmark Runners (`NotImplementedException`), Unused Example Code & Broken Spec Links
- **Exact Location(s):**
  - `demos/Research.Benchmark/src/Research.Benchmark.Runners/Postgres/PostgresEngine.cs:11-24` (all 5 `IBettingRiskEngine` methods throw `NotImplementedException`; 0 callers)
  - `demos/Research.Benchmark/src/Research.Benchmark.Runners/RedisStream/RedisStreamEngine.cs:11-24` (all 5 methods throw `NotImplementedException`; 0 callers; paired with `demos/Research.Benchmark/docker-compose.yml`)
  - `examples/LiveReactiveLeaderboard/Program.cs:62` (unused local array `var top = new (int Id, string Name, int Score)[10];`)
  - `demos/Research.Benchmark/FAIRPLAY.md:5` (broken link to gitignored `../../docs/superpowers/specs/2026-06-23-fair-play-benchmark-suite-design.md`)
  - `demos/realtime-benchmarks/browser-wasm-showcase/README.md` (planning doc specifying unimplemented `DataVo.Browser` JS exports `SubscribeSql`/`DispatchPendingNotifications`/`DrainReactiveChanges`/`Unsubscribe`)
- **What it is & Why it's there:** Abandoned benchmark scaffolds (`Postgres`/`RedisStream`), minor dead variable in an example, and a planning README for unimplemented browser reactive exports.
- **Proposed Action:** Delete `PostgresEngine.cs`, `RedisStreamEngine.cs`, and unused `docker-compose.yml` (if not pursuing client-server comparisons); delete unused `top` array in `LiveReactiveLeaderboard/Program.cs:62`; fix `FAIRPLAY.md:5` link; archive or remove `browser-wasm-showcase/`.
- **Risk / Dependencies:** None (0 callers).
- **Effort Estimate:** `XS`

#### [DONE] `LE-59` — Tracked Stray Files, Personal Paths, & Build-Artifact Bloat Committed in Git
- **Exact Location(s):**
  - `DataVo.Core/.local/share/NuGet/Migrations/1` (0-byte tracked NuGet CLI migration file)
  - `DataVo.Data/.local/share/NuGet/Migrations/1` (0-byte tracked NuGet CLI migration file)
  - `.vscode/mcp.json:5-7` (tracked file hardcoding author's personal paths `/Users/arintonakos/Projects/AI-Utils/ef-core-mcp/...`)
  - `demos/Research.Benchmark/src/Research.Benchmark.Host/latest-run.txt` (780-line tracked raw zsh terminal dump with personal prompt & Hungarian console text)
  - `examples/semantic-inventory/frontend/dist/*` (169 KB built Vite bundle), `tsconfig.tsbuildinfo`, and 6 transpiled `.js` duplicates next to `.tsx`/`.ts` files (plus the 22.6 MB `semantic-inventory.db` noted in `BC-45`)
- **What it is & Why it's there:** Accidental commits of local tool/build state and developer terminal logs.
- **Proposed Action:** `git rm` all listed files and add `.local/`, `.vscode/mcp.json`, `*.tsbuildinfo`, and `dist/` to `.gitignore`.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`

#### [DONE] `LE-60` — `.vscode/` & Root Git Config Loose Ends (`launch.json`, `tasks.json`, `.gitignore`, `.gitattributes`)
- **Exact Location(s):**
  - `.vscode/launch.json:1-4` (empty `"configurations": []` stub)
  - `.vscode/tasks.json:17-27` (`pack` runs `dotnet pack DataVo.sln -c Release` which fails without `wasm-tools` on `DataVo.Browser`, and individual pack tasks only cover `Core` and `Data` — missing `EntityFrameworkCore`, `Generators`, `VectorData`)
  - `.gitignore:32-41,440-443` (duplicate `output/` lines making `:32-41` redundant; personal `supabase_answers.txt` entry)
  - `.gitattributes:7-8` (sets `whitespace=-trailing-space` for `UnitySmoke` `.asset`/`.meta` files, omitting `demos/unity/DataVo.UnityArenaDemo/`)
- **What it is & Why it's there:** Stale editor tasks and minor config duplication.
- **Proposed Action:** Fix `.vscode/tasks.json` to pack the 5 publishable projects individually (matching `publish-nuget.yml`), remove or populate `.vscode/launch.json`, deduplicate `.gitignore`, and add `UnityArenaDemo` to `.gitattributes`.
- **Risk / Dependencies:** None.
- **Effort Estimate:** `XS`

---

## 4. Recommended Prioritization Waves (For Planning)

### Wave 1 — Broken Builds, Silent Correctness/Security Bugs, & Ship-Blocking Doc Errors (`XS`–`S`, High ROI)
1. **Broken builds & packaging:** Commit 7 pending `DataVo.VectorData` path fixes (`LE-53`); fix `tools/DataVo.PortablePackageSmoke/Program.cs:15` (`BC-38`), `scripts/prepare-unity-candidate.sh:88` (`BC-39`), `scripts/test-unity-mathematics-package.sh:4` (`LE-56`), and gitignored `Samples~/QuickStart` + `.meta` files (`LE-41`, `LE-42`).
2. **Core correctness & security bugs:** Fix `BTreeIndexFactory` `minDegree = 3` → `50` (`LE-02`), `UnionSelect` missing `HasErrors = true` (`LE-03`), `SessionSecurityStore` admin privilege escalation & missing `EnsureInitialized` (`LE-04`), `ReactiveRegistry._incomingBuffer` memory leak (`LE-09`), `UpdateFixedWidthByPrimaryKeyBatch` missing tx/lock checks (`LE-08`), `TableColumnService` missing `DATETIME` & `GroupBy` hash keys (`LE-10`), `EngineCatalog.cs:229` raw `0x00` NUL byte (`LE-15`), and Volcano join `NULL = NULL` (`LE-06`).
3. **Provider bugs:** Fix `DataVo.VectorData` `CreateVector` `EmbeddingGenerator` wiring (`LE-29`) + add `DataVo.VectorData` to `DataVo.AotSmoke` (`LE-33`); fix `DataVo.Generators` `IsVector` handling in `InspectOrder`/`LinqUpdateTranslator` (`LE-36`); fix `DataVo.EntityFrameworkCore` `UseInMemoryStorage()` (`LE-24`) & non-transactional `SaveChanges` (`LE-25`); fix `DataVo.Data` `ConnectionString` setter, `IsClosed`, `NextResult`, & `DataVoTransaction.Dispose` (`LE-17`, `LE-20`, `LE-22`).
4. **Ship-blocking doc inaccuracies:** Fix `configuration.md` (`LE-46`), `ado-net.md` + `installation.md` (`LE-47`), `roadmap.md:61` Track A label (`LE-49`), `README.md` + `CLAUDE.md` (`BC-31`, `BC-32`), and add `vector-data.md` to `public-manual.cjs` + bump `generate-ai-docs.cjs` to `v0.5` (`LE-48`, `BC-34`).

### Wave 2 — Zero-BC Legacy Removal & Repo Hygiene (`XS`–`M`)
- Delete dead `DataVo.Core` remnants: `Catalog.cs` (`BC-05`), `CacheStorage.cs` (`BC-06`), `BloomFilter.cs` (`BC-03`), `TaskCancellationWaiter.*.cs` (`BC-07`), dead `abkr` compiler island + `TopologicalSort` + `StringExtensions` (`LE-14`), dead vector `ExportEntries`/`ImportEntries` (`BC-12`), reflection `File.Replace` (`BC-13`), dead `LockManager`/`TransactionManager`/`MVCC`/`FileHandlePool`/`RowKeyConversions` shims (`BC-14`, `LE-13`), pre-AST `AggregateModel` string parser (`BC-15`).
- Delete legacy on-disk formats (zero-BC): v2/v4 row-keyed SSTables (`BC-01`), v3 keyed SSTables (`BC-02`), dead per-table LSM WAL (`BC-04`), and dead test `TestSsTableWriter` (`BC-43`).
- Clean up repo bloat & obsolete trees: delete 22 superseded `docs/features/` files after relocating `insert-intent-row-locking.md` (`BC-30`), delete `examples/semantic-inventory/` (22.6 MB SQLite DB + build artifacts, `BC-45`), delete tracked `.local/`/`.vscode/mcp.json`/`latest-run.txt` (`LE-59`), delete obsolete scripts (`BC-40`, `BC-41`, `BC-42`, `BC-47`), and decide on `docs/public/datavo-wasm/` (17 MB) + `legacy-redirects.ts` (`BC-33`, `BC-36`).

### Wave 3 — Larger Architectural Follow-Ups (`M`–`L`)
- Remove `VipExposureReactiveQuery` hack & finish zero-alloc IVM (`LE-01`, `LE-09`), fix `ALTER TABLE`/`VACUUM` vector & columnar index rebuild (`LE-05`), parameterize EF Core & fix `DATETIME` mapping (`LE-26`, `LE-27`, `LE-28`), modernize `DataVo.Browser` (STJ + SAB reuse + base-path fix, `BC-26`, `LE-38`–`LE-40`), refactor `DataVoQueryGenerator` incremental pipeline (`LE-35`), eliminate `DataVoEngine.Current()` singleton (`BC-08`), and retire `Legacy*` dictionary SQL execution once Volcano covers outer joins/`HAVING`/subqueries (`BC-09`).
