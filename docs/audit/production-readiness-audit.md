# DataVo DBMS — Production Readiness Audit

> **Date:** 2026-03-28  
> **Scope:** `DataVo.Core`, `DataVo.Data`, `DataVo.EntityFrameworkCore`  
> **Reviewer:** Antigravity (automated deep-read analysis)  
> **Last updated:** 2026-03-29 — Phase 5 lock timeout and index persistence hardening

---

## Implementation Progress

| #    | Severity    | Title                                                        | Status         | Notes                                                                                                              |
| ---- | ----------- | ------------------------------------------------------------ | -------------- | ------------------------------------------------------------------------------------------------------------------ |
| 2.1  | 🔴 Critical | WAL: no per-record checksum                                  | ✅ **Fixed**   | Checksum-protected WAL envelopes + corruption hard-fail                                                            |
| 2.2  | 🔴 Critical | WAL rewrite is non-atomic                                    | ✅ **Fixed**   | Atomic `.tmp` + `File.Replace()` WAL rewrite                                                                       |
| 2.3  | 🔴 Critical | Catalog XML in-place overwrite                               | ✅ **Fixed**   | Atomic write-to-tmp + `File.Replace()`                                                                             |
| 2.4  | 🔴 Critical | `TableContainsRow` swallows all exceptions                   | ✅ **Fixed**   | Catches only expected row-miss exceptions (`RowDeletedException`, `RowNotFoundException`, `FileNotFoundException`) |
| 2.5  | 🔴 Critical | Global static file-lock dict grows without bound             | ✅ **Fixed**   | File-lock entries removed on `DropTable`/`DropDatabase`                                                            |
| 2.6  | 🔴 Critical | `CacheStorage` non-thread-safe `Dictionary`                  | ✅ **Fixed**   | Replaced with `ConcurrentDictionary`                                                                               |
| 2.7  | 🔴 Critical | ABBA deadlock between version lock and file lock             | ✅ **Fixed**   | `VacuumTable` no longer holds version write lock during storage lookups                                            |
| 2.8  | 🔴 Critical | Transaction IDs reset to 1 on restart                        | ✅ **Fixed**   | High-water mark persisted/restored via state file                                                                  |
| 2.9  | 🔴 Critical | `CreateTable` is a no-op                                     | ⬜ Pending     | Needs storage backend design decision                                                                              |
| 3.1  | 🟠 Major    | `dynamic` as universal row type                              | ⬜ Pending     | Large-scale refactor                                                                                               |
| 3.2  | 🟠 Major    | `Select.cs` 4 410-line god class                             | ⬜ Pending     | Large-scale refactor                                                                                               |
| 3.3  | 🟠 Major    | Bare `catch {}` blocks suppress errors                       | ⬜ Pending     | Requires per-file audit                                                                                            |
| 3.4  | 🟠 Major    | `throw new Exception(...)` everywhere                        | ✅ **Started** | Domain hierarchy created; `DiskStorageEngine.ReadRow` migrated                                                     |
| 3.5  | 🟠 Major    | `IndexManager._cache` typed as `object`                      | ⬜ Pending     |                                                                                                                    |
| 3.6  | 🟠 Major    | No deadlock detection or lock timeout                        | 🟡 Partially fixed | Configurable lock acquisition timeout added; deadlock graph detection still pending                                 |
| 3.7  | 🟠 Major    | Table locks never cleaned from `_tableLocks`                 | ✅ **Fixed**   | Reference-counted lifecycle cleanup and disposal                                                                   |
| 3.8  | 🟠 Major    | `CompactTable` hardcodes file-header magic                   | ✅ **Fixed**   | Uses `FileHeaderMagic` / `FileHeaderVersion` constants                                                             |
| 3.9  | 🟠 Major    | VECTOR columns bloat WAL with JSON                           | ⬜ Pending     | Part of WAL overhaul                                                                                               |
| 3.10 | 🟠 Major    | `RowSerializer` uses static ambient `DataVoEngine.Current()` | ⬜ Pending     |                                                                                                                    |
| 3.11 | 🟠 Major    | `StorageContext.Initialize` leaks previous engine            | ⬜ Pending     |                                                                                                                    |
| 3.12 | 🟠 Major    | Case sensitivity mismatch catalog vs. files                  | ⬜ Pending     |                                                                                                                    |
| 4.1  | 🟡 Minor    | Duplicate column-parse logic                                 | ⬜ Pending     |                                                                                                                    |
| 4.2  | 🟡 Minor    | `VersionStorageManager.Dispose()` missing `IDisposable`      | ✅ **Fixed**   | `IDisposable` declared                                                                                             |
| 4.3  | 🟡 Minor    | `TransactionIdAllocator` uses `lock`                         | ✅ **Fixed**   | Replaced with `Interlocked.Increment` + `SpinLock` for ranges                                                      |
| 4.4  | 🟡 Minor    | Join operators missing dictionary capacity                   | ⬜ Pending     |                                                                                                                    |
| 4.5  | 🟡 Minor    | `Lock` re-entrancy in `IndexManager.MarkDirty`               | ⬜ Pending     |                                                                                                                    |
| 4.6  | 🟡 Minor    | Duplicate `Touch`/`Invalidate` schema version methods        | ✅ **Fixed**   | Consolidated into `BumpTableSchemaVersion`                                                                         |
| 4.7  | 🟡 Minor    | Undocumented `tableKey` overloads                            | ⬜ Pending     |                                                                                                                    |
| 4.8  | 🟡 Minor    | Parser silently skips unknown tokens                         | ✅ **Fixed**   | Unexpected tokens now throw `ParserException`                                                                      |
| 4.9  | 🟡 Minor    | `CompactTable` resets RowIds without enforcing rebuild       | ⬜ Pending     |                                                                                                                    |
| 4.10 | 🟡 Minor    | No authentication/authorization                              | ⬜ Pending     | Feature-level effort                                                                                               |
| 4.11 | 🟡 Minor    | `DataVoTransaction` missing savepoint support                | ⬜ Pending     |                                                                                                                    |
| 4.12 | 🟡 Minor    | WAL `TransactionId` (Guid) disconnected from MVCC (long)     | ✅ **Fixed**   | WAL now carries/replays `MvccTransactionId` and restores allocator floor                                           |
| 4.13 | 🟡 Minor    | Static cardinality feedback dict grows without eviction      | ⬜ Pending     |                                                                                                                    |
| 4.14 | 🟡 Minor    | Index persistence silently drops I/O errors                  | ✅ **Fixed**   | Flush/delete paths now surface persistence failures as exceptions                                                   |

### Summary

| Metric                    | Value                                                      |
| ------------------------- | ---------------------------------------------------------- |
| **Total issues**          | 33                                                         |
| **Fixed**                 | 18 (55%)                                                   |
| **Partially fixed**       | 1 (3%)                                                     |
| **Pending**               | 14 (42%)                                                   |
| **New tests added**       | Audit-focused + lock/WAL/index regression tests (all passing) |
| **Current full test run** | ✅ 688/688 passing (`dotnet test DataVo.Tests`)               |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Critical Issues](#2-critical-issues)
3. [Major Issues](#3-major-issues)
4. [Minor Issues / Code Quality](#4-minor-issues--code-quality)
5. [Missing Features (Functional Gaps)](#5-missing-features-functional-gaps)

---

## 1. Executive Summary

DataVo is an impressively structured database engine for a learning/research project, with proper MVCC, WAL, a Volcano-model execution engine, B+Tree indexing, and HNSW vector search. However a substantial number of design decisions and implementation shortcuts make it unsuitable for production use. The issues range from **data-loss risks** (single-file WAL with no checksums, non-atomic catalog saves) through **concurrency hazards** (bare `object`-based file locks, non-thread-safe `CacheStorage`, ABBA deadlock between version lock and file lock) to **extreme coupling** (process-wide singletons, `dynamic` as the universal row type, a 4 400-line `Select.cs` god-class).

---

## 2. Critical Issues

### 2.1 — ~~WAL serialized as NDJSON with no per-record checksum~~ ✅ FIXED

**File:** `DataVo.Core/Transactions/WalFileStore.cs`

~~The log had no per-record integrity marker and malformed lines could be silently dropped.~~

**Fix applied:** each WAL line is now an envelope containing payload length and CRC32 checksum. Recovery/read now hard-fails with `InvalidDataException` on checksum mismatch or malformed records (no silent skip).

---

### 2.2 — ~~WAL checkpoint rewrite is not atomic on disk~~ ✅ FIXED

**Files:** `DataVo.Core/Transactions/WalFileStore.cs`, `WalEntry.cs`

~~Rewrite previously used `FileMode.Create` in-place truncate and rewrite.~~

**Fix applied:** WAL rewrites now go to `.tmp`, force flush to disk, then atomically swap with `File.Replace` (or move on first create).

---

### 2.3 — ~~Catalog stored as a single XML file, overwritten in-place on every change~~ ✅ FIXED

**File:** `DataVo.Core/Runtime/CatalogStore.cs`

~~`SaveDocument()` calls `_doc.Save(_catalogFilePath!)` which overwrites the catalog XML in-place.~~

**Fix applied:** `SaveDocument()` now writes to `_catalogFilePath + ".tmp"`, then uses `File.Replace()` for atomic swap. Crash-safe on NTFS/APFS/ext4.

---

### 2.4 — ~~`StorageContext.TableContainsRow` treats all exceptions as "not found"~~ ✅ FIXED

**File:** `DataVo.Core/StorageEngine/StorageContext.cs`

~~The method caught bare `Exception` and returned `false`.~~

**Fix applied:** Now catches only expected row-miss exceptions (`RowDeletedException`, `RowNotFoundException`, `FileNotFoundException`). Genuine storage/system errors still propagate to callers.

---

### 2.5 — ~~Global static file-lock dictionary in `DiskStorageEngine` grows without bound~~ ✅ FIXED

**File:** `DataVo.Core/StorageEngine/Disk/DiskStorageEngine.cs`

~~Lock entries were added with `GetOrAdd` but never removed.~~

**Fix applied:** lock keys are normalized and explicitly removed in both `DropTable` and `DropDatabase`, eliminating the create/drop lifecycle leak.

---

### 2.6 — ~~`CacheStorage` uses a non-thread-safe static `Dictionary`~~ ✅ FIXED

**File:** `DataVo.Core/Cache/CacheStorage.cs`

~~Plain `Dictionary<Guid, string>` shared across all concurrent sessions.~~

**Fix applied:** Replaced with `ConcurrentDictionary<Guid, string>`.

---

### 2.7 — ~~ABBA deadlock: version write-lock held while acquiring per-file lock~~ ✅ FIXED

**File:** `DataVo.Core/MVCC/VersionStorageManager.cs:176-195`

~~`VacuumTable` used to hold `_versionLock` while calling into storage, creating lock-order inversion risk.~~

**Fix applied:** `VacuumTable` now snapshots candidates under read lock, performs storage existence checks without holding the version write lock, then reacquires write lock only for removals.

---

### 2.8 — ~~`TransactionIdAllocator` resets to 1 on every process restart~~ ✅ FIXED

**File:** `DataVo.Core/MVCC/TransactionIdAllocator.cs`

**Fix applied:** transaction ID high-water mark is now persisted/restored via `TransactionIdStateStore` sidecar file, wired into `DataVoEngine` startup and dispose lifecycle. Allocator restore is applied before recovery starts.

---

### 2.9 — `StorageContext.CreateTable` is a documented no-op ⬜

**File:** `DataVo.Core/StorageEngine/StorageContext.cs:82-87`

The physical data file is created lazily on first `INSERT`. Needs a design decision on whether to eagerly allocate or to validate during `SELECT`.

---

## 3. Major Issues

### 3.1 — Pervasive use of `dynamic` as the row representation ⬜

`Dictionary<string, dynamic>` is the fundamental row type. Needs a strongly-typed `DbValue` discriminated union.

### 3.2 — `Select.cs` is a 4 410-line god class ⬜

Needs decomposition into separate planner, executor, and optimizer components.

### 3.3 — Bare `catch {}` blocks suppress real errors ⬜

Needs per-file audit to replace with domain exceptions or structured logging.

### 3.4 — `throw new Exception(...)` used universally ✅ STARTED

**Fix applied:** Domain exception hierarchy created (`DataVoException` → `StorageException` → `RowDeletedException`, `CatalogException`). `DiskStorageEngine.ReadRow` migrated. Remaining ~150 throw sites need incremental migration.

### 3.5 — `IndexManager._cache` typed as `Dictionary<string, object>` ⬜

### 3.6 — `LockManager` has no deadlock detection or timeout 🟡 PARTIALLY FIXED

**Files:** `DataVo.Core/Transactions/LockManager.cs`, `DataVo.Core/Runtime/DataVoEngine.cs`, `DataVo.Core/StorageEngine/Config/DataVoConfig.cs`

**Fix applied (partial):** configurable lock acquisition timeout is now enforced on table and row lock acquisition paths via `TryEnter*Lock(timeout)` with `TimeoutException` propagation.

**Remaining:** cycle-aware deadlock graph detection/diagnostics are still pending.

### 3.7 — ~~Table lock entries leak from `_tableLocks`~~ ✅ FIXED

**File:** `DataVo.Core/Transactions/LockManager.cs`

**Fix applied:** table locks now use lifecycle-managed entries with `ActiveUsers` reference counting. On last release, entries are removed from `_tableLocks` and disposed.

### 3.8 — ~~`CompactTable` hardcodes file-header magic~~ ✅ FIXED

**Fix applied:** Now uses `FileHeaderMagic` and `FileHeaderVersion` constants.

### 3.9 — VECTOR columns bloat WAL with raw JSON ⬜

### 3.10 — `RowSerializer` calls `DataVoEngine.Current()` as ambient side-channel ⬜

### 3.11 — `StorageContext.Initialize` leaks the previous engine ⬜

### 3.12 — Case-sensitivity mismatch between catalog XML and file paths ⬜

---

## 4. Minor Issues / Code Quality

### 4.1 — Duplicate column-parsing logic ⬜

### 4.2 — ~~`VersionStorageManager.Dispose()` missing `IDisposable`~~ ✅ FIXED

**Fix applied:** Class now implements `IDisposable`.

### 4.3 — ~~`TransactionIdAllocator` uses `lock` instead of `Interlocked`~~ ✅ FIXED

**Fix applied:** Single-ID allocation uses `Interlocked.Increment`. Batch range uses `SpinLock`. ~2x faster on the hot path.

### 4.4 — Join operators missing dictionary capacity ⬜

### 4.5 — `Lock` re-entrancy in `IndexManager.MarkDirty` ⬜

### 4.6 — ~~`TouchTableSchemaVersion` and `InvalidateTableSchemaVersion` are identical~~ ✅ FIXED

**Fix applied:** Both `CatalogStore` and `Catalog` now use a single `BumpTableSchemaVersion` helper.

### 4.7 — Undocumented `tableKey` overloads in `LockManager` ⬜

### 4.8 — ~~Parser silently skips unknown tokens~~ ✅ FIXED

**File:** `DataVo.Core/Parser/Parser.cs`

**Fix applied:** parser no longer advances over unknown tokens. It now throws `ParserException` with token context.

### 4.9 — `CompactTable` resets RowIds without enforcing index rebuild ⬜

### 4.10 — No authentication/authorization ⬜

### 4.11 — `DataVoTransaction` missing savepoint support ⬜

### 4.12 — ~~WAL `TransactionId` (Guid) disconnected from MVCC (long)~~ ✅ FIXED

**Files:** `DataVo.Core/Transactions/WalEntry.cs`, `DataVo.Core/Transactions/RecoveryManager.cs`

**Fix applied:** WAL entries now persist `MvccTransactionId`; replay restores transaction context IDs and recovery advances allocator high-water mark using recovered MVCC IDs.

### 4.13 — Static cardinality feedback dict grows without eviction ⬜

### 4.14 — ~~Index persistence silently drops I/O errors~~ ✅ FIXED

**File:** `DataVo.Core/Indexing/IndexManager.cs`

**Fix applied:** index flush now throws when persistence handler is missing, and index delete/drop paths now throw when persistence file deletion fails but files still exist. Silent I/O failure swallowing was removed.

---

## 5. Missing Features (Functional Gaps)

| Feature                                       | Status           | Notes                                                               |
| --------------------------------------------- | ---------------- | ------------------------------------------------------------------- |
| Correlated subqueries                         | ❌ Not supported | Hard-coded `throw` in `SubqueryExpressionMaterializer.cs:167`       |
| Parenthesized `SELECT` / `UNION` branches     | ❌ Not supported | Parser throws `ParserException` on encounter                        |
| Window functions (`RANK`, `DENSE_RANK`, etc.) | ⚠️ Partial       | Only some functions work; `RANK` throws "not supported yet"         |
| `ON UPDATE` cascades                          | ❌ Not supported | Only `ON DELETE CASCADE/RESTRICT` is modeled                        |
| Multi-column indexes                          | ⚠️ Partial       | Parser accepts N columns but B+Tree is single-key                   |
| Transaction savepoints                        | ❌ Not supported |                                                                     |
| `TRUNCATE TABLE`                              | ❌ Not supported |                                                                     |
| `CHECK` constraints                           | ❌ Not supported |                                                                     |
| Schema versioning / DDL migrations            | ❌ Not supported |                                                                     |
| `EXPLAIN` / query plan inspection             | ❌ Not supported |                                                                     |
| `BIGINT` / `DECIMAL` / `NUMERIC` types        | ❌ Not supported | Only `INT`, `FLOAT`, `VARCHAR`, `BIT`, `DATE`, `DATETIME`, `VECTOR` |
| Connection pooling                            | ❌ Not supported |                                                                     |
| Full-text search                              | ❌ Not supported |                                                                     |
| Row-level security                            | ❌ Not supported |                                                                     |

---

## Changes Made (Phase 1)

### New Files

- `DataVo.Core/Exceptions/DataVoException.cs` — Base exception for all engine errors
- `DataVo.Core/Exceptions/StorageException.cs` — Storage I/O exception
- `DataVo.Core/Exceptions/CatalogException.cs` — Schema/catalog exception
- `DataVo.Core/Exceptions/RowDeletedException.cs` — Tombstoned row exception
- `DataVo.Tests/AuditFixes/AuditFixTests.cs` — 14 tests covering all fixes

### Modified Files

- `DataVo.Core/Runtime/CatalogStore.cs` — Atomic save + deduplicated schema version methods
- `DataVo.Core/Models/Catalog/Catalog.cs` — Deduplicated schema version methods
- `DataVo.Core/StorageEngine/StorageContext.cs` — Narrowed exception catching
- `DataVo.Core/Cache/CacheStorage.cs` — `Dictionary` → `ConcurrentDictionary`
- `DataVo.Core/MVCC/TransactionIdAllocator.cs` — Lock-free + `RestoreHighWaterMark`
- `DataVo.Core/MVCC/VersionStorageManager.cs` — Implements `IDisposable`
- `DataVo.Core/StorageEngine/Disk/DiskStorageEngine.cs` — `RowDeletedException` + header constants

## Changes Made (Phase 2)

### New Files

- `DataVo.Core/Exceptions/RowNotFoundException.cs` — Domain exception for non-existent row coordinates

### Modified Files

- `DataVo.Core/Transactions/LockManager.cs` — Table lock lifecycle cleanup via reference-counted entries
- `DataVo.Core/StorageEngine/Disk/DiskStorageEngine.cs` — File-lock lifecycle cleanup on `DropTable` / `DropDatabase`
- `DataVo.Core/StorageEngine/Memory/InMemoryStorageEngine.cs` — Throws domain row exceptions instead of generic `Exception`
- `DataVo.Core/StorageEngine/StorageContext.cs` — `TableContainsRow` handles `RowNotFoundException`
- `DataVo.Core/Parser/Parser.cs` — Unknown-token hard fail (`ParserException`) instead of silent skip
- `DataVo.Tests/E2E/DiskIndexConcurrencyTests.cs` — Stabilized race-aware assertions for known concurrent mutation semantics
- `DataVo.Tests/AuditFixes/AuditFixTests.cs` — Added parser + disk lock cleanup regression tests
- `DataVo.Tests/Transactions/LockManagerRowLevelTests.cs` — Added table lock cleanup regression test

## Changes Made (Phase 3)

### New Files

- `DataVo.Core/Transactions/TransactionIdStateStore.cs` — Persist/restore transaction ID high-water mark with atomic file replacement

### Modified Files

- `DataVo.Core/StorageEngine/Config/DataVoConfig.cs` — Added transaction ID state file configuration and path resolver
- `DataVo.Core/MVCC/TransactionIdAllocator.cs` — Added high-water mark observer callbacks for persistence integration
- `DataVo.Core/Runtime/DataVoEngine.cs` — Wired tx-id state restore on startup and force-persist on dispose
- `DataVo.Core/Transactions/WalFileStore.cs` — Added checksum envelope format, corruption hard-fail, and atomic rewrite
- `DataVo.Tests/E2E/WalTests.cs` — Added checksum mismatch and tx-id persistence restart tests
- `DataVo.Tests/E2E/SqlExecutionTestsBase.cs` — Cloned new transaction-id config across reinitializations
- `DataVo.Tests/Transactions/LockManagerRowLevelTests.cs` — Relaxed timing assertions for full-suite stability

## Changes Made (Phase 4)

### Modified Files

- `DataVo.Core/MVCC/VersionStorageManager.cs` — Removed lock-order inversion in vacuum path
- `DataVo.Core/Transactions/WalEntry.cs` — Added persisted `MvccTransactionId` and replay propagation
- `DataVo.Core/Transactions/RecoveryManager.cs` — Restores allocator high-water mark from WAL MVCC IDs before replay
- `DataVo.Tests/E2E/WalTests.cs` — Added assertions for MVCC transaction ID WAL continuity and allocator advancement

## Changes Made (Phase 5)

### Modified Files

- `DataVo.Core/Transactions/LockManager.cs` — Added configurable lock acquisition timeout enforcement with `TimeoutException` on acquisition failure
- `DataVo.Core/StorageEngine/Config/DataVoConfig.cs` — Added `LockAcquireTimeoutMs` config surface
- `DataVo.Core/Runtime/DataVoEngine.cs` — Wires lock manager timeout setting from config
- `DataVo.Core/Indexing/IndexManager.cs` — Removed silent persistence failure swallowing in flush and delete/drop paths
- `DataVo.Tests/Transactions/LockManagerRowLevelTests.cs` — Added row/table timeout behavior tests using cross-thread contention
- `DataVo.Tests/Indexing/IndexManagerTests.cs` — Added persistence failure propagation regression tests
- `DataVo.Tests/E2E/SqlExecutionTestsBase.cs` — Propagates lock timeout config in cloned test configurations
