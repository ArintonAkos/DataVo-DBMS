# Typed Storage Migration — Design (GC Reduction Slice 4, Step 2)

**Status:** Draft v2 (revised after review; pending re-review)
**Epic:** [GC Reduction Roadmap](../plans/2026-06-21-gc-reduction-roadmap.md) — Phase 6 / Slice 4, Step 2
**Date:** 2026-06-22

## Goal & evidence

Slice 4 Step 1 (validation-metadata cache) cut per-tick allocation 8,353 → 4,590 B/tick and the
complex-vip macro 428.5 → 259.4 MB. The profiled remainder (storage-write + validation ≈ **3,655
B/tick**) is **dictionary materialization on the insert/read path**, not the durable format itself.

> **Corrected storage model (key review fix).** Backends do **not** store dictionaries. `StorageContext`
> serializes a `Dictionary<string,object?>` to `byte[]` via `RowSerializer.Serialize` and calls
> `IStorageEngine.InsertRow(db, table, byte[])` (`StorageContext.cs:118,136`); reads deserialize `byte[]`
> back into dictionaries. **The `Dictionary` is an in-flight intermediate; the stored format is already
> `byte[]`.** So this migration does **not** change `IStorageEngine` or the table store — it adds typed
> (de)serialization + typed materialization that **bypass the dictionary intermediate**, preserving the
> existing binary wire format.

**Non-goals:** changing SQL semantics, the public dictionary-returning API, the on-disk wire format, or
`IStorageEngine`. No schema-version-tagged row payloads in this slice.

## 1. Target architecture

### The typed row (storage-specific, explicit ownership)

Reuse the Slice 1–3 currency (`CellValue`, `ReactiveRowSchema`) but **do not reuse `TypedRow` as the hot
container** — `TypedRow` copies the `CellValue[]` on construction (`TypedRow.cs:13`), which is the wrong
default for an allocation-reduction path. Introduce storage-specific types with explicit ownership:

- **`StoredRow`** — owned `(ReactiveRowSchema schema, CellValue[] cells)`; the canonical owned row that
  serialization writes and full materialization produces. Owns its array.
- **`StoredRowView`** — a borrowed `ref struct` `(schema, ReadOnlySpan<CellValue>)` for read/probe paths
  that must not allocate (analogous to `RowRef`). Never escapes its frame.

A row is positional: `row["Name"]` → `schema.TryGetOrdinal("Name", out i)` → `cells[i]`.

### `CellValue` completeness (hard P0 prerequisite)

Catalog SQL types (`Enums/DataTypes.cs`): **INT, FLOAT, BIT, DATE, VARCHAR, VECTOR**. `CellValue` today
covers `null/bool/int/long/double/decimal/string` (`CellValue.cs`). Mapping + gaps:

| SQL type | CLR | CellValue today |
|---|---|---|
| INT | `int` | ✅ Int32 |
| FLOAT | `double` | ✅ Double |
| BIT | `bool` | ✅ Boolean |
| VARCHAR | `string` | ✅ String |
| **DATE** | `DateOnly` | ❌ **add `CellType.Date`** |
| **VECTOR** | `float[]` | ❌ **add `CellType.Vector`** (reference cell with strict ownership at **every** boundary: `From(float[])` **clones on store** so the cell owns its array; `ToObject()` and the dictionary adapter return a **clone**, never the stored array, so legacy callers can't mutate stored row state — mirrors `NormalizeParsedValue`'s `vector.ToArray()`) |

`decimal`/`long` stay (runtime aggregate results), but they are **not** catalog storage types and must not
drive the storage design. **DATETIME** appears in `RowSerializer` but is **not** in the enum — verify it is
parser/catalog reachable before supporting it (default: exclude).

### Write / read flow (dictionary bypass)

- **Write:** `InsertTypedRow` normalizes the typed cells into a `StoredRow` (no `inputRow`/`normalized`
  dicts), then `RowSerializer.SerializeCells(StoredRow)` → `byte[]` → backend. Same wire format.
- **Read:** backend `byte[]` → `RowSerializer.DeserializeCells(bytes, schema)` → `CellValue[]`; hot readers
  consume `StoredRowView`. Legacy readers get a dictionary via the adapter (below).

### The compatibility adapter (enables incrementalism) + return-type reality

A read-only `IReadOnlyDictionary<string,object?>` view over `(schema, CellValue[])` lets un-migrated
consumers behave identically during the transition (boxes on demand via `CellValue.ToObject()`, so it is a
*bridge*, not an endpoint — hot paths must go fully typed).

> **Review fix — signatures.** Several APIs return **concrete** `Dictionary<long, Dictionary<string,
> object?>>` (`StorageContext.SelectFromTable/GetTableContents`, `:196`). A read-only adapter cannot slot
> in transparently. The plan must, per call site, either (a) **loosen return types** to
> `IReadOnlyDictionary<...>`, or (b) keep the concrete dictionary and **materialize at that legacy
> boundary**. New typed read APIs are added alongside the legacy ones; legacy signatures are loosened or
> boundary-materialized, never silently broken.

## 2. Subsystem blast radius

| Subsystem | File(s) | Change |
|---|---|---|
| Serialization | `StorageEngine/Serialization/RowSerializer.cs` | add `SerializeCells(StoredRow)` + `DeserializeCells(bytes, schema)`; **preserve the binary wire format** (no durable-format versioning this slice) |
| Storage facade | `StorageEngine/StorageContext.cs` | typed insert + typed read APIs alongside legacy; legacy `GetTableContents`/`SelectFromTable` loosened to `IReadOnlyDictionary` or boundary-materialized; backend `byte[]` untouched |
| Insert | `Parser/DML/InsertRowService.cs` | typed normalization, typed constraint checks (PK/unique/FK from cells), typed index-key extraction — replacing `inputRow`/`normalized` dicts and the INT/VARCHAR/BIT-only `ValidateTypedCell` (`:143,221`) |
| Select / projection | `Parser/DQL/Select.cs`, `CompiledQueries/DataVoCompiledQuery.cs` | project from `StoredRowView` by ordinal |
| Update / Delete | `Parser/DML/Update.cs`, `DeleteFrom.cs` | typed read-modify-write |
| DDL (typed in-place rewrite) | `Parser/DDL/AlterTableAddColumn.cs:25`, `AlterTableDropColumn.cs:26`, `AlterTableModifyColumn.cs:27`, `CreateIndex.cs` | keep the **existing full-table rewrite + reindex** model under the DDL write lock; make the rewrite typed (**DATE included; VECTOR only if `ColumnDefinitionParser.ParseType` parses VECTOR on ALTER paths — create-table parsing does, ALTER may not; verify and, if missing, adding ALTER VECTOR parser support is part of the plan**) |
| Indexing | `Indexing/IndexManager.cs` | index keys from typed cells |
| MVCC / txn | `MVCC/*`, `Parser/Transactions/Commit.cs`, WAL | decide explicitly (see Risk): keep dictionary boundary payloads as durable/compat format, or type them |
| Reactive / capture | `Runtime/Reactive/ReactiveRegistry.cs`, `Runtime/Changes/ChangeRecorder.cs` | seed reads typed; capture typed after-image without a dict clone (operators already typed, Slice 3) |
| Public facade | `DataVoContext.cs`, `Runtime/DataVoEngine.cs` | `QueryResult`/public results stay dictionaries (public API) — materialized at the boundary |

## 3. Execution strategy — phased, incremental (no atomic cutover)

Each phase is its own commit and leaves the full suite green. The adapter + additive typed APIs make this
subsystem-by-subsystem; an atomic cutover is rejected as unreviewable.

- **P0 — Foundation.** Add `CellValue` `Date` (`DateOnly`) + `Vector` (`float[]`, owned/cloned). Add
  `StoredRow`/`StoredRowView` with explicit ownership. Add the read-only dictionary adapter. Pure additive;
  unit-tested in isolation. **Prerequisite for everything.**
- **P1 — Typed serializer + typed insert.** `RowSerializer.SerializeCells`/`DeserializeCells` (same wire
  format, parity with dictionary serialization for INT/FLOAT/BIT/DATE/VARCHAR/VECTOR). Rewrite
  `InsertTypedRow` to typed normalization + typed constraints + typed index keys + typed serialize (no
  dict). Public `BulkInsert` stays dictionary via boundary conversion. *The per-tick `InsertTyped`
  storage-write bucket starts dropping here; standard parser / `BulkInsert` inserts remain dictionary-bound
  until separately migrated. The complex-vip benchmark drives `InsertTyped`, so its macro number moves;
  most other workloads improve only once their path is migrated too.*
- **P2 — Typed read APIs + hot readers.** Typed `StorageContext` reads alongside legacy `GetTableContents`.
  Migrate reactive seed, compiled-query candidate reads, and SELECT projection to typed reads. Adapter only
  at public/legacy boundaries.
- **P3 — DML/DDL/index/txn.** Typed update/delete + index-key extraction; typed in-place DDL rewrite
  (existing model); explicit decision on TransactionContext/WAL (keep dictionary durable boundary vs. type
  it — documented either way).
- **P4 — Remove adapter** once no hot/internal consumer needs it. Public `QueryResult`/`DataVoContext`
  results remain dictionaries (public API). Final allocation + macro-benchmark comparison.

## 4. Risk mitigation — zero corruption, 100% parity

- **The 935-test suite is the oracle**, run after every phase with no weakening: the reactive **IVM oracle**
  (independent full-recompute parity), **Disk-mode** `[InlineData(StorageMode.Disk)]` round-trip tests,
  **HNSW/vector-index** tests (already caught a Step-1 invalidation bug), **ALTER TABLE** rewrite tests,
  **WAL recovery** (if touched), and `InsertTyped` tests.
- **TDD per phase**: a failing parity test first (typed output ≡ dictionary output for that subsystem), then
  the implementation. Explicit DATE + VECTOR round-trip parity tests (dictionary-serialize vs cell-serialize
  must produce equivalent reads).
- **Adapter = behavioral oracle** for un-migrated consumers (identical dictionary view).
- **Durability**: wire format unchanged → existing on-disk data still reads; add legacy-bytes → typed-read
  compatibility tests. If TransactionContext/WAL stay dictionary, mark them explicitly as public/durable
  compatibility boundaries.
- **Allocation/throughput gating**: re-run the per-tick profiler and the macro complex-vip benchmark at each
  phase boundary; the 3,655 bucket must trend toward 0 with no regression.
- **Revertibility**: phases are isolated commits behind the adapter; any can be backed out independently.

## Pre-code verification (guardrail — must pass before P1 implementation)

1. Confirm backend rows are `byte[]` through `IStorageEngine` / in-memory + disk backends.
2. Confirm catalog SQL types from `DataTypes` and parser behavior.
3. Confirm whether `DATETIME` is parser/catalog reachable or only in `RowSerializer`.
4. Confirm which public APIs require concrete `Dictionary` return values.
5. Confirm DDL row-rewrite behavior and row-id/index consequences.
6. Confirm existing test coverage for InsertTyped, Disk round-trip, WAL/vector recovery, ALTER TABLE,
   HNSW/vector index, and reactive IVM.

## Definition of done

- Insert/read hot paths produce no per-row dictionary; the stored `byte[]` format and public dictionary API
  are unchanged. Adapter removed from internal/hot paths.
- Per-tick storage-write bucket ≈ 0 beyond the inherent stored row; macro complex-vip GC materially below
  259.4 MB.
- Full suite green at every phase (incl. IVM oracle, Disk round-trip, HNSW, ALTER, WAL); no SQL/API/wire-
  format change.
