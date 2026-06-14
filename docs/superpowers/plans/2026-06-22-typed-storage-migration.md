# Typed Storage Migration — Implementation Plan (GC Reduction Slice 4, Step 2)

> **For agentic workers:** Use `superpowers:test-driven-development` (RED → GREEN → REFACTOR) for every
> task. Small commits, one logical change each. Run the full suite at each phase boundary; never weaken a
> test. Design: [`../specs/2026-06-22-typed-storage-design.md`](../specs/2026-06-22-typed-storage-design.md).

**Architecture (approved v2):** backend stays `byte[]` (no `IStorageEngine` change); typed APIs are
**additive**; public dictionary results remain boundary materialization; schema evolution uses the
existing full-table DDL rewrite. Stored row = `CellValue[]` over a shared `ReactiveRowSchema`; hot reads
use a borrowed `StoredRowView`; legacy consumers use a read-only dictionary adapter until migrated.

**Baseline:** full suite 935/935 green; complex-vip macro 259.4 MB; per-tick storage-write+validation
3,655 B/tick. **Verify command (all phases):** `dotnet test DataVo.Tests/DataVo.Tests.csproj`.

---

## Gate 0 — Pre-code verification (must pass before P1; mostly read-only)

Confirm against the repo and record findings inline in this plan. **No P1 code until all six are answered.**

- [x] **G0.1 Backend is `byte[]`. CONFIRMED.** `DataVo.Core/StorageEngine/IStorageEngine.cs:16,25,33` —
  `long InsertRow(string,string,byte[])`, `List<long> InsertRows(string,string,List<byte[]>)`, `byte[]
  ReadRow(string,string,long)`. Impls: `DataVo.Core/StorageEngine/Memory/InMemoryStorageEngine.cs:29,52,80`,
  `DataVo.Core/StorageEngine/Disk/DiskStorageEngine.cs`, `DataVo.Core/StorageEngine/Backends/WasmStorageBackend.cs:24-26`.
  `DataVo.Core/StorageEngine/StorageContext.cs:118,136` serialize the dict to `byte[]` before the backend.
- [x] **G0.2 Catalog SQL types. CONFIRMED.** `DataVo.Core/Enums/DataTypes.cs:7` = exactly `Int, Float,
  Bit, Date, Varchar, Vector`. So `CellValue` must add `Date` (`DateOnly`) and `Vector` (`float[]`) — the
  other four already map (Int32/Double/Boolean/String). Catalog/runtime mapping: `Date`→`DateOnly`,
  `Vector`→`float[]`.
- [x] **G0.3 DATETIME reachability. CONFIRMED — exclude.** `DATETIME` appears only in
  `DataVo.Core/StorageEngine/Serialization/RowSerializer.cs`, not in `DataTypes` nor any parser type table.
  Not parser/catalog reachable → **do not** add a `CellValue` DATETIME case for storage.
- [x] **G0.4 Public concrete-dictionary APIs. CONFIRMED.** Surfaces that **stay dictionaries** (boundary
  materialization, P4 keeps them): `DataVo.Core/Contracts/Results/QueryResult.cs:24` (`Data` /
  `:55` `Success(...)`), `DataVo.Core/DataVoContext.cs:358,425` (`SearchNearest`). Internal
  `StorageContext.GetTableContents`/`SelectFromTable` (`StorageContext.cs:196,205`) may be loosened/typed.
- [x] **G0.5 DDL rewrite + row-id reassignment. CONFIRMED.** `DataVo.Core/Parser/DDL/AlterTableAddColumn.cs:25,41,75,85`
  reads all rows (`Context.GetTableContents`), `RewriteTable` re-inserts via `Context.InsertIntoTable`
  → **new row ids are assigned** (`:85,100`) and indexes rebuilt. P3.2 makes this rewrite typed and must
  re-extract index keys against the new ids.
- [x] **G0.6 VECTOR ALTER parser support. CONFIRMED GAP.** `DataVo.Core/Parser/DDL/ColumnDefinitionParser.cs:10-18`
  `ParseType` maps `int/float/bit/date` and **falls through to `Varchar`** — it does **not** parse
  `VECTOR`. ALTER ADD/MODIFY use it (`AlterTableAddColumn.cs:112`). ⇒ P3.2 must add a `VECTOR` case to
  `ParseType` (+ `ParseLength` for dimension) with a parse test **before** typed ALTER VECTOR; DATE works
  today.
- [x] **G0.7 Coverage map (regression oracles).** InsertTyped: `DataVo.Tests/E2E/InsertTypedTests.cs`.
  HNSW/vector: `DataVo.Tests/E2E/DML/VectorIndexTests.cs` (+ in-memory/disk variants). Reactive IVM oracle:
  `DataVo.Tests/Property/ReactiveIvmOracleTests.cs`. Recursive/Join: `DataVo.Tests/E2E/ReactiveRecursiveCteTests.cs`,
  `ReactiveJoinTests.cs`. WAL recovery: `DataVo.Tests/E2E/WalTests.cs`. Disk round-trip:
  `[InlineData(StorageMode.Disk)]` across reactive + DML + `DataVo.Tests/E2E/DiskIndexConcurrencyTests.cs`.

**Recording:** write each finding (with file:line evidence) inline above and commit as
`docs(storage): record typed-storage Gate 0 findings` once all six are answered. (No production code in
Gate 0.)

---

## P0 — Foundation (additive, isolated; prerequisite for all)

### Task P0.1 — `CellValue` gains `DateOnly`
- **Files:** `DataVo.Core/Runtime/Reactive/CellValue.cs` (+ `CellType` enum).
- **Test (add):** `DataVo.Tests/Reactive/CellValueDateTests.cs` — `From(DateOnly)`/`AsDate()` round-trip;
  `From(object?)` with a boxed `DateOnly`; `ToObject()` returns `DateOnly`; `IsNull`/`Null` behavior; type
  mismatch throws.
- **Command:** `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter CellValueDateTests`
- **RED:** compile error (`CellType.Date`/`AsDate` missing). **GREEN:** all pass.
- **Commit:** `feat(cellvalue): add DateOnly cell (Slice 4 P0)`

### Task P0.2 — `CellValue` gains `Vector` (`float[]`) with strict ownership
- **Files:** `CellValue.cs`.
- **Behavior:** `From(float[])` **clones** the input; `ToObject()` returns a **clone**; `AsVector()` returns
  a clone (or a `ReadOnlySpan<float>` accessor) — never the stored array.
- **Test (add):** `DataVo.Tests/Reactive/CellValueVectorTests.cs` — store a vector, mutate the original →
  cell unaffected; `ToObject()` result mutation → cell unaffected; round-trip equality; null/empty.
- **Command:** `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter CellValueVectorTests`
- **RED:** compile error / mutation leaks. **GREEN:** ownership isolation proven.
- **Commit:** `feat(cellvalue): add owned Vector(float[]) cell (Slice 4 P0)`

### Task P0.3 — `StoredRow` (owned) + `StoredRowView` (borrowed)
- **Files (add):** `DataVo.Core/StorageEngine/StoredRow.cs`.
- **Types:** `StoredRow(ReactiveRowSchema schema, CellValue[] cells)` (owns array); `readonly ref struct
  StoredRowView(ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells)`; ordinal + by-name access.
- **Test (add):** `DataVo.Tests/Storage/StoredRowTests.cs` — by-name (case-insensitive) + ordinal access,
  `Count`, missing-column behavior, view-over-span correctness, no copy in the view path.
- **Command:** `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter StoredRowTests`
- **RED → GREEN.** **Commit:** `feat(storage): StoredRow + StoredRowView typed row containers (Slice 4 P0)`

### Task P0.4 — Read-only dictionary adapter over `(schema, cells)`
- **Files (add):** `DataVo.Core/StorageEngine/StoredRowDictionaryView.cs` (implements
  `IReadOnlyDictionary<string, object?>`).
- **Behavior:** case-insensitive lookup; `ContainsKey`/`TryGetValue`/indexer/`Keys`/`Values`/`Count`/
  enumeration in schema order; values via `CellValue.ToObject()` (Vector returns a **clone**); read-only
  (no mutation surface).
- **Test (add):** `DataVo.Tests/Storage/StoredRowDictionaryViewTests.cs` — all of the above + missing key,
  null cells, enumeration order = schema order, and **mutation-impossibility** (vector clone) test.
- **Command:** `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter StoredRowDictionaryViewTests`
- **RED → GREEN.** **Commit:** `feat(storage): read-only dictionary adapter over typed rows (Slice 4 P0)`

**Phase gate:** `dotnet test DataVo.Tests/DataVo.Tests.csproj` (full suite green; P0 is purely additive).

---

## P1 — Typed serializer + typed insert (the per-tick win)

### Task P1.1 — `RowSerializer` typed overloads (same wire format)
- **Files:** `DataVo.Core/StorageEngine/Serialization/RowSerializer.cs`.
- **Add:** `SerializeCells(StoredRow, schema metadata) → byte[]` and `DeserializeCells(byte[], schema) →
  CellValue[]`, producing/consuming the **identical binary layout** as the existing dict
  `Serialize`/`Deserialize`.
- **Test (add):** `DataVo.Tests/Storage/RowSerializerTypedParityTests.cs` — for each catalog type
  (INT/FLOAT/BIT/DATE/VARCHAR/VECTOR): dict-`Serialize` bytes == `SerializeCells` bytes, and
  `DeserializeCells` ∘ `SerializeCells` == original; legacy bytes (dict path) read back equal via
  `DeserializeCells`.
- **Command:** `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter RowSerializerTypedParityTests`
- **RED:** methods missing / byte mismatch. **GREEN:** byte-for-byte parity.
- **Commit:** `feat(storage): typed RowSerializer SerializeCells/DeserializeCells, wire-compatible (Slice 4 P1)`

### Task P1.2 — Typed `InsertTypedRow` (no dictionaries)
- **Files:** `DataVo.Core/Parser/DML/InsertRowService.cs` (`InsertTypedRow` + new typed helpers),
  `DataVo.Core/StorageEngine/StorageContext.cs` — add exactly
  `public List<long> InsertTypedRows(IReadOnlyList<StoredRow> rows, string tableName, string databaseName)`
  that calls `RowSerializer.SerializeCells` per row and `_storageEngine.InsertRows(db, table, List<byte[]>)`
  (mirrors the existing dict `InsertIntoTable` at `:136`; no `IStorageEngine` change).
- **Change:** replace `inputRow`/`normalized` dict construction with **typed normalization** into a
  `CellValue[]` (defaults/coercion), **typed constraint checks** (PK/unique/FK read from cells via the
  cached `TableValidationMetadata`), **typed index-key extraction**, and `SerializeCells` → backend. Extend
  the typed cell validation beyond INT/VARCHAR/BIT to all catalog types.
- **Tests (modify/add):** `DataVo.Tests/E2E/InsertTypedTests.cs` (existing must stay green); add typed
  DATE/VECTOR/FLOAT insert + read-back parity vs the dict path.
- **Command:** `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "InsertTypedTests|TypedInsertStorage"` then full suite +
  disk-mode (`--filter "FullyQualifiedName~Disk"`).
- **RED:** new typed-insert parity test fails (dict path still used). **GREEN:** typed path passes; existing
  InsertTyped + disk round-trip green.
- **Commit:** `perf(insert): typed normalization + typed serialize on InsertTyped path (Slice 4 P1)`

### Task P1.3 — Measure
- **Command:** recreate the per-tick profiler spike (storage-write/capture/delivery) + macro benchmark
  `dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario
  complex-vip --engine datavo`; record before/after in the roadmap; remove the spike.
- **Expected:** storage-write bucket drops materially; macro GC below 259.4 MB. **Commit:**
  `docs(gc-reduction): record Slice 4 P1 typed-insert measurement`

**Phase gate:** full suite green incl. disk round-trip + InsertTyped + reactive IVM.

---

## P2 — Typed read APIs + hot readers

### Task P2.1 — Typed `StorageContext` read APIs (additive)
- **Files:** `DataVo.Core/StorageEngine/StorageContext.cs` — add typed companions to `ReadRowsById` /
  `GetTableContents` returning typed rows. **⚠ Gate 0 resolution required (G0.4):** for each legacy
  concrete-`Dictionary` signature, the G0.4 finding decides per call site — loosen to `IReadOnlyDictionary`
  (internal) or keep concrete + materialize at the boundary (public). Do not start P2.1 until G0.4 lists
  each site's choice.
- **Test (add):** `DataVo.Tests/Storage/TypedReadParityTests.cs` — typed read == legacy `GetTableContents`
  for a seeded table across all catalog types.
- **Command:** `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter TypedReadParityTests` → full suite. **RED → GREEN.**
- **Commit:** `feat(storage): typed read APIs alongside legacy GetTableContents (Slice 4 P2)`

### Task P2.2 — Migrate hot readers to typed
- **Files:** `Runtime/Reactive/ReactiveRegistry.cs` (seed), `CompiledQueries/DataVoCompiledQuery.cs`
  (candidate reads), `Parser/DQL/Select.cs` (projection). Adapter only at public boundaries.
- **Tests:** existing reactive/IVM-oracle, compiled-query, and SELECT tests are the parity oracle (must stay
  green); add a reactive-seed allocation test.
- **Command:** `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~Reactive|FullyQualifiedName~CompiledQuery|FullyQualifiedName~Select"` → full suite.
- **Commit (per reader):** `perf(read): typed <reader> materialization (Slice 4 P2)`

**Phase gate:** full suite green incl. IVM oracle.

---

## P3 — DML / DDL / index / transactions

### Task P3.1 — Typed update/delete + index-key extraction
- **Files:** `Parser/DML/Update.cs`, `DeleteFrom.cs`, `Indexing/IndexManager.cs`.
- **Tests:** existing update/delete/index tests (parity); add typed index-key tests.
- **Commit:** `perf(dml): typed update/delete + index key extraction (Slice 4 P3)`

### Task P3.2 — Typed DDL rewrite (existing model)
- **Files:** `DataVo.Core/Parser/DDL/AlterTableAddColumn.cs`, `DataVo.Core/Parser/DDL/AlterTableDropColumn.cs`,
  `DataVo.Core/Parser/DDL/AlterTableModifyColumn.cs`. **⚠ Gate 0 resolution required (G0.6):** if
  `DataVo.Core/Parser/DDL/ColumnDefinitionParser.cs` does not parse VECTOR on ALTER paths, add that parser
  support as the first sub-task of P3.2 (and a parse test); otherwise DATE-only for ALTER. The G0.6 finding
  sets which.
- **Change:** make the read-all → rewrite → reindex path typed, under the existing DDL write lock; no
  schema-version-tagged payloads.
- **Tests (add):** ALTER ADD/DROP/MODIFY rewrite for DATE and VECTOR; index rebuild after rewrite.
- **Command:** `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "AlterTable|FullyQualifiedName~Ddl"` → full suite.
- **Commit:** `perf(ddl): typed in-place table rewrite for ADD/DROP/MODIFY (Slice 4 P3)`

### Task P3.3 — Transaction/WAL decision (documented)
- **Decide + document:** keep `TransactionContext`/WAL payloads as dictionary **durable/compat boundaries**
  (default, lower risk) or type them. If unchanged, mark them explicitly as boundaries in the design doc.
- **Tests:** WAL recovery + MVCC visibility tests (if touched).
- **Commit:** `docs(storage): record txn/WAL dictionary boundary decision (Slice 4 P3)` (+ code commit only
  if typed).

**Phase gate:** full suite incl. WAL recovery + MVCC + ALTER.

---

## P4 — Adapter removal + final measurement

### Task P4.1 — Remove the dictionary adapter from internal/hot paths
- **Pre-req:** no internal/hot consumer still requires the adapter (public `QueryResult`/`DataVoContext`
  results stay dictionaries — materialized at the boundary).
- **Files:** delete `StoredRowDictionaryView` usages internally; keep public boundary materialization.
- **Command:** full suite. **Commit:** `refactor(storage): drop internal dictionary adapter; data plane typed (Slice 4 P4)`

### Task P4.2 — Final measurement
- **Command:** per-tick profiler + macro benchmark; record final GC + throughput in the roadmap; update the
  memory + roadmap status to Slice 4 COMPLETE.
- **Commit:** `docs(gc-reduction): Slice 4 complete — typed storage final numbers`

---

## Risk controls (every task)
- TDD: failing parity test first, then implement. Typed output ≡ dictionary output.
- Full suite green at each phase boundary; the IVM oracle, Disk round-trip, HNSW/vector, ALTER, and WAL
  tests are the corruption/parity oracles — never weakened.
- Wire format unchanged → existing on-disk data still reads (P1.1 legacy-bytes test guards this).
- Allocation + macro re-measured at P1, P2, P4; bucket must trend to ~0 with no regression.
- Each phase is an isolated, revertible commit behind the adapter.
