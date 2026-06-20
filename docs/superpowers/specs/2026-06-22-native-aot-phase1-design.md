# Native AOT — Phase 1 (Engine Core) Design

> **Status:** Design / scope agreed (2026-06-22). Verification fence implemented; Core cleanup to be
> planned next (`brainstorm → spec → plan` per the program roadmap). Supersedes nothing; this is the
> first slice of the Native AOT initiative recorded in
> [`../plans/2026-06-19-advanced-features-roadmap.md`](../plans/2026-06-19-advanced-features-roadmap.md).

## Goal

Make the DataVo engine core **100% Native AOT compatible** — no runtime reflection / dynamic code on any
exercised path, trim-safe, publishes and runs as a native binary. This is the next strategic priority
after the GC-reduction program hit its goal (complex-vip 143.5 MB, competitive with native engines).

## Scope (Phase 1)

**In:** `DataVo.Core` and `DataVo.Data` (ADO.NET).
**Out (explicit, by user decision 2026-06-22):** `DataVo.EntityFrameworkCore` — EF has its own AOT story
(compiled models + precompiled queries) and is the long pole; it gets a later, separate phase. Also out:
the deferred GC items (dispatch pooling, MVCC object) — the GC program is paused.

## Decisions (user-approved 2026-06-22)

1. **Eradicate Newtonsoft.Json** → System.Text.Json with its built-in **source generator**
   (`JsonSerializerContext` / `[JsonSerializable]`). Breaking the on-disk WAL/HNSW/BTree/catalog formats
   is **approved** — this is a major-version/architectural shift.
2. **Eradicate `dynamic`** → typed `CellValue` / typed `object` references with explicit operations.
3. **Verification before refactor:** a durable AOT fence (this document's other half) measures progress.

## Measured Reflection Hit List (baseline)

Captured by temporarily enabling `<IsAotCompatible>true</IsAotCompatible>` (trim + AOT analyzers) and
clean-building, plus a whole-program `dotnet publish /p:PublishAot=true` of the smoke app. **`DataVo.Core`
baseline = 184 IL warnings (92 IL2026 trim + 92 IL3050 dynamic-code). `DataVo.Data` = 2 (fixed → 0).**

| # | Area | Severity | Where | Fix |
|---|---|---|---|---|
| A | **Serialization — Newtonsoft.Json** | 🔴 | `WalFileStore`, `WalEntry`, `HNSWIndexPersistence`, `JsonBTreeIndex`, `BTreeNode`. Whole `Newtonsoft.Json.dll` emits IL2104/IL3053. | Replace with STJ source-gen contexts |
| B | **Serialization — `XmlSerializer`** | 🔴 | `CatalogStore` (catalog persistence), `Catalog`. **Missed by the initial grep; caught by the analyzer baseline.** `XmlSerializer` generates code at runtime → **the native binary fails immediately on `CREATE DATABASE`** ("error generating the XML document"). | Replace catalog persistence with STJ source-gen |
| C | **Serialization — System.Text.Json reflection mode** | 🟡 | `Select.cs` (HNSW snapshot dict), Volcano `SortOperator` / `HashAggregateOperator` disk spill. | Add `[JsonSerializable]` contexts (no API change) |
| D | **`dynamic` / DLR** | 🔴 | 36 uses / 22 files: `ScalarEvaluator`, `StatementEvaluator(WOJoin)`, `JoinLookupTable : Dictionary<dynamic,…>` + all join strategies, aggregations (Min/Max/Avg/Sum/Count), `AlterTableModifyColumn`, `Update`, `Column`. Binds via `Microsoft.CSharp` (RequiresDynamicCode). | Replace with typed `CellValue`/`object` |
| E | **`Activator.CreateInstance(Type, …)`** | 🟡 | `AggregationService.cs:42` (dynamic aggregation instantiation) — IL2067. | Generated factory / `switch` |
| F | **Trivial** | 🟢 | `AtomicFileOperations` `typeof(File).GetMethod` (one-time probe); `DataVoDataReader.GetType()`. | Annotate / leave |

**Confirmed non-issues:** zero `Reflection.Emit`/`DynamicMethod`/`ILGenerator`; ADO.NET layer clean
(only the now-annotated `GetFieldType`); no `Type.GetType(string)` / assembly scanning / attribute mapping.

## Verification strategy (the durable fence) — IMPLEMENTED

Two layers + a ratchet, the AOT analog of `InsertAllocationGuardTests` ("tighten, never loosen"):

1. **Per-library analyzers (`<IsAotCompatible>true</IsAotCompatible>`)** on `DataVo.Core` and
   `DataVo.Data`: emit IL trim/AOT warnings on every build, pinpointing call sites.
2. **`DataVo.Data` is LOCKED:** its csproj makes the IL trim/AOT codes **errors** (`<WarningsAsErrors>`).
   Currently 0 warnings → any regression fails its build.
3. **`DataVo.Core` ratchet:** analyzers on (baseline 184, NOT yet errors). `scripts/check-aot-baseline.sh`
   builds Core, counts IL warnings, and **fails if the count exceeds the baseline**. Lower the baseline as
   each cleanup lands; at 0, switch Core to `<WarningsAsErrors>` like Data and retire the script.
4. **End-to-end native gate — `DataVo.AotSmoke`:** a console app exercising the real public surface
   (engine + `InsertTyped` + borrowed `SubscribeZeroAlloc` + ADO.NET reader; no EF), with `PublishAot`.
   - Managed run: **green** (all checks pass).
   - `dotnet publish -r <rid> /p:PublishAot=true`: produces a 15 MB native binary today, which **fails at
     runtime on `CREATE DATABASE`** (XmlSerializer). This is the concrete pass target: the cleanup is done
     when the native binary prints `ALL SMOKE CHECKS PASSED`.
5. **CI (to wire up):** run `scripts/check-aot-baseline.sh` + the AOT publish of `DataVo.AotSmoke` on
   linux-x64; both must stay green. The per-project IL count is the live scoreboard.

## Suggested execution order for the Core cleanup (to be planned next)

Sequenced by impact × independence (measured): (1) **catalog `XmlSerializer` → STJ source-gen** — unblocks
the very first native operation (`CREATE DATABASE`); (2) **Newtonsoft → STJ source-gen** across
WAL/HNSW/BTree; (3) **STJ reflection → contexts** for Volcano spill + Select snapshot; (4) **`dynamic`
removal** (ScalarEvaluator/join/aggregations) → typed `CellValue`; (5) **`Activator` aggregation factory**;
ratchet the Core baseline to 0 and lock it; native smoke must go green. TDD per step; on-disk format break
is accepted.

## Out of scope / non-goals

- EF Core AOT (separate phase, EF's own tooling).
- Changing the `byte[]` storage wire format of rows (that is Slice 4's typed serializer; unrelated to JSON).
- Public API changes — `DataVoContext`, ADO.NET surface, reactive APIs stay as-is.
