# Compile-Time Typed Row Materialization — Design

> **Status:** Approved for build (2026-06-24). Next feature on branch
> `feature/roslyn-compile-time-access-path-poc`, building on the shipped access-path tag work
> (`CompiledAccessPath`, runtime honoring, `SelectMany`/`SelectSingle` tagging). Targets the **Layer 3** cost
> identified by the 2026-06-23 profiling spike: **1,766 B per matching row** from `MaterializeStoredRow`'s
> `Dictionary<string,object?>` + projection dict + boxing. (Layer 2 param/key dicts — ~504 B/call, 9% — and the
> LINQ-plumbing fixed cost — ~3,210 B/call — are explicitly later tracks.)

## 1. Goal

Make the source generator emit a **strongly-typed, zero-boxing projector** that constructs the projection type
directly from the stored row's typed cells, so a generated `SelectSingle`/`SelectMany` returns rows with
**zero per-row heap allocation** — no `Dictionary<string,object?>` materialization, no projection dictionary,
no boxing. Identical observable results to today's dictionary path.

## 2. Constraints that shape the design

Verified in the codebase:

- **`CellValue` is public and zero-boxing** (`DataVo.Core/Runtime/Reactive/CellValue.cs`): typed accessors
  `AsInt32/AsInt64/AsDouble/AsDecimal/AsBoolean/AsString/AsDate/AsVector`, plus `IsNull`/`Type`. The boxing
  today is solely `view[i].ToObject()` inside `MaterializeStoredRow`.
- **`StoredRow`/`StoredRowView` are `internal`, and `StoredRowView` is a `ref struct`**
  (`DataVo.Core/StorageEngine/StoredRow.cs`). The generated mapper lives in the *consumer* assembly, which
  cannot see internals; and a `ref struct` cannot be a `Func<>` type argument. → We need a **public reader
  type** wrapping the internal view, plus a **custom delegate** (not `Func<>`).
- **`DataVo.Generators` is `netstandard2.0` and cannot reference `DataVo.Core`.** The generator emits the
  typed projector as *text* referencing the public reader/delegate; it never touches Core types directly.
- Both `SelectSingle` and `SelectMany` already flow through `ExecuteSelect` → `TryReadMatchingRowEntries`
  (`DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`), which is where `StoredRow` is available before it is
  materialized to a dictionary.

## 3. Architecture

```
COMPILE TIME (DataVoQueryGenerator), per generated SelectSingle/SelectMany:
  CLEAN CTOR MATCH?  (a non-implicit ctor whose params match the projected columns by name,
                      AND every param type is in the supported set §6)
   ├─ yes → emit the TYPED path:
   │     private static T __DataVoMap_X(CompiledRowReader reader)
   │         => new T(reader.GetInt32("OrderId"), reader.GetString("Sku")!);
   │     private static readonly CompiledRowMapper<T> __DataVoMapper_X = __DataVoMap_X;   // cached once
   │     return DataVoCompiledQuery.SelectManyTyped<T>(db, plan, prms, __DataVoMapper_X);
   └─ no  → emit TODAY'S dict mapper unchanged:
         return DataVoCompiledQuery.SelectMany<T>(db, plan, prms, static row => new T((int)row["…"]!, …));

RUNTIME (DataVo.Core):
  SelectManyTyped<T> / SelectSingleTyped<T>
    → ExecuteSelectTyped<T>:
        build expectedKey (ToParameterDictionary + BuildComparisonKey — unchanged; Layer-2 cost stays)
        matches = TryReadMatchingStoredRows(context, plan, db, expectedKey)   // shared finder, returns StoredRow
        for each (rowId, StoredRow row) in matches:
            results[i] = mapper(new CompiledRowReader(row.AsView()))           // NO dict, NO projection, NO boxing
```

## 4. Components

| Unit | File | Responsibility | Dependencies |
|---|---|---|---|
| `CompiledRowReader` | `DataVo.Core/CompiledQueries/CompiledRowReader.cs` (new) | **public ref struct** wrapping an internal `StoredRowView`. **Internal constructor** (only Core builds it; the consumer only receives it). Public zero-boxing getters keyed by column name (§6). Each getter reads `_view[column]` (throws `KeyNotFoundException` on an unknown column — fail-fast) then the matching `CellValue.AsX()`. No allocation. | `StoredRowView`, `CellValue` (same assembly) |
| `CompiledRowMapper<T>` | `DataVo.Core/CompiledQueries/CompiledRowMapper.cs` (new) | `public delegate T CompiledRowMapper<T>(CompiledRowReader reader);` A custom delegate because a `ref struct` parameter is illegal on `Func<>`. Instances are one-time cached statics → no per-row allocation. | `CompiledRowReader` |
| Typed executor | `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs` (modify) | `public static IReadOnlyList<T> SelectManyTyped<T>(DataVoContext, DataVoCompiledQueryPlan, IReadOnlyList<DataVoCompiledQueryParameter>, CompiledRowMapper<T>)` and `public static T? SelectSingleTyped<T>(…)`, sharing `private static IReadOnlyList<T> ExecuteSelectTyped<T>(…)`. Validates plan kind exactly as the dict siblings do. | shared finder; `CompiledRowReader` |
| Shared finder | same file (modify) | Extract `private static List<KeyValuePair<long, StoredRow>> TryReadMatchingStoredRows(context, plan, db, expectedKey)` carrying the existing branch structure (compile-time tag → PK → secondary → typed scan), returning `StoredRow` (not dict). `TryReadMatchingRowEntries` becomes `TryReadMatchingStoredRows(...).Select(kvp => new KeyValuePair<…>(kvp.Key, MaterializeStoredRow(kvp.Value)))`. `ReadRowsViaIndex` is likewise split so the index branch yields `StoredRow`. | existing index/scan logic |
| Generator emission | `DataVo.Generators/DataVoQueryGenerator.cs` (modify) | Detect a clean ctor-name match (reuse the existing ctor-match logic in `MapperArguments`). Map each ctor param's C# type → reader getter (§6). Emit the `__DataVoMap_X` method, the cached `__DataVoMapper_X` field, and the `…Typed` invocation. If not a clean match or any param type is unsupported → emit today's dict mapper for that query. | none (emits text) |

Lifetime safety: the `StoredRow` instances come from `GetTypedTableContents` (held in a `Dictionary<long,StoredRow>` and the returned list); `AsView()` borrows their live cell arrays; mapping happens eagerly inside `ExecuteSelectTyped` while that collection is alive. The `ref struct` reader never escapes the per-row loop.

## 5. Fallback & safety model (the invariant)

- **Generation-time fallback only.** Not a clean ctor-name match, or any param type outside §6 → the generator
  emits today's dict mapper for that whole query. Full correctness, zero risk, no diagnostic (consistent with
  the deferred-diagnostics decision). Typed materialization is a pure additive optimization the generator opts
  into.
- **No runtime fallback, by design.** On valid data the typed and dict paths produce **identical results**. On
  invalid data — a cell whose type ≠ the projection param type, or SQL NULL into a non-nullable param —
  `CellValue.AsX()` throws `InvalidOperationException`; the dict path's `(int)row["c"]!` throws on the very
  same data. The paths are **fail-equivalent**, so a runtime fallback would only mask a real schema/projection
  contract bug. Fail-fast is the chosen behavior.

## 6. Supported types → reader getter

| Projection param C# type | Reader getter | `CellValue` accessor |
|---|---|---|
| `int` / `int?` | `GetInt32` / `GetInt32OrNull` | `AsInt32` |
| `long` / `long?` | `GetInt64` / `GetInt64OrNull` | `AsInt64` |
| `double` / `double?` | `GetDouble` / `GetDoubleOrNull` | `AsDouble` |
| `decimal` / `decimal?` | `GetDecimal` / `GetDecimalOrNull` | `AsDecimal` |
| `bool` / `bool?` | `GetBoolean` / `GetBooleanOrNull` | `AsBoolean` |
| `DateOnly` / `DateOnly?` | `GetDate` / `GetDateOrNull` | `AsDate` |
| `string` (non-null) / `string?` | `GetString` + `!` / `GetString` | `AsString` |
| `float[]` | `GetVector` | `AsVector` |

`GetXOrNull(col)` reads the cell once: `CellValue c = _view[col]; return c.IsNull ? null : c.AsX();`.
Non-nullable getters delegate straight to `_view[col].AsX()` (which throws on NULL/type mismatch — the
fail-fast path). Any ctor param type not in this table forces
the generation-time dict fallback for the query.

## 7. Scope

**In:** all 8 `CellValue` scalar types + nullable value-type params + non-null `string`/`string?`; both
`SelectSingle` and `SelectMany`; every generated select with a clean ctor match, regardless of access path.
**Out (later tracks):** Layer-2 param/comparison-key dict removal; LINQ-plumbing de-LINQ; composite indexes;
the heuristic positional mapper (`InferCastType`) stays dict-only — typed emission requires a real ctor-name
match.

## 8. Testing

1. **Typed parity** (`DataVo.Tests/E2E`): `SelectManyTyped`/`SelectSingleTyped` return results identical to the
   dict `SelectMany`/`SelectSingle` for the same query across every supported type, including a row with SQL
   NULLs projected into nullable params.
2. **Zero-allocation** (headline): the row-count differencing harness on the typed path asserts **≈0 B per
   matching row** (vs the dict path's ~1,766 B/row) — per-row allocation strictly far below the dict path.
3. **Generator emission** (`DataVo.Generators.Tests`, string asserts): clean match → generated source contains
   `CompiledRowReader`, the right `GetX("Col")` calls, `SelectManyTyped`, and the cached `CompiledRowMapper<T>`
   field; `SelectSingle` → `SelectSingleTyped`; non-clean match / unsupported param type → falls back to dict
   `SelectMany`/`SelectSingle` (no `CompiledRowReader`).
4. **Source-generated E2E** (`DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests` pattern): a generated typed
   query compiles and runs end-to-end, returning correct rows — proves the emitted typed code is valid and
   executes.
5. **Reader behavior via the typed path** (the reader ctor is internal, so exercise it through `…Typed`): each
   supported type round-trips; a type mismatch throws; `IsNull`/nullable getters behave; an unknown column
   throws `KeyNotFoundException`.

## 9. Success criteria

- Generated clean-match selects emit the typed projector + `…Typed` call; non-matches keep the dict mapper.
- Typed path returns results identical to the dict path (test 1) and allocates ≈0 B/row (test 2).
- Generated typed code compiles and runs (test 4).
- Whole solution builds with **0 warnings** (PoC code), stays **AOT-clean** (delegate + ref struct + typed
  `CellValue` access — no reflection), full suite green.

## 10. Risks

- **Delegate-with-ref-struct-parameter legality** — allowed in C# (the ref struct is a concrete parameter, not
  a generic arg); validated by the source-generated E2E test compiling and running.
- **`StoredRowView` lifetime** — mitigated by eager per-row mapping inside `ExecuteSelectTyped` while the
  `StoredRow` collection is alive; the reader never escapes the loop.
- **Finder refactor regression** — `TryReadMatchingStoredRows` must preserve the exact tag/PK/secondary/scan
  behavior; guarded by the existing `CompiledAccessPathTests` + `CompiledQueryRuntimeTests` staying green plus
  the new parity tests.

## 11. Bottom line

A public `CompiledRowReader` ref struct + a cached-static `CompiledRowMapper<T>` delegate let the generator emit
a typed, zero-boxing projector that constructs the projection straight from typed `CellValue` cells, driven by a
shared `TryReadMatchingStoredRows` finder that keeps the dict path intact. It reclaims the 1,766 B/row Layer-3
cost for every generated select with a clean constructor match, fail-fast by design, with no new reflection and
no change to observable results.
