# Row-Fetch Projection Pushdown — Design

> **Status:** Approved for build (2026-06-24). Builds on the typed-materialization work
> (`CompiledRowReader`/`SelectManyTyped`) on branch `feature/roslyn-compile-time-access-path-poc`. Targets the
> row-fetch/deserialization cost the 2026-06-24 per-phase profiling spike isolated.

## 1. Context — what the profiling spike found

Per-phase attribution of the typed read path (tagged `SelectMany`, 8 matched rows, wide row):

| Phase | B/call | B/row | Share |
|---|---|---|---|
| **`GetTypedTableContents` (deserialize)** | **11,648** | **1,456** | **~80%** |
| `FilterUsingIndex` (index lookup) | 1,288 | 161 | 9% |
| `expectedKey` (Layer-2 dicts) | 736 | 92 | 5% |
| LINQ Where/Select/ToList | 472 | 59 | 3% |
| map loop | 408 | 51 | 3% |

The earlier "de-LINQ" hypothesis was wrong (LINQ = 3%). The dominant cost is **per-row deserialization**. Key
facts established by reading the code:

- **In-memory `ReadRow` returns the *stored* `byte[]` by reference — zero allocation** (`InMemoryStorageEngine.cs:92`).
  The per-row `byte[]` is only a *disk*-mode cost.
- `RowSerializer.DeserializeCells` (`RowSerializer.cs:182`) allocates, **per row**: `new MemoryStream(data)` +
  `new BinaryReader(...)` (two objects), a full `CellValue[columns.Count]`, and — via `ReadString` — **one string
  per VARCHAR column, including columns the query never projects**. `GetTypedTableContents` then collects rows
  into a `Dictionary<long,StoredRow>`.
- The wire format (`RowSerializer`) is **forward-only**: per column, a 1-byte null flag then the typed value
  (fixed width for INT/FLOAT/BIT/DATE/VECTOR; `BinaryWriter`-style 7-bit length-prefixed UTF8 for strings). It is
  **not random-access but is skippable** — an unprojected column can be advanced past without allocating.

## 2. Goal

Bring the compiled typed query path's per-row allocation to its theoretical minimum: **only the projected string
cells + the result record**. No per-row stream objects, no decoding of unprojected columns, no `StoredRow`, no
`Dictionary<long,StoredRow>`.

## 3. Architecture (two layers)

```
SHARED (RowSerializer) — benefits any caller:
  ByteSpanReader (ref struct over ReadOnlySpan<byte>): ReadBoolean/Int32/Int64/Single/String + Skip*,
     byte-identical to BinaryWriter's format (incl. 7-bit-encoded string length). Allocation-free.
  DeserializeCells reimplemented on ByteSpanReader → drops the per-row MemoryStream + BinaryReader globally.

COMPILED TYPED PATH — projection pushdown + streaming:
  Per query (once): resolve storage columns (cached); mark which are projected (plan.ProjectedColumns) and their
     storage positions; build a projected ReactiveRowSchema (projected columns in storage order); allocate ONE
     reused CellValue[] buffer (length = projected count).
  Per matched rowId (streamed, no dict):
     bytes = ReadRow(...)                 // in-memory: stored ref, 0 alloc
     MVCC visible? --no--> skip           // visibility checked BEFORE paying decode cost
     ByteSpanReader over bytes: for each storage column in order ->
         projected? decode into reusedBuffer[k++]   :   skip-advance (no alloc)
     mapper(new CompiledRowReader(new StoredRowView(projectedSchema, reusedBuffer)))  -> T
     append T to results
```

Per-row allocation collapses to the **projected** string cells (irreducible) + the result record (irreducible).

## 4. Components

| Unit | File | Responsibility |
|---|---|---|
| `ByteSpanReader` | `DataVo.Core/StorageEngine/Serialization/ByteSpanReader.cs` (new) | `ref struct` forward reader over `ReadOnlySpan<byte>`. `ReadBoolean/ReadInt32/ReadInt64/ReadSingle/ReadString`, `SkipString`, `Skip(int)`. Mirrors `BinaryWriter` encoding exactly (little-endian primitives; 7-bit-encoded length + UTF8 for strings). No heap allocation except the strings `ReadString` returns. |
| `DeserializeCells` rewrite + projected decode | `RowSerializer.cs` (modify) | Reimplement `DeserializeCells` on `ByteSpanReader` (keep the `byte[]` signature; it wraps a span). Add `DecodeProjectedCells(ReadOnlySpan<byte> data, IReadOnlyList<Column> columns, ReadOnlySpan<bool> isProjected, Span<CellValue> dest)`: walk columns in order, decode where `isProjected[i]` into `dest[k++]`, else skip-advance. A single per-column-type switch drives both decode and skip so widths can never diverge. |
| Projected typed executor | `DataVoCompiledQuery.cs` (modify `ExecuteSelectTyped`) | Build the per-query projected schema + reused buffer once; resolve matching row ids; for index-resolved ids, stream each row (MVCC-checked) through `DecodeProjectedCells` → `CompiledRowReader` → mapper, with no dict. |
| Row-id resolver | `DataVoCompiledQuery.cs` (refactor of the finder) | `TryResolveMatchingRowIds(...)` returns the matching ids when an index path (compile-time tag → PK → single-column secondary) yields a non-empty set; returns "unresolved" to signal the **scan fallback**. Mirrors the exact control flow of `TryReadMatchingStoredRows` (empty/`IndexException` → fall through). |
| MVCC visibility (streaming) | `DataVoCompiledQuery.cs` / `StorageContext` | A per-row visibility check usable in the streaming loop, identical in effect to `ApplyTypedMvccVisibilityFilter` (snapshot → `EnsureRowVersionExists` → `IsVersionVisible`). Reuse the existing engine APIs; do not re-implement the rule. |

`CompiledRowReader` and the generator are **unchanged** — the reader already resolves columns by name; we back it
with the projected view.

## 5. Correctness invariants (load-bearing)

- **Wire-format fidelity.** `ByteSpanReader` must decode byte-identical to `BinaryReader` for every type — most
  critically `ReadString` (7-bit-encoded length prefix, then UTF8). Guarded by a round-trip test:
  `SerializeCells` → `ByteSpanReader` decode equals `BinaryReader`/`DeserializeCells` decode, across all cell types
  including multi-byte UTF8 and NULLs.
- **Exact skip-advance.** Skipping an unprojected column advances precisely past its encoded bytes (fixed widths;
  string = length prefix + bytes; vector = count + N×4). Decode and skip share one type switch so they can never
  diverge; a dedicated test projects a subset of a row with mixed types before/after the projected columns and
  asserts the projected values are correct.
- **MVCC parity.** Visibility is checked per row *before* decoding, with the same outcome as the dict path's
  `ApplyTypedMvccVisibilityFilter` under a transaction snapshot. Guarded by a snapshot-visibility parity test.
- **Scan fallback.** When no index path resolves (the where column is unindexed and not the PK), the typed path
  falls back to the existing full-decode finder (`TryReadMatchingStoredRows`) + `CompiledRowReader` — correct,
  unchanged behavior. Projection pushdown applies to the index/tag/PK-resolved paths (the common compiled-query
  case, and the benchmark regime). This is an explicit scope boundary, not a gap.
- **Disk mode.** `ReadRow` on disk still allocates the row `byte[]` (poolable; a later lever). The span reader +
  projection + streaming already cut decode and the dict there too; the full near-zero win lands in-memory.

## 6. Scope

**In:** `ByteSpanReader`; `DeserializeCells` rewritten on it (global removal of per-row `MemoryStream`/
`BinaryReader`); `DecodeProjectedCells`; the compiled typed path streams index/tag/PK-resolved rows with
projection pushdown + per-row MVCC + a reused buffer (no dict); scan fallback preserved.
**Out:** the dict `Deserialize` (interpreted-query path) keeps its current impl (could adopt `ByteSpanReader`
later — noted, not done); disk read-buffer pooling; projection pushdown for the scan path; the Layer-2 param/key
dicts (separate, 5%).

## 7. Success criteria

- The compiled typed path's per-row allocation drops to ≈ projected strings + result record: the ~1,456 B/row
  deserialization slice is largely eliminated (target ≥ ~1,000 B/row reclaimed on the wide-row tagged regime),
  far below the dict path.
- Result parity: streaming projected path returns results identical to the dict path (all existing
  `SelectManyTyped`/`SelectSingleTyped` + source-generated tests stay green), including NULLs and a NULL-into-
  nullable projection.
- MVCC parity under a snapshot.
- `ByteSpanReader` decodes byte-identical to `BinaryReader` (round-trip test).
- 0 new warnings, AOT-clean (span reader + ref struct + typed `CellValue`; no reflection), full suite green.

## 8. Testing

1. **`ByteSpanReader` round-trip** — `SerializeCells` then decode via `ByteSpanReader` equals the `BinaryReader`
   decode for every type, multi-byte UTF8 strings, NULLs, and an explicit skip test.
2. **Projection pushdown** — a row with mixed types where only a subset is projected (projected columns
   interleaved with skipped ones, incl. skipped strings) decodes the projected values correctly; unprojected
   columns are never materialized.
3. **Typed parity** — streaming projected path == dict path across types + NULL + wide rows (existing typed +
   source-generated suites must stay green).
4. **MVCC parity** — under a transaction snapshot, streamed visibility matches the dict path.
5. **Allocation proof** — per-row differencing: typed path per-row falls below a tight budget and ≫ below the
   dict path (the deserialization slice gone).
6. **Full suite + AOT smoke** green.

## 9. Risks

- **Wire-format drift** — `ByteSpanReader` must track `BinaryWriter`'s exact encoding (esp. 7-bit string length);
  mitigated by the round-trip test (#1) and reusing one type switch for decode+skip (#2).
- **MVCC regression** — streaming must not change visibility; mitigated by reusing the engine's existing rule and
  the parity test (#4).
- **Finder refactor regression** — `TryResolveMatchingRowIds` must preserve the tag/PK/secondary/scan control
  flow; guarded by the existing `CompiledAccessPathTests` + `CompiledQueryRuntimeTests` staying green.

## 10. Bottom line

A shared allocation-free `ByteSpanReader` removes the per-row stream objects globally; the compiled typed path
decodes only the columns the query projects (skip-advancing the rest), checks MVCC before decoding, and streams
each row straight to the mapper through a reused buffer — no `StoredRow`, no dict. The typed read drops to its
theoretical minimum (projected strings + record), with the dict path and interpreted queries untouched and the
scan path preserved.
