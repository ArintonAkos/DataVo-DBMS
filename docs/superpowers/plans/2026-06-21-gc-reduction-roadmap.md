# GC Reduction — Epic Roadmap

**Program goal:** Drive `complex-vip` benchmark allocations down toward the ultimate
0 MB target by progressively replacing boxed/dictionary cell currency with the borrowed
`Span<CellValue>` / `ref struct` fast lane across the reactive, insert, and storage paths.

**How this is tracked:** The epic is delivered as dated per-slice implementation plans in
this directory. This file is the single roadmap that records slice/phase status and links
to the detailed plans. Each slice has its own plan + design doc with task-level checkboxes.

**Status legend:** `COMPLETE` · `IN PROGRESS` · `PLANNED`

---

## Completed

### Slice 1 — Capture-Clone Fix + Fast-Lane Vertical Slice — COMPLETE

Single-allocation capture clone plus a materialization-free borrowed delivery path
(`SubscribeZeroAlloc` → `QueryChangeRef`) wired through the first migrated operator
(`VipExposureReactiveQuery`) and adopted by the benchmark.

- **Measured:** complex-vip `552.891 MB → 489.554 MB` (≈ −11.5%).
- **Plan:** [`2026-06-21-gc-reduction-slice-1.md`](2026-06-21-gc-reduction-slice-1.md)
- **Design:** [`../specs/2026-06-21-gc-reduction-slice-1-design.md`](../specs/2026-06-21-gc-reduction-slice-1-design.md)

### Slice 2 — Upstream Insert (Typed Insert Fast Lane) — COMPLETE

Strict typed single-row insert fast lane (`DataVoContext.InsertTyped`) that attaches a
typed `RowChange` after-image (`TypedRow`) and lets `VipExposureReactiveQuery` consume
order inserts by ordinal without boxing. The dictionary `BulkInsert` path is retained
unchanged as the validation fallback.

- **Scope:** typed change image, public `InsertTyped` API + strict insert service entry,
  VipExposure typed `RowChange` consumption, benchmark runner adoption, and Task 5
  allocation/regression tests (`DataVo.Tests/E2E/InsertTypedTests.cs`).
- **Measured:** ~75 MB further reduction reported against the `489.554 MB` Slice 1
  baseline (final figures recorded in the Slice 2 plan / implementer report).
- **Plan:** [`2026-06-21-typed-insert-fast-lane.md`](2026-06-21-typed-insert-fast-lane.md)
- **Design:** [`../specs/2026-06-21-typed-insert-fast-lane-design.md`](../specs/2026-06-21-typed-insert-fast-lane-design.md)

---

## Planned

### Phase 4 — Technical Debt: The Generator Release Hang — CANNOT REPRODUCE

**Reported symptom:** the `DataVo.Generators` Roslyn source generator hangs during
`Release` builds (timing out after 5 minutes) when encountering the new
`ReadOnlySpan<CellValue>` / `ref struct` signatures introduced in Slice 2, suspected to be
an infinite loop in the syntax walker.

**Investigation (2026-06-21, .NET SDK 10.0.103, Microsoft.CodeAnalysis.CSharp 4.14.0):**
the symptom does not reproduce, and the suspected mechanism is structurally impossible.

- Three clean `Release` builds (after deleting `obj/bin`) all succeed in ~1–3s:
  `DataVo.Tests` (2.35s), `DataVo.Generators.Tests` (~1.0s), full `DataVo.sln` (2.83s).
- The entire `DataVo.Generators` source contains **zero** loop constructs
  (`while`/`for`/`do`/`goto`) — there is no loop that could spin.
- The generator's syntax predicate (`DataVoQueryGenerator.cs:22-24`) only selects
  `partial` methods carrying attribute lists, then filters to
  `[DataVo.Core.CompiledQueries.DataVoQueryAttribute]`. It never references
  `ReadOnlySpan`, `ref struct`, or `CellValue`.
- `InsertTyped` is a non-`partial`, non-attributed method in `DataVo.Core`, which does not
  reference the generator; the generator never inspects its span parameters.

**Disposition:** closed as not-reproducible; no generator code changed. To reopen, attach a
concrete repro (exact build command + environment/CI image, or a `[DataVoQuery] partial`
method sample that triggers the hang).

### Phase 5 — Fast Lane Expansion (Slice 3) — COMPLETE (4 of 4 operators migrated)

Migrate the remaining operators onto the borrowed `IBorrowedReactiveQuery` + `QueryChangeBuilder`
fast lane (previously only `VipExposure` used it). Design + per-operator detail:
[Slice 3 design](../specs/2026-06-21-gc-reduction-slice-3-design.md).

- **Aggregate (GroupBy)** — ✅ done. COUNT/SUM/AVG **0-byte** steady state; MIN/MAX verified
  already 0-byte (de-boxing declined — no benefit).
- **TopK (Sort)** — ✅ done as an allocation-light reduction: **window-size-independent**,
  ~3920→~904 B/iter at a 50-row window (~77%). True 0-byte needs entry/node pooling (deferred).
- **RecursiveCte** — ✅ emit-side (borrowed delivery + parity); deep purification **deferred**
  (a retraction recomputes the whole closure, so 0-byte is structurally infeasible here).
- **Join** — ✅ done. Emit-side + parity; Step 2 allocation-light reduction ~4384→~3176 B/iter
  (~28%) by skipping the per-candidate `BuildContext` (no-WHERE) + reusing delta/candidate
  containers. Deeper typed-arrangement work deferred; not 0-byte (per-row arrangements inherent).

Measurements were taken, not assumed; strict 0-byte holds for Aggregate, with TopK/Join/RecursiveCte
honestly scoped to what their structures allow (allocation-light / emit-side).

**Macro benchmark (complex-vip, datavo, 50k — Slice 1+2 path; does NOT exercise these generic
operators):** 552.9 MB (pre-program) → 489.6 (Slice 1) → **428.5 MB** now (Slice 2 typed insert),
≈ −22.5% overall; ~699 ms total, p99 0.023 ms. Slice 3's generic-operator wins are captured by the
per-operator allocation tests, not this macro number.

### Phase 6 — Deep Storage Purification (Slice 4) — PLANNED (profiled)

Rewrite the lowest-level storage engine, index B-Trees, and byte-serialization to accept
`Span` directly. This will eliminate the final internal `Dictionary` fallback currently
retained for validation, achieving the ultimate 0 MB allocation goal.

**Profile evidence (2026-06-22, complex-vip per-tick, VipExposure path):** the remaining
allocation is overwhelmingly the **storage-write + validation** path inside `InsertTyped`,
not capture or delivery —

| Bucket | B/tick | Share |
|---|---:|---:|
| storage-write + validation | **7,418** | **~89%** |
| change-capture (`TypedRow` copy) | 784 | ~9% |
| delivery (registry per-drain snapshots) | 151 | ~2% |

So Slice 4 targets the insert/validation/storage path: per-insert catalog lookups, LINQ-built
HashSets/Dictionaries (`columnNames`, `foreignKeysByAttribute`, accepted-key sets), the
`row[i].ToObject()` dict **re-boxing** of typed cells, and the `normalized`/storage row dicts +
MVCC version (`InsertRowService.InsertTypedRow`). Delivery is already negligible (the Slice 1/3
fast lane); capture is a minor follow-up.

**Step 1 — DONE: validation-metadata cache.** `EngineCatalog.GetTableValidationMetadata` computes
the per-table keys/indexes/columns + `columnNames`/`foreignKeysByAttribute` once and caches them by
schema version (both insert paths use it; the cache also drove a correctness fix — `CREATE`/`DROP
INDEX` now bump the schema version, so the version is a true schema version). Measured per-tick:
storage-write+validation **7,418 → 3,655 B/tick (−51%)**, total **8,353 → 4,590 (−45%)**; capture
784 and delivery 151 unchanged. Full suite green.

**Step 2 — next: typed storage.** The remaining ~3,655 B/tick is the `row[i].ToObject()` dict
re-boxing + `normalized`/storage row dicts + MVCC version. Make the storage/validation path accept
typed `CellValue` rows directly (no per-insert dict materialization). Deeper and riskier (shared
storage format) — measure-then-reduce.

---

## Next action

Phase 5 (Slice 3) is **complete** — all four operators migrated onto the borrowed fast lane. Next
candidates: **Phase 6 (Slice 4) deep storage purification**, or the deferred deeper-purification
follow-ups (typed arrangements for Join/TopK, RecursiveCte closure rework, the `DateOnly`
borrowed-currency gap) — sequence by measured impact.
