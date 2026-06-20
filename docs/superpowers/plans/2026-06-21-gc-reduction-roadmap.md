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

**Macro impact (complex-vip, datavo, 50k):** GC **428.5 → 259.4 MB (−39.5%)**, total time
**698.8 → 322.6 ms (−54%, 2.2× faster)**, p99 **0.023 → 0.008 ms**. The cache also removed 5 *locked*
XML catalog walks per insert, hence the large throughput gain. Full GC trajectory:
552.9 → 489.6 → 428.5 → **259.4 MB (−53% from program start)**.

**Slice 4 P1 typed-insert measurement (2026-06-22, after `InsertTypedRow` typed normalization +
typed serialization):** complex-vip, DataVo-only, 10k baseline + 50k live ticks measured
**258.8 MB GC**, **376.7 ms** total, p50 **0.0058 ms**, p99 **0.0104 ms**. This is only a marginal
macro GC movement from the prior **259.4 MB** checkpoint; the expected large drop did not materialize
at macro level in P1, so the remaining allocation needs the P2 typed-read migration/profiler pass rather
than another insert-only change.

**Slice 4 P2 typed-read measurement (2026-06-22, after typed read APIs + hot reader migration):**
complex-vip, DataVo-only, 10k baseline + 50k live ticks measured **260.4 MB GC**, **393.6 ms** total,
p50 **0.0061 ms**, p99 **0.0122 ms**. Focused reactive seed allocation improved
**877,176 → 646,312 bytes** for a 1,000-row seed, but the macro benchmark remains essentially flat;
the remaining macro allocation is therefore not explained by the typed read candidates migrated in P2.

**Slice 4 P4 measurement (2026-06-22, after typed read materialization + dropping the dictionary
adapter from the compiled-query/Select read paths):** complex-vip, DataVo-only, 10k baseline + 50k
live ticks, 3 runs **258.9 / 260.2 / 260.5 MB GC** (mean ≈ 259.9), total ≈ **386–415 ms**, p99
**0.009–0.012 ms** — flat vs the 259.4 MB checkpoint (no regression). Macro-derived overall per-tick
≈ **5,440 B/tick** (MB·1048576/50000), also flat. complex-vip is insert/reactive-dominated and does
not exercise the read-scan path P4.1 improved (typed ordinal/key filter → non-matching scanned rows
skip dict materialization entirely), so the macro is expected flat; P4.1's allocation win lands on
read-scan-heavy workloads, not this benchmark.

**Step 2 — COMPLETE (typed storage, P0–P4).** Typed `CellValue`/`StoredRow` serialize/insert/read,
shared dict-parity `IndexKeyEncoder` typed key extraction (fixed a latent P1.2 numeric-key divergence),
VECTOR ALTER parser, and removal of the dictionary adapter from internal read paths; public results and
WAL/TransactionContext durable JSON stay dictionary boundaries. Full suite 999/999.
**Honest macro conclusion:** the migration is correct and parity-safe but did **not** move the
complex-vip macro below the 259.4 MB Step-1 plateau — the per-tick spike attributed 3,655 B/tick to
storage-write+validation, yet typing those paths did not translate into macro GC reduction (P1, P2, P4
all flat at ~259–260 MB). The remaining macro allocation is therefore **not** in the storage
(de)serialization/materialization paths but elsewhere on the per-tick insert→capture→deliver→requery
cycle (leading hypothesis: MVCC `RowVersion` objects per insert + reactive requery). The fine-grained
per-tick bucket profiler was a temporary instrumented spike removed after Step 1; it was not
reconstructed for P4 because P4 changed the read-scan path, not the insert bucket it measured (a
confirmatory null result). **Next allocation target should be re-profiled fresh against MVCC/version
churn**, not the now-typed storage path.

### Phase 7 — Pipeline & Serializer Optimization (Slice 5) — COMPLETE

Re-profiled the per-tick insert→capture→deliver cycle fresh (2026-06-22) and **disproved the MVCC
hypothesis**: per-tick ≈ 4,680 B, of which MVCC was only **220 B (4.7%)** and the reactive seed bridge
≈ 0/tick. The real hogs were the storage-write serializer (per-row `MemoryStream`/`BinaryWriter` +
`GetTableColumns`/`List` churn + a redundant `StoredRow` clone), the **dual after-image** in change
capture (eager dict *and* `TypedRow` clone), residual per-call framework (lock scope + insert service),
and constraint-validation scaffolding. Slice 5 attacked those.

- **P1 — serializer & clone:** thread-local pooled stream in `RowSerializer.SerializeCells`
  (wire-identical); hand owned cells to `StoredRow.FromOwnedCells` (drop redundant clone).
- **P2 — framework & scaffolding:** single-row typed insert path (no per-row `List`/column refetch);
  `SnapshotLockScope` → `readonly struct` (no per-tick alloc/boxing); reuse one `InsertRowService` per
  context; retained table write-lock (lock framework 432 → 64 B/op).
- **P3 — validation & capture fast-paths:** skip the validation `List`/`HashSet`/dict scaffolding for
  constraint-free tables; `TypedRow.FromOwnedCells` no-clone factory; **collapse the dual after-image** —
  `RowChange.After` is materialized lazily from `TypedAfter` (and the `StoredRow`'s owned immutable cells
  are shared with the captured image), so the borrowed typed lane never builds the owned dict.

**Per-insert measurement (durable `InsertAllocationGuardTests`):**

| Warm insert path | Before Slice 5 | After Slice 5 | Note |
|---|---:|---:|---|
| No subscriber (capture off) | ~4,680 B/insert | **~1,090 B/insert** (−77%) | serializer + clone + framework + constraint-free validation |
| VIP borrowed subscriber (capture on) | ~2,551 B/insert | **~2,370 B/insert** (−~180 B) | dual after-image collapse only |

The capture-off path saw the large reduction. The capture-on VIP path improved only modestly because
its per-insert cost is **dominated by inherent retained/dispatch allocation** (storage row retention,
MVCC version, `ChangeSet`/`RowChange`, per-drain registry snapshots), not the after-image — the after-image
collapse is the ~180 B slice it could remove. The plan's borrowed estimate (~1,900 / ~600 B) assumed a
query shape (`SELECT Id, Stake`) that routes to the non-borrowed `ReactiveSubscription`; the corrected
test uses the real VIP shape (`VipExposureReactiveQuery`), the only production borrowed operator that
reads the typed lane. Full suite 1005/1005.

**Macro benchmark (complex-vip, user-run 2026-06-22):** DataVo **143.5 MB** GC, **237 ms** total,
**p99 0.007 ms**. Same environment, same workload: DuckDB 131 MB / **103 s**, SQLite 115 MB / **290 s**.
DataVo is now within ~10–25% of the native engines' memory footprint while finishing **~435× faster than
DuckDB and ~1,200× faster than SQLite**, at sub-10µs p99 latency. Full program GC trajectory:
552.9 → 489.6 → 428.5 → 259.4 → **143.5 MB (−74% from program start)**.

- **Plan:** [`2026-06-22-slice5-pipeline-serializer-plan.md`](2026-06-22-slice5-pipeline-serializer-plan.md)
- **Design:** [`../specs/2026-06-22-slice5-pipeline-serializer-design.md`](../specs/2026-06-22-slice5-pipeline-serializer-design.md)

---

## Program status — PAUSED (2026-06-22)

**The GC-reduction program is officially paused. Goal achieved: DataVo competes with native C/C++
engines on memory (143.5 MB vs DuckDB 131 / SQLite 115) while running 2–3 orders of magnitude faster.**
Further GC micro-optimization has poor ROI versus the next strategic priority.

Phases 5–7 are complete: Slice 3 (operator fast-lane), Slice 4 (typed storage P0–P4), and Slice 5
(pipeline & serializer optimization). Slice 5 re-profiled the per-tick cycle, **disproved the MVCC
hypothesis** (MVCC was only 4.7% of per-tick), cut the capture-off warm insert path ~77%
(~4,680 → ~1,090 B/insert), and collapsed the dual after-image on the capture-on VIP path. The VIP
capture-on per-insert (~2,370 B) is now **dominated by inherent retained/dispatch allocation** (storage
row retention, MVCC version per insert, `ChangeSet`/`RowChange`, per-drain registry snapshots) — the
accepted managed-.NET noise floor.

**Explicitly NOT planned (deferred indefinitely — do not open a "Slice 6"):** per-drain dispatch churn
(pooling snapshot arrays / `ChangeSet`/`RowChange`), the MVCC version object per insert, and the deferred
typed-arrangement follow-ups (Join/TopK pooling, RecursiveCte closure rework). These remain documented
here as the known remaining buckets should the program ever resume.

**Next strategic priority → Native AOT + Source-Generated Bindings.** A C# Source Generator to eliminate
all remaining runtime reflection, emit zero-allocation native entity bindings, and make the DataVo engine
100% Native AOT compatible. Tracked in the program-level roadmap
([`2026-06-19-advanced-features-roadmap.md`](2026-06-19-advanced-features-roadmap.md)); its own
`brainstorm → spec → plan → implement` cycle to follow.
