# GC Reduction — Slice 3 (Fast-Lane Expansion) Design

**Status:** Draft (pending review)
**Epic:** [GC Reduction Roadmap](../plans/2026-06-21-gc-reduction-roadmap.md) — Phase 5 / Slice 3
**Date:** 2026-06-21

## Goal

Migrate the remaining reactive operators onto the existing borrowed zero-allocation
fast lane (`IBorrowedReactiveQuery` + the `QueryChangeBuilder` "arena"), so that
materialization-free delivery — proven for `VipExposureReactiveQuery` in Slices 1–2 — is
available for general query shapes.

Today only `VipExposureReactiveQuery` implements `IBorrowedReactiveQuery`. Every other
operator still produces an owned `QueryChange` (boxed `Dictionary` rows + `List`s) on every
dispatch.

## Scope

Migrate four operators, in this order (easiest → hardest, to prove the blueprint before the
hard cases):

1. **`AggregateReactiveQuery`** (GROUP BY) — first; state is already primitive and it is the
   general form of the proven `VipExposure` blueprint.
2. **`TopKReactiveQuery`** (ORDER BY + LIMIT) — stands in for "Sort"; there is no dedicated
   ORDER-BY-only reactive operator.
3. **`JoinReactiveQuery`** (INNER/LEFT/RIGHT/FULL) — the "final boss"; deepest boxing.
4. **`RecursiveCteReactiveQuery`** (WITH RECURSIVE) — last, with a **0-byte feasibility
   decision** (see Risks): its algorithm recomputes the closure from scratch on retraction,
   so strict zero-alloc may require an algorithmic redesign rather than a migration.

### Out of scope (YAGNI)

- New benchmark scenarios. Success is proven by per-operator unit tests (see below), not by a
  new `complex-vip`-style macro benchmark — the existing benchmark does not exercise these
  generic operators, and maintaining bespoke scenarios is high-overhead.
- Changing the owned `Subscribe(Action<QueryChange>)` path or the legacy `Apply` semantics.
- Any algorithmic change to recursive-CTE maintenance (deferred to its feasibility decision).
- `MIN`/`MAX` multiset de-boxing is a **Step 2 sub-goal** for Aggregate, not a separate slice.

## Success criteria (per operator)

Each migrated operator must satisfy both, enforced by unit tests:

1. **Parity** — the borrowed path (`ApplyInto` → `QueryChangeRef`) must produce output
   strictly identical to the owned path (`Apply` → `QueryChange`) for any input sequence
   (seed + arbitrary insert/update/delete batches), including added/updated/removed sets and
   per-cell values.
2. **Zero-alloc** — steady-state dispatch on the borrowed fast lane must allocate ~0 bytes,
   measured with the existing `GC.GetAllocatedBytesForCurrentThread()` template (see
   `DataVo.Tests/Reactive/SubscribeZeroAllocTests.cs`,
   `DataVo.Tests/Reactive/VipExposureBorrowedTests.cs`).

"Steady state" = a workload whose maintained structures do not grow (e.g., repeated updates
to existing groups/rows, or constant-size churn), so amortized one-time buffer growth is
excluded — matching how the existing allocation tests are written.

---

## The Migration Blueprint: `Apply` → `ApplyInto` delegation

Every migrated operator adopts the `VipExposureReactiveQuery` shape so the owned and borrowed
paths can never diverge (the owned path is *defined in terms of* the borrowed one):

```csharp
internal sealed class XxxReactiveQuery : IBorrowedReactiveQuery   // was IReactiveQuery
{
    private readonly ReactiveRowSchema _outputSchema;   // built once in the constructor
    private readonly QueryChangeBuilder _legacyBuilder;  // owns the owned-path arena
    private readonly CellValue[] _rowScratch;            // reused; length = _outputSchema.ColumnCount

    public ReactiveRowSchema OutputSchema => _outputSchema;

    // Owned path: build the borrowed delta into our own arena, then copy out. Behavior-identical
    // to the pre-migration Apply (same QueryChange shape and values) -> structural parity.
    public QueryChange Apply(IReadOnlyList<RowChange> changes)
    {
        _legacyBuilder.Reset();
        ApplyInto(changes, _legacyBuilder);
        return _legacyBuilder.Build().Materialize();
    }

    // Borrowed path: maintain state, then write rows into the caller-owned builder.
    public void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder)
    {
        // ... existing state-maintenance loop, unchanged ...
        ClassifyInto(/* touched / candidates */, builder);
    }
}
```

Key properties:

- `IBorrowedReactiveQuery` extends `IReactiveQuery`, so `Tables`/`Seed`/`Apply` keep working.
  Implementing it is what makes an operator eligible for the `SubscribeZeroAlloc` fast lane
  (Slice 1; non-implementing shapes throw `NotSupportedException`). The owned
  `Subscribe`/`Apply` path is unchanged.
- `Apply` delegating through `ApplyInto` + `Materialize()` makes the parity test structural:
  the owned output is literally a copy of the borrowed output.
- `ClassifyInto(...)` writes each output row into a reused `CellValue[] _rowScratch` (in
  `_outputSchema` column order) and appends via the builder's
  `AddAddedRow` / `AddUpdatedRow` / `AddRemovedRow` (and `AddUpdatedBeforeRow` where the
  operator emits update before-images — Aggregate and DISTINCT-style operators do not; Join
  and TopK do).

The `QueryChangeBuilder` arena reuses four flat `CellValue[]` buffers across deltas via
`Reset()`; growth is one-time and amortized, so steady-state building allocates nothing.

---

## Step-by-Step Implementation (always parity-green)

Each operator is migrated in **two ordered steps**, and the test suite stays green after each:

### Step 1 — Emit-side (wire the arena)

- Switch the interface to `IBorrowedReactiveQuery`; add `_outputSchema`, `_legacyBuilder`,
  `_rowScratch`, `OutputSchema`.
- Rewrite `Apply` as the delegation above.
- Add `ApplyInto` that runs the **existing, unchanged** state-maintenance loop, then calls a
  new `ClassifyInto(..., builder)` that writes `_rowScratch` rows into the builder instead of
  constructing `Dictionary`/`List`/`QueryChange`.
- **Outcome:** removes the output-side allocations (the per-row dicts, the 3–4 `List`s, the
  `QueryChange`). **Parity test passes.** Allocation is *reduced but not yet zero* — see
  Boxing Guardrails for the residuals that intentionally remain here.

### Step 2 — State purification (reach 0 bytes)

- Replace boxed internal state with typed `CellValue` storage and primitive accumulation.
- Eliminate the residual per-batch allocations (string keys, boxed aggregate results,
  per-call scratch).
- **Outcome:** the steady-state **zero-alloc test passes**; parity test still passes.

This ordering guarantees we always have a working, parity-correct intermediate (Step 1) before
attempting the riskier state rewrite (Step 2), and lets each step land as its own commit.

---

## Boxing Guardrails

### Residual boxing permitted in Step 1 (documented, temporary)

Step 1 deliberately leaves these allocations in place; they are removed in Step 2. They are
acceptable in Step 1 because that step is validated only by the **parity** test, not the
zero-alloc test:

- **`ComputeGroupKey` strings** (`AggregateReactiveQuery.cs:391`) — `string.Join` + LINQ
  `Select` + `Convert.ToString` per change. Still allocates one key string per row change.
- **Boxed `ComputeAggregate` results** (`:340`) — returns `object?` (boxed `long`/`decimal`),
  wrapped at the emit boundary via `CellValue.From(object?)`. The cell itself does not box
  (decimal/long are stored inline in `CellValue`), but reading the boxed accumulator result
  does. One box per aggregate column per emitted group.
- **Boxed group key values** (`_groupKeyValues`, `object?[]`, `:46`) — captured once per group
  (`CaptureGroupValues`, `:407`); read via `CellValue.From(object?)` at emit. Amortized at
  the group level, but still a boxed array.

Step 1 wraps all of the above with `CellValue.From(object?)` (the compatibility constructor)
so the emit path is correct and parity-green while the boxed state remains underneath.

### Step 2 exact goals (per de-boxing target)

- **Primitive accumulation (already true) — confirm & preserve.** `GroupState` already holds
  `long Count` and `Dictionary<string,long/decimal>` accumulators (`:32-39`); no boxed row
  copies are kept for `COUNT`/`SUM`/`AVG`. Step 2 must not regress this.
- **Typed group-key storage.** Replace `_groupKeyValues` (`object?[]`) with `CellValue[]` per
  group so emit reads typed cells with no boxing.
- **Direct aggregate emission.** Replace `ComputeAggregate` returning `object?` with a
  `WriteAggregate(spec, state, out CellValue)` (or equivalent) that produces a `CellValue`
  directly — no intermediate box. `SUM`/`AVG`/`COUNT` map to `CellValue.From(long/decimal)`.
- **Cached / non-allocating group identity.** Eliminate the `ComputeGroupKey` string
  allocation on the hot path: cache each group's key on first sight and reuse it, or replace
  the composed-string key with a non-allocating key strategy. Probing an existing group must
  allocate nothing.
- **Reused per-batch scratch.** `touched` becomes a reused `HashSet<string>` field cleared
  per `ApplyInto` (mirrors `VipExposure._touched`); `_rowScratch` is reused across emits.
- **`MIN`/`MAX` multiset — Step 2b: VERIFIED already 0-byte; de-boxing not pursued.**
  Measurement (`MinMaxDispatch_IsAllocationFree_OnSteadyState`) shows MIN/MAX steady-state
  dispatch already allocates **0 bytes**. Row values arrive pre-boxed (`object?`), so the
  multiset stores an existing reference and `CellValue.From(.Min/.Max)` reads it back — there
  is no per-op boxing to remove. The only MIN/MAX allocation is sorted-tree **node churn** on
  distinct-value workloads (not a bounded steady state), and de-boxing `object`→`CellValue`
  would **not** fix it (`SortedSet<CellValue>.Add` still allocates a node). De-boxing was
  therefore declined as a no-benefit refactor; the regression test is kept as a guard. Node
  churn, if ever needed, is a separate structural change (pooling), out of this slice.

---

## Per-operator notes

### 1. AggregateReactiveQuery (first; detailed above)

- Output schema order: `_groupOutputs` (group columns, in declared order) followed by
  `_aggregates` (aggregate outputs, in declared order). `_rowScratch` is written in that order.
- Emits **no** update before-image (`Classify` uses the 3-arg `QueryChange` ctor, `:187`), so
  `ClassifyInto` uses only `AddAddedRow`/`AddUpdatedRow`/`AddRemovedRow`. The
  `_emittedGroups` add/update/remove classification (`:169-184`) is preserved verbatim.
- Precompute the group-output → group-column index map in the constructor (today
  `BuildOutputRow` does `_groupColumns.FindIndex` per emit, `:307`).

### 2. TopKReactiveQuery (medium)

- Fully boxed today (`Entry.Row` dicts, `SortedSet<Entry>`, `_window` rebuilt every `Apply`).
- Step 2 needs: typed `CellValue[]` entry rows, a comparator over typed cells, a
  double-buffered window (swap, not realloc), projected scratch, and a cached identity.
- Emits update before-images → `ClassifyInto` uses `AddUpdatedBeforeRow` aligned with
  `AddUpdatedRow`.

### 3. JoinReactiveQuery (final boss)

- Deepest boxing: arrangements, string identities/join-keys (`Compose`), `BuildContext` dicts,
  per-row `Copy`. See the Join analysis in the conversation log; its migration follows the
  same blueprint but Step 2 is substantial (key redesign + WHERE-context de-boxing + typed
  arrangements). Emits update before-images.

### 4. RecursiveCteReactiveQuery (feasibility-gated)

- Algorithmically allocating: any retraction triggers a full from-scratch fixpoint recompute
  (`:168,231`), and `EmitDiff` rebuilds the entire projected output every batch (`:360`).
- Before migrating, decide whether strict 0-byte is achievable without an algorithmic rewrite.
  If not, this operator may move to its own slice. The blueprint still applies for the
  insertion-only steady state.

---

## Testing strategy

For each operator, add tests mirroring the existing borrowed/zero-alloc templates:

- **Parity test** — drive seed + randomized/representative insert/update/delete batches through
  both `Apply` (owned) and `ApplyInto` (borrowed, then `Materialize`/inspect), asserting
  identical added/removed/updated sets and per-cell values. Land this with **Step 1**.
- **Zero-alloc steady-state test** — warm up, then measure
  `GC.GetAllocatedBytesForCurrentThread()` across a steady-state dispatch loop and assert ~0
  bytes. Land this with **Step 2**.

Verify per operator:

```
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter <Operator>BorrowedTests
```

## Risks & open questions

- **RecursiveCte 0-byte feasibility** — likely needs an algorithmic redesign; gated by an
  explicit decision before its migration (see scope).
- **Group-key de-boxing strategy (Aggregate Step 2)** — caching the existing composed string
  vs. a structural non-allocating key. Caching is the lower-risk default; the structural key
  is a later optimization if profiling shows the cached string still dominates.
- **`MIN`/`MAX` de-boxing size** — may split into Step 2b within the Aggregate migration.
- **"Steady-state" workload definition in tests** — must keep maintained structures
  non-growing so amortized buffer growth is excluded, consistent with existing tests.

## Future Improvements

### Known limitation: borrowed currency does not support `DateOnly` (and other non-scalar types)

Routing the owned aggregate path through the `CellValue` arena constrains group-key and aggregate
output to the borrowed currency's scalar set (`bool`/`int`/`long`/`double`/`decimal`/`string`).
A reactive `GROUP BY`, `MIN`, or `MAX` over a `DATE` column (`DateOnly`) — or any other type
outside that set — now throws from `CellValue.From(object?)`, whereas the pre-migration owned path
carried it as a boxed `object?`. No current test exercises this shape (the only `DATE` aggregate is
a non-reactive `Execute` query), so nothing regresses today, but it is a real behavior change for
that untested shape.

**Plan:** extend `CellValue` with `DateOnly` (and audit the full SQL type set) so the borrowed
currency is type-complete. **Prioritized after Step 2b (MIN/MAX de-boxing)**, since both touch the
`CellValue` surface and the MIN/MAX path is where date extrema would first appear.

## Delivered status (as of 2026-06-22)

| Operator | Status | Result |
|---|---|---|
| **Aggregate** (GROUP BY) | ✅ Step 1 + Step 2 | COUNT/SUM/AVG **0-byte** steady state; MIN/MAX **verified already 0-byte** (Step 2b declined — no benefit) |
| **TopK** (ORDER BY + LIMIT) | ✅ Step 1 + Step 2 (reduction) | **allocation-light, window-size-independent** — ~3920→~904 B/iter at k=50 (~77%); typed entries not pursued (parity-risky non-boxing comparer); true 0-byte needs pooling (deferred) |
| **RecursiveCte** (WITH RECURSIVE) | ✅ Step 1 (emit-side) | borrowed delivery + parity; **Step 2 formally deferred** — a retraction recomputes the whole closure, so 0-byte is structurally infeasible in this slice |
| **Join** (INNER/LEFT/RIGHT/FULL) | ⏳ pending | the "final boss"; deepest boxing — to be migrated next (its own focused effort) |

All four operators now route through `SubscribeZeroAlloc` once migrated; the owned
`Subscribe`/`Apply` path and the full reactive test suite (incl. the IVM oracle property
tests) remain green. Each migrated operator has borrowed parity tests; Aggregate and TopK
additionally have allocation tests (0-byte and allocation-light respectively).

**Honest scope note:** the strict "0-byte steady state" criterion is met for Aggregate; for
TopK it is an allocation-light reduction (per-entry/sorted-node cost is inherent) and for
RecursiveCte it is emit-side parity only (closure recompute is inherent). These were
measured, not assumed.

## Definition of done (Slice 3)

- Operators 1–3 (and 4, subject to its feasibility decision) implement
  `IBorrowedReactiveQuery` via the blueprint.
- Each has a passing parity test (post Step 1) and a passing zero-alloc steady-state test
  (post Step 2).
- The owned `Apply`/`Subscribe` path and all existing tests remain green.
- The roadmap's Slice 3 entry is updated with the result.
