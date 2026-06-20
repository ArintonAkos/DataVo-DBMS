# DataVo Advanced Features Roadmap

> **Status:** Roadmap / sequencing document — *not* yet a set of implementation specs.
> Each work item below gets its own `brainstorm → spec → plan → implement` cycle later.
>
> **Date:** 2026-06-19
> **Backing research:** [`docs/research/feature-bibliography.md`](../../research/feature-bibliography.md)
> **Depends on in-flight work:** [`2026-06-19-runtime-observability-and-compiled-queries.md`](./2026-06-19-runtime-observability-and-compiled-queries.md) — being finalized by another agent.

This document turns the ten brainstormed feature ideas into a prioritized, dependency-aware
roadmap, after a literature review narrowed and de-risked the scope.

---

## ⭐ Current strategic priority (set 2026-06-22): Native AOT + Source-Generated Bindings

The GC-reduction program is **complete and paused** — DataVo now competes with native C/C++ engines on
memory (complex-vip: **143.5 MB** vs DuckDB 131 / SQLite 115) while running **~435× faster than DuckDB
and ~1,200× faster than SQLite** (237 ms, p99 0.007 ms). See
[`2026-06-21-gc-reduction-roadmap.md`](2026-06-21-gc-reduction-roadmap.md). Further GC micro-optimization
has poor ROI; the next major architectural leap is **Native AOT readiness**.

**Goal:** a C# Source Generator that (1) eliminates all remaining runtime **reflection** on the hot
paths, (2) emits **zero-allocation native entity bindings** (typed materialization / parameter binding
with no boxing or dictionaries), and (3) makes the DataVo engine **100% Native AOT compatible**
(trim-safe, no dynamic codegen, AOT-clean across the public surface: ADO.NET, EF Core preview, WASM).

**Relationship to existing items:** this initiative **elevates and subsumes** the source-generator work
already sketched as W7 (schema-aware compiled-query verification) and W8 (zero-allocation `Span<T>`
projection), and builds on the in-flight `DataVo.Generators`. Those become sub-tracks of the AOT push
rather than independent Wave-3 items.

**Status:** awaiting its own `brainstorm → spec → plan` cycle (not yet started). The waves below remain
the backlog for feature work that resumes after the AOT initiative.

---

## Guardrails: what this roadmap does NOT change

The core product is unchanged. Embedded SQL engine, vector/HNSW search, ADO.NET, EF Core
preview, and WASM all stay exactly as they are. **Every item here is additive** — a differentiator
layered on top of a working engine. Nothing here is required for DataVo to ship.

If an item ever forces a change to the core SQL pipeline or storage contract, that is a signal to
re-scope the item, not the engine.

---

## What the research changed (net: smaller, safer)

| Original idea | Change after research | Source |
| --- | --- | --- |
| Reactive queries over arbitrary SQL | **Ship linear-only V1** (single-table WHERE; no joins/aggregates). Joins/aggregates need materialized operator state → opt-in V2. | DBSP (VLDB 2023, Best Paper) |
| Memory-mapped row storage (`.dat` via mmap) | **Do not use mmap.** Multithreaded + write-heavy + latency-sensitive is the exact anti-pattern. Reframe as a vmcache-style buffer pool, or defer. | Crotty/Leis/Pavlo (CIDR 2022); vmcache (SIGMOD 2023) |
| 4 separate features (reactive, snapshot-diff, time-travel, branch-diff) | **Recognized as one shared "delta/change-set" primitive** seen four ways. Build the primitive once. | DBSP Z-set algebra |

### Research trust tiering (per the "validate authenticity" requirement)

- **Load-bearing (drive design):** DBSP (McSherry/Tannen/Budiu; VLDB 2023 Best Paper; productized by Feldera), mmap paper (Pavlo/Leis/Crotty; CIDR 2022; public benchmark repo), vmcache (Leis; SIGMOD 2023), Disruptor (LMAX industry standard), Intermittent Query Processing (PVLDB 2019), Photon/Velox (Databricks/Meta production; SIGMOD 2022).
- **Illustrative only (NOT relied on):** master's theses and brand-new 2025/26 papers with few citations (BranchBench, CloudyBench, Streaming Democratized, Orsten, Tran, Kučera). Used as evidence the problem space is active, never as a design basis.
- Note: the `citations: 0` values from the paper-search tool are a scraper artifact, not real citation counts.

---

## The architectural backbone: one delta primitive (W1)

Four features are the same primitive viewed differently:

```
                 ┌─────────────────────────────┐
                 │  W1: Change/Delta primitive  │
                 │  (signed change-sets +       │
                 │   per-table change log)      │
                 └──────────────┬──────────────┘
        ┌────────────┬──────────┼───────────┬──────────────┐
        ▼            ▼          ▼            ▼              ▼
   W2 Reactive   W3 Snapshot  W4 Time-    W5 COW        (branch-diff =
   queries V1    diff (net-   travel      branching      W5 + W3)
   (linear)      code)        (AS OF)
```

Building W1 well unlocks W2–W5 instead of four bespoke subsystems. This is the single most
important sequencing decision in the roadmap.

---

## Work items

Effort: **S** ≈ days, **M** ≈ 1–2 weeks, **L** ≈ 3–4 weeks, **XL** ≈ 5+ weeks (single-developer, rough).

### W1 — Change/Delta primitive `[foundational]`
- **Goal:** A signed change-set representation (insert = +1, delete = −1, update = −old/+new) and a per-table change log, modeled on DBSP Z-sets. Internal API only; no SQL surface yet.
- **Why:** Foundation for W2/W3/W4/W5.
- **Depends on:** existing transaction/WAL layer.
- **Effort:** M · **Risk:** medium (get the representation right once).
- **Spec status:** not started.

### W2 — Reactive queries V1 (linear subset) `[cross-cutting]`
- **Goal:** `db.Subscribe(sql, onChanged)` for single-table `WHERE` (comparisons, AND/OR). Push committed deltas through the predicate; fire after COMMIT only; async callbacks; hard subscription cap; no writes inside callbacks.
- **Why:** Headline differentiator for game dev / HFT / reactive UI.
- **Research:** DBSP (linear operators are stateless, `Q^Δ = Q`); Noria (partial state, bounding cost); Shared Arrangements (many-subscription sharing).
- **Depends on:** W1; existing diagnostics (already shipped) for observability.
- **Staged delivery — all additive: same final capability, no API break, no rework between stages:**
  - **W2a — linear (Group A spec):** single-table `WHERE` (comparisons, AND/OR, IS NULL). Stateless. Effort **M**.
  - **W2b — single-table aggregates + top-K:** `COUNT`/`SUM`/`MIN`/`MAX` with `GROUP BY`, and `ORDER BY`+`LIMIT` maintained views. Moderate per-group / heap state. Effort **M–L**.
  - **W2c — joins / multi-table:** materialized indexed inputs (DBSP arrangements). Effort **L–XL**. Build only on real demand.
- **Why staged, not all at once:** each stage ships usable value, each builds on W1 + the prior stage with no rewrite, and the expensive/risky joins (W2c) come last — when we actually know they're needed.
- **Risk:** medium for W2a (callback safety/reentrancy, not the algebra); rising with W2b/W2c (operator state).
- **Spec status:** W2a spec written (Group A); W2b and W2c are tracked phases, not yet specced.

### W3 — Snapshot diff (netcode) `[game dev]`
- **Goal:** `db.Diff(a, b)` → compact binary delta between two states; apply-delta on the other side.
- **Research:** DBSP (delta = `𝒟` of states); games delta-encoding literature (illustrative).
- **Depends on:** W1.
- **Effort:** S–M · **Risk:** low–medium.
- **Spec status:** not started.

### W4 — Time-travel `AS OF TICK` `[game dev]`
- **Goal:** `SELECT ... AS OF TICK n`; query a past state for replay/debug/anti-cheat. Tick-stamped change log + retention policy.
- **Research:** ImmortalDB / transaction-time (Lomet); version compression for retention cost.
- **Depends on:** W1 (change log is the history).
- **Effort:** M · **Risk:** medium (retention/storage growth is the real cost).
- **Spec status:** not started.

### W5 — Copy-on-write branches `[testing parity]`
- **Goal:** `using var b = db.Branch();` — instant isolated fork; discard on dispose. Combined with W3 gives branch-diff testing.
- **Research:** Vive la Différence (DB branching + diff testing), BranchBench/Neon COW (illustrative).
- **Depends on:** storage engine COW support; pairs with W3.
- **Effort:** M–L · **Risk:** medium–high (storage forking correctness).
- **Spec status:** not started.

### W6 — Chaos storage mode `[testing parity]`
- **Goal:** `StorageMode.Chaos` wrapping Disk; injects partial writes, disk-full, latency spikes for resilience tests.
- **Research:** Chaos Engineering for Databases (CWI); Rosenthal & Jones (principles).
- **Depends on:** Disk storage engine (stable).
- **Effort:** S–M · **Risk:** low. **Independent quick win — can slot in any wave.**
- **Spec status:** not started.

### W7 — Schema-aware compiled query verification `[testing parity / DX]`
- **Goal:** Build-time validation of `[DataVoQuery]` SQL strings against the entity schema → bad column/table = compile error.
- **Research:** J% type-safe SQL embedding (compile-time validation against schema); Static Typing Meets Adaptive Optimization (SIGMOD 2025).
- **Depends on:** ⚠️ **the in-flight `DataVo.Generators` source generator (Tasks 7–11 of the observability/compiled-queries plan).** This is an *extension* of that generator — do not start until it lands.
- **Effort:** M · **Risk:** medium.
- **Spec status:** blocked on in-flight work.

### W8 — Zero-allocation struct projection `[HFT]`
- **Goal:** Generator emits code projecting results into a caller `Span<T>` — no heap, no boxing, reused buffers.
- **Research:** Photon (native specifically to escape GC pressure); Velox (small-batch low latency).
- **Depends on:** ⚠️ same in-flight `DataVo.Generators`; plus storage read path returning typed values without dictionary boxing.
- **Effort:** M–L · **Risk:** medium–high.
- **Spec status:** blocked on in-flight work.

### W9 — Lock-free append-only tables `[HFT]`
- **Goal:** `CREATE TABLE ... MODE APPEND_ONLY` backed by an internal lock-free ring buffer; no row locks/tombstones/VACUUM.
- **Research:** Disruptor (canonical lock-free ring buffer); Copy-Ahead Segment Ring (ring buffer as memtable); BBQ (modern bounded queue).
- **Depends on:** storage engine table-type abstraction.
- **Effort:** M · **Risk:** medium (concurrency correctness).
- **Spec status:** not started.

### W10 — Frame-budget suspendable execution `[game dev]`
- **Goal:** Yield mid-scan when a microsecond budget is exceeded; resume next call. Suspendable Volcano iterators.
- **Research:** Intermittent Query Processing (DISS: suspend/resume under budget).
- **Depends on:** Volcano execution pipeline (touches the hot path — high blast radius).
- **Effort:** L–XL · **Risk:** high. **Defer until W1–W5 prove out.**
- **Spec status:** not started.

### W11 — vmcache-style buffer pool (was: mmap storage) `[HFT / storage]`
- **Goal:** DB-controlled, virtual-memory-assisted buffer pool for faster row reads — *without* mmap's pitfalls. Keep true mmap only for read-only/immutable shipped data files.
- **Research:** mmap paper (why NOT mmap); vmcache (the right design).
- **Depends on:** Disk storage engine internals (large rework).
- **Effort:** L–XL · **Risk:** high. **Defer.**
- **Spec status:** not started (reframed from original mmap idea).

---

## Sequencing (waves)

**Wave 0 — in flight (other agent):** runtime diagnostics ✅ · compiled-query runtime helpers ✅ · `DataVo.Generators` source generator ⏳ (Tasks 7–11).

**Wave 1 — independent, high leverage (start after roadmap sign-off):**
- W1 Delta primitive → W2 Reactive queries V1
- W6 Chaos mode (parallel quick win)

**Wave 2 — builds on W1:**
- W3 Snapshot diff · W4 Time-travel · W5 COW branching (+ branch-diff = W5+W3)

**Wave 3 — builds on the source generator once it lands:**
- W7 Schema-aware verification · W8 Zero-allocation projection

**Wave 4 — larger / nicher, deferred:**
- W9 Lock-free append-only tables · W10 Frame-budget execution · W11 vmcache buffer pool

---

## Relationship to the in-flight observability/compiled-queries plan

- **Reuse, don't duplicate:** W2 reactive queries should surface through the **already-shipped**
  `DataVoDiagnostics`. W7 and W8 are **extensions of `DataVo.Generators`** — they wait for and
  build on the other agent's Tasks 7–11, never reimplement them.
- **No collisions in Wave 1:** W1/W2/W6 touch the delta/change layer and a storage wrapper, none
  of which the in-flight plan modifies — safe to start in parallel.

---

## Recommended next step

Write the first full spec for **W1 + W2 (delta primitive + reactive queries V1)** as a single
brainstorm→spec cycle, because W2 is the smallest useful thing that exercises W1 end-to-end and is
fully independent of the in-flight generator work. The remaining items get specced wave by wave.
