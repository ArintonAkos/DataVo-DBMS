# Roslyn Compile-Time Access Paths — Design

> **Status:** Roadmap (documented 2026-06-23, **not scheduled for build**). Architectural roadmap item for a
> future major phase. This is the "Step 2 / future fast-path" half of the Dual-Track query-planner work;
> **Step 1 (the runtime planner fix) shipped** in commit `fe7ae84`
> (`perf(query): route compiled non-PK equality predicates to secondary indexes`). No Step 2 code exists yet.

## Context: where this sits in the Dual-Track plan

The Dual-Track directive was: fix the runtime query planner first (so JIT/dynamic queries are fast on their
own), then add a compile-time fast-path for AOT/static queries — additive, never a crutch.

- **Step 1 (done):** `DataVoCompiledQuery.TryReadMatchingRowEntries` now routes any single-column non-PK
  equality predicate through `IndexManager.FilterUsingIndex` (mirroring the interpreted path
  `StatementEvaluatorWOJoin.HandleIndexableStatement`), not just PK/UK. Scenario B (deep document, 2,000
  orders, same machine): **~21.1 s → ~238 ms (~89×), p99 21.3 ms → 0.048 ms, GC 28.4 GB → 122.6 MB.** That
  captured the **algorithmic** `O(n²) → O(log n)` win.
- **Step 2 (this doc):** make the *existing* Roslyn source generator schema/index-aware so the access path is
  resolved at **compile time**, removing the per-call constant-factor re-derivation that the runtime path
  still performs — plus build-time diagnostics that turn "you forgot an index" into a compiler warning.

## Honest framing of the prize

This is **not another order of magnitude.** Step 1 already removed the complexity bottleneck. Step 2 removes
the **per-invocation constant-factor work** in `ExecuteSelect → TryReadMatchingRowEntries` that re-derives
facts already knowable at compile time, on every call:

1. `ResolveCurrentDatabase` (session lookup)
2. `ToParameterDictionary` + `BuildComparisonKey` (two dictionary allocations)
3. `GetTablePrimaryKeys` → `isPrimaryKeyPredicate`
4. **`TryResolveSingleColumnIndex` → `GetTableIndexes` catalog scan** (the resolution Step 1 added)
5. the real `FilterUsingIndex` probe + row materialization

Items 3–4 are pure re-derivation: a table's PK set and which index covers `OrderId` do not change between
calls. Eliminating them matters at the **high-QPS reactive/trading workloads** in this repo, not at 2,000
loads. The headline number stays Step 1's; Step 2 is a constant-factor + developer-experience play.

## This is not greenfield

A working incremental generator already exists and is the foundation to extend, not replace:

- `DataVo.Generators/DataVoQueryGenerator.cs` — `[Generator] IIncrementalGenerator`. Finds `partial` methods
  annotated `[DataVoQuery("SQL")]`, parses the SQL **shape**, and emits a `static readonly
  DataVoCompiledQueryPlan` plus a partial-method body that calls `DataVoCompiledQuery.SelectSingle/SelectMany/
  Insert/Update`.
- `DataVo.Generators/Sql/DataVoQueryShapeParser.cs` → `GeneratedQueryModel` (table, projected columns,
  where-column, where-parameter, kind, insert/update fields).
- `DataVo.Generators/Diagnostics/DataVoGeneratorDiagnostics.cs` — existing diagnostic plumbing.
- `DataVo.Core/CompiledQueries/DataVoQueryAttribute.cs` — `[DataVoQuery(sql, Kind = …)]`.

**The gap:** the generator only sees the *query text*. It has **no schema knowledge** — it doesn't know which
columns are indexed — so it cannot choose an access path today. Closing that gap is the center of gravity of
this PoC.

## Architecture

### 1. Compile-time catalog from an `AdditionalFiles` schema manifest

Point the generator at the same DDL the app runs (the `CREATE TABLE` / `CREATE INDEX` script) via
`AdditionalFiles` in the consuming `.csproj`. The generator ingests it through
`context.AdditionalTextsProvider`, parses it into a cached `CompileTimeCatalog`:

```
table → { primaryKeys: string[], singleColumnIndexes: Map<column, indexName> }
```

This keeps **one source of truth** (the DDL). Alternatives considered and rejected for the PoC:
- **Attribute index hints** (`[DataVoQuery("…", Index = "ix_…")]`) — duplicates the DDL, drifts.
- **EF Core model** — richer but couples the generator to `DataVo.EntityFrameworkCore`.

### 2. Resolve the access path at compile time → tag the plan

Extend `DataVoCompiledQueryPlan` with an **optional, backward-compatible** pre-resolved access path:

```csharp
public enum CompiledAccessPath { RuntimeResolve, PrimaryKey, SingleColumnIndex }
// plan also gains:  CompiledAccessPath AccessPath  +  string? ResolvedIndexName
```

Knowing the schema, the generator resolves `WhereColumn` → access path at build time and emits the plan
**already tagged**:

```csharp
// generated for:
//   [DataVoQuery("SELECT Sku,Name,Quantity,UnitPrice FROM OrderItems WHERE OrderId = @orderId")]
private static readonly DataVoCompiledQueryPlan __DataVoPlan_LoadItems =
    DataVoCompiledQueryPlan.SelectMany(
        "OrderItems", new[] { "Sku", "Name", "Quantity", "UnitPrice" },
        whereColumn: "OrderId", parameterName: "orderId",
        accessPath: CompiledAccessPath.SingleColumnIndex,   // resolved at COMPILE time
        resolvedIndexName: "ix_OrderItems_OrderId");
```

### 3. Runtime honors the tag (the Dual-Track seam)

In `TryReadMatchingRowEntries`: if `plan.AccessPath == SingleColumnIndex`, call
`FilterUsingIndex(expectedKey, plan.ResolvedIndexName, …)` directly — skipping `GetTablePrimaryKeys` and the
`GetTableIndexes` scan. Hand-built/JIT plans keep `AccessPath == RuntimeResolve` and flow through the Step 1
runtime resolution. Same executor, two tracks.

### 4. Safety invariant: a compile-time bet about runtime state must fail safe

The resolved index name is a bet that the runtime database actually has that index (migration applied, same
deployment). If the bet is wrong, it **must degrade, never break**: `FilterUsingIndex` throws `IndexException`
→ **fall through to the existing runtime resolution + typed scan** that Step 1 already guarantees. A stale tag
costs the optimization, never correctness. This is non-negotiable.

### 5. Build-time diagnostics (possibly the biggest near-term win)

With schema awareness the generator can surface, at compile time, problems that today only appear as slow
runtime behavior:

- unknown table / unknown column → **error**;
- predicate column with no covering index → **warning `DV1001`: "query will full-scan OrderItems.OrderId;
  add an index"** (exactly the trap Scenario B fell into);
- projected-column / mapper-arity / type mismatches → **error**.

### 6. Fix generator incrementality while we are in here

`DataVoQueryGenerator` currently does `methods.Combine(compilation)` and calls `GetSemanticModel` per method —
re-running the whole generator on every keystroke (a known source-generator anti-pattern). Restructure to:

```
ForAttributeWithMetadataName("DataVo.Core.CompiledQueries.DataVoQueryAttribute")
  → extract a small value-type record model
  → .Combine(cached CompileTimeCatalog from AdditionalFiles)
  → emit
```

This is both a build-perf correctness fix and the structural seam the schema work needs.

## Scope, success criteria, risks

| | |
|---|---|
| **In scope** | manifest ingestion + `CompileTimeCatalog`; access-path resolution for `SelectSingle`/`SelectMany` single-column equality; tagged plan + runtime honoring with safe fallback; the `DV1001` un-indexed-predicate diagnostic; incremental-pipeline restructure |
| **Out of scope (PoC)** | composite-index / range / `IN` predicates; joins; multi-predicate `AND`; eliminating the parameter-dict / comparison-key allocation (a "Layer 2" stretch toward absolute zero-overhead) |
| **Success criteria** | generated Scenario-B plans carry `SingleColumnIndex` and perform no runtime catalog lookup; result-parity tests vs the runtime path; a microbenchmark showing lower per-call allocation/time; a wrong/missing-index test proving safe fallthrough; `DV1001` fires on an unindexed predicate |
| **Risks** | manifest↔runtime schema drift (mitigated by §4 fallback + the `DV1001` diagnostic); generator incrementality regressions (mitigated by `ForAttributeWithMetadataName` + value-type models); must stay AOT-clean (generated code is plain method calls, no reflection — already satisfied) |

## Bottom line

Step 1 fixed the complexity bug and owns the headline number. Step 2 is a constant-factor + DX play: it removes
per-call re-derivation for AOT/high-QPS paths and turns "you forgot an index" from an 18-second surprise into a
build warning — built directly on the generator that already exists. Deferred to a future major phase.
