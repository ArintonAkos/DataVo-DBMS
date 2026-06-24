# Roslyn Compile-Time Access Path — PoC Design (Tracer Bullet)

> **Status:** Approved for build (2026-06-23). This is the buildable PoC slice of the roadmap item
> [`2026-06-23-roslyn-compile-time-access-paths.md`](./2026-06-23-roslyn-compile-time-access-paths.md)
> (Dual-Track **Step 2**). Step 1 — the runtime planner fix that routes single-column non-PK equality
> predicates through `IndexManager.FilterUsingIndex` — shipped in commit `fe7ae84` and is the foundation this
> builds on. No Step 2 code exists yet; this spec defines the first vertical slice.

## 1. Goal

Fire a **tracer bullet** through every architectural layer of compile-time access-path resolution: prove that
a schema fact declared in an external DDL manifest can travel through Roslyn into a compiled query plan as a
pre-resolved access-path tag, and that the runtime honors that tag — bypassing per-call catalog re-derivation —
while remaining safe enough to fall back to the Step 1 runtime path when the compile-time bet is wrong.

The prize is **not** another order of magnitude (Step 1 already removed the `O(n²)` complexity bug and owns the
headline number). It is the removal of **per-invocation constant-factor catalog re-derivation** on the hot read
path, validated end to end on the thinnest possible slice.

## 2. Scope

### In scope (the slice)

- A bespoke, minimal DDL manifest parser **inside the generator** producing a value-equatable
  `CompileTimeCatalog`.
- Compile-time access-path resolution for **`SelectMany` with a single-column equality predicate only**.
- An optional, backward-compatible `CompiledAccessPath` tag on `DataVoCompiledQueryPlan`.
- Runtime honoring of the `SingleColumnIndex` tag in `TryReadMatchingRowEntries`, skipping
  `GetTablePrimaryKeys` + the `GetTableIndexes` scan.
- The safe-fallback invariant: `IndexException` (or empty result) → fall through to the untouched Step 1
  runtime resolution + typed scan.
- Tests: DDL-parser unit, generator emission (string assertions), runtime parity, safe fallback, per-call
  allocation microbenchmark.

### Scope boundary — what is deliberately NOT touched

The directive "bypass the catalog/dictionary allocations" is scoped precisely to the **catalog
re-derivation**: `GetTablePrimaryKeys` (a `List<string>` allocation) and the `GetTableIndexes` scan inside
`TryResolveSingleColumnIndex` (roadmap-doc items 3–4). The PoC does **NOT** remove the
`ToParameterDictionary` / `BuildComparisonKey` allocations (roadmap-doc item 2, the "Layer 2" stretch);
eliminating those would require reworking how `expectedKey` is constructed across both tracks and is
explicitly deferred.

### Deferred to a later phase (not this PoC)

- The `DV1001` un-indexed-predicate **warning** and schema-aware unknown-table/column **errors** (the DX
  surface).
- The `ForAttributeWithMetadataName` **incrementality restructure** of `DataVoQueryGenerator` (this slice
  bolts the catalog onto the existing `methods.Combine(compilation)` pipeline rather than restructuring it).
- Tagging `SelectSingle`, `Insert`, `Update`.
- Composite-index / range / `IN` / multi-predicate `AND` / joins.
- Layer-2 parameter-dictionary / comparison-key allocation elimination.

## 3. Architecture

```
COMPILE TIME — DataVo.Generators (netstandard2.0)
  schema.sql  (AdditionalFile, DataVoSchemaManifest="true")
      │   AnalyzerConfigOptionsProvider: build_metadata.AdditionalFiles.DataVoSchemaManifest == "true"
      ▼
  DataVoDdlManifestParser  ──►  CompileTimeCatalog   (cached, value-equatable)
      │                              table → { primaryKeys[], singleColumnIndexes: column → indexName }
      │
  methods.Combine(compilation).Combine(catalog)
      ▼
  EmitForMethod:  SelectMany  +  single-column equality  +  catalog resolves WhereColumn → index
      └─► DataVoCompiledQueryPlan.SelectMany(
              table, columns, whereColumn, parameterName,
              accessPath: CompiledAccessPath.SingleColumnIndex,     // resolved at COMPILE time
              resolvedIndexName: "ix_OrderItems_OrderId")
          (catalog miss → emit untagged plan, AccessPath = RuntimeResolve)

RUNTIME — DataVo.Core (net10.0), DataVoCompiledQuery.TryReadMatchingRowEntries
      if plan.AccessPath == SingleColumnIndex && plan.ResolvedIndexName is not null:
          try   ReadRowsViaIndex(plan.ResolvedIndexName)   ◄── skips GetTablePrimaryKeys + GetTableIndexes scan
          catch IndexException ─┐
          empty result         ─┴─► FALL THROUGH to the existing Step 1 runtime resolution + typed scan
      else:
          existing Step 1 path (GetTablePrimaryKeys → TryResolveSingleColumnIndex → typed scan)
```

**Constraint that shapes this design:** `DataVo.Generators` targets `netstandard2.0` and references only
`Microsoft.CodeAnalysis.CSharp`. It cannot reference `DataVo.Core` (`net10.0`). Therefore the generator
**cannot** reuse the engine's DDL parser or catalog types — it carries its own minimal parser, and it never
sees the live database. The compile-time tag is purely a bet about runtime state, which is precisely why the
§7 fail-safe is non-negotiable.

## 4. Components

| Unit | File | Purpose / interface | Dependencies |
|---|---|---|---|
| `CompileTimeCatalog` | `DataVo.Generators/Sql/CompileTimeCatalog.cs` (new) | Immutable, **value-equatable** (structural equality required for Roslyn incremental caching) schema snapshot. `bool TryResolveSingleColumnIndex(string table, string column, out string indexName)`; `bool IsPrimaryKey(string table, string column)`. | none (pure data) |
| `DataVoDdlManifestParser` | `DataVo.Generators/Sql/DataVoDdlManifestParser.cs` (new) | Minimal regex parser mirroring `DataVoQueryShapeParser`. Recognizes `CREATE TABLE t ( … )` (extract single-column `PRIMARY KEY`, inline or table-constraint form) and `CREATE [UNIQUE] INDEX ix ON t (col)` (single column only). Ignores composite indexes/PKs and unrecognized statements. Returns a merged `CompileTimeCatalog`. | `System.Text.RegularExpressions` |
| Generator pipeline | `DataVo.Generators/DataVoQueryGenerator.cs` (edit — bolt-on, not restructure) | Add an `IncrementalValueProvider<CompileTimeCatalog>` from `context.AdditionalTextsProvider.Combine(context.AnalyzerConfigOptionsProvider)`, filtered by the `DataVoSchemaManifest` metadata, parsed and `.Collect()`-merged into one catalog. Combine into the existing `methods.Combine(compilation)` registration. In `GeneratePlan`, when the resolved shape is `SelectMany` and the catalog resolves `WhereColumn` to a single-column index, emit the tagged factory call; otherwise emit the existing untagged call. | Roslyn |
| Plan tag | `DataVo.Core/CompiledQueries/DataVoCompiledQueryPlan.cs` (edit) | Add `enum CompiledAccessPath { RuntimeResolve, PrimaryKey, SingleColumnIndex }`; add `CompiledAccessPath AccessPath { get; }` and `string? ResolvedIndexName { get; }`; add **optional** params to the `SelectMany` factory only (`accessPath = RuntimeResolve`, `resolvedIndexName = null`). Validate: `SingleColumnIndex` ⇒ non-empty `resolvedIndexName`. All existing callers and other factories are unaffected (default `RuntimeResolve`). | none |
| Runtime honor | `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs` → `TryReadMatchingRowEntries` (edit, ~line 203) | New guarded branch at the top that calls the existing `ReadRowsViaIndex(plan.ResolvedIndexName)`; on `IndexException` or empty result, fall into the unchanged Step 1 resolution below. | none |

Each unit answers cleanly: *what it does, how you use it, what it depends on.* The catalog and parser are pure
and independently testable; the plan tag is additive data; the runtime change is one guarded branch over an
existing helper.

## 5. Compile-time contract (consuming project)

The consuming `.csproj` declares the manifest and makes the marker visible to the generator:

```xml
<ItemGroup>
  <AdditionalFiles Include="schema.sql" DataVoSchemaManifest="true" />
</ItemGroup>
<ItemGroup>
  <CompilerVisibleItemMetadata Include="AdditionalFiles" MetadataName="DataVoSchemaManifest" />
</ItemGroup>
```

The generator reads `options.GetOptions(additionalText).TryGetValue("build_metadata.AdditionalFiles.DataVoSchemaManifest", out var v)`
and treats the file as a manifest when `v == "true"`. Multiple manifests merge into one catalog.

## 6. Generated output (illustrative)

For:

```csharp
[DataVoQuery("SELECT Sku, Name, Quantity, UnitPrice FROM OrderItems WHERE OrderId = @orderId")]
public static partial List<OrderItemRow> LoadItems(DataVoContext db, int orderId);
```

with `schema.sql` containing `CREATE INDEX ix_OrderItems_OrderId ON OrderItems (OrderId);`, the generator emits
a plan tagged at compile time:

```csharp
private static readonly global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan __DataVoPlan_LoadItems =
    global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan.SelectMany(
        "OrderItems", new string[] { "Sku", "Name", "Quantity", "UnitPrice" }, "OrderId", "orderId",
        accessPath: global::DataVo.Core.CompiledQueries.CompiledAccessPath.SingleColumnIndex,
        resolvedIndexName: "ix_OrderItems_OrderId");
```

With no manifest (or no covering index for `OrderId`), the emitted call is the current untagged form
(`AccessPath` defaults to `RuntimeResolve`).

## 7. Error handling / fail-safe (the invariant)

- **Stale / missing index at runtime** → `IndexManager.FilterUsingIndex` throws `IndexException` →
  fall through to Step 1 resolution. A wrong compile-time bet costs the optimization, never correctness.
- **Empty index result** → fall through to the typed scan, mirroring the existing primary-key and secondary
  branches exactly (`DataVoCompiledQuery.cs:242`). A genuinely absent key therefore costs one scan, identical
  to today's behavior.
- **Unparseable or absent manifest at compile time** → emit untagged (`RuntimeResolve`) plans: a silent, safe
  degrade. Compile-time *diagnostics* for this case (`DV1001`, unknown-table/column errors) are the deferred
  DX phase and intentionally out of this slice.

## 8. Testing

Mapped to existing test homes — no new project required.

1. **DDL-parser unit** (`DataVo.Generators.Tests/`, new file): DDL strings → assert `CompileTimeCatalog`
   contents (single-column PK captured; single-column index mapped; composite index/PK ignored;
   unrecognized statements skipped).
2. **Generator emission** (`DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`, extend `RunGenerator` to
   accept `AdditionalText`s + analyzer-config options): manifest + indexed `SelectMany` → generated source
   `Contains` `CompiledAccessPath.SingleColumnIndex` and `resolvedIndexName: "ix_…"`. Negative cases: no
   manifest, and indexed-table-but-unindexed-column → untagged plan (no `SingleColumnIndex` token).
   **String-based assertions only** (fast, deterministic, isolated).
3. **Runtime parity** (`DataVo.Tests/E2E/CompiledQueryRuntimeTests.cs`): a `SingleColumnIndex`-tagged plan and
   an equivalent `RuntimeResolve` plan over the same seeded table return identical rows.
4. **Safe fallback** (`DataVo.Tests/E2E/CompiledQueryRuntimeTests.cs`): a plan tagged with a **nonexistent**
   `resolvedIndexName` over a populated table still returns correct rows — proving the `IndexException` →
   fallthrough path. (Tests 3 and 4 also inherently prove the generated code is syntactically valid and
   executes.)
5. **Allocation microbenchmark** (`DataVo.Tests`, alloc-budget-guard idiom from
   `DataVo.Tests/Indexing/HNSWPagedBackingTests.cs`): per-call allocation of the tagged path is strictly less
   than the `RuntimeResolve` path (it skips the `GetTablePrimaryKeys` list and the `GetTableIndexes`
   enumeration).

## 9. Success criteria

- Generated `SelectMany` plans for an indexed predicate carry `CompiledAccessPath.SingleColumnIndex` + the
  correct `resolvedIndexName`; unindexed/no-manifest cases stay `RuntimeResolve`.
- At runtime the tagged path performs **no** `GetTablePrimaryKeys`/`GetTableIndexes` lookup on the happy path.
- Result parity with the runtime path (test 3).
- A wrong/missing tag degrades to correct results (test 4).
- A microbenchmark shows strictly lower per-call allocation for the tagged path (test 5).
- Whole solution builds with **0 warnings**, stays **AOT-clean** (generated code is plain method calls, no
  reflection), and the full suite stays green.

## 10. Risks

- **Manifest ↔ runtime schema drift** — mitigated by the §7 `IndexException` fallthrough; the `DV1001`
  diagnostic that would surface drift at build time is deferred.
- **Generator incrementality** — bolting `.Combine(catalog)` onto the existing (already non-ideal) pipeline
  must not regress caching; `CompileTimeCatalog` is value-equatable specifically so the catalog node caches.
  The full `ForAttributeWithMetadataName` restructure remains deferred.
- **DDL-parser brittleness** — the minimal regex parser handles only single-column `CREATE TABLE`
  PK / `CREATE INDEX` forms; anything else is ignored and safely degrades to `RuntimeResolve`.

## 11. Bottom line

The tracer bullet proves the Dual-Track seam end to end: schema → compile-time catalog → tagged plan → runtime
honor → safe fallback, on the thinnest slice (`SelectMany`, single-column equality). It builds directly on the
generator and Step 1 runtime that already exist, removes per-call catalog re-derivation on the hot read path,
and establishes the structure the deferred DX and incrementality work will extend.
