# Roslyn Compile-Time Access Path PoC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove end-to-end that a single-column index declared in a DDL manifest can be resolved at compile time into a `CompiledAccessPath.SingleColumnIndex` tag on a `SelectMany` plan, and that the runtime honors the tag (skipping catalog re-derivation) with a safe fallback when the tag is wrong.

**Architecture:** A bespoke regex DDL parser inside the `netstandard2.0` source generator builds a value-equatable `CompileTimeCatalog` from `AdditionalFiles` flagged `DataVoSchemaManifest="true"`. The generator tags `SelectMany` plans whose `WHERE` column the catalog resolves to a single-column index. The `net10.0` runtime adds one guarded branch in `TryReadMatchingRowEntries` that routes a tagged plan straight through `ReadRowsViaIndex`, falling through to the existing Step-1 resolution on `IndexException` or empty result.

**Tech Stack:** C# / .NET 10 (`DataVo.Core`), Roslyn incremental source generators (`Microsoft.CodeAnalysis.CSharp` 4.14, `netstandard2.0`), xUnit (`DataVo.Tests`, `DataVo.Generators.Tests`).

**Spec:** `docs/superpowers/specs/2026-06-23-roslyn-compile-time-access-path-poc-design.md`
**Branch:** `feature/roslyn-compile-time-access-path-poc` (already checked out; spec committed at f6b5c33)

## Global Constraints

- **Generator project (`DataVo.Generators`) targets `netstandard2.0`** and may reference only `Microsoft.CodeAnalysis.CSharp`. It must NOT reference `DataVo.Core`. New generator types use only BCL + Roslyn APIs available on `netstandard2.0`.
- **`CompileTimeCatalog` must be value-equatable** (structural `Equals`/`GetHashCode`) so the incremental catalog node caches correctly.
- **Backward compatibility:** the `CompiledAccessPath` tag is additive. Existing `DataVoCompiledQueryPlan` factory calls (no access-path args) must compile unchanged and default to `CompiledAccessPath.RuntimeResolve`.
- **Safe-fallback invariant (non-negotiable):** a wrong/missing compile-time tag must degrade to correct results, never throw for that reason. Only `IndexException` (missing/unhealthy index) is caught in the tagged branch; any other exception propagates (consistent with the existing secondary-index branch).
- **Scope:** `SelectMany`, single-column equality only. Do NOT touch `ToParameterDictionary`/`BuildComparisonKey` ("Layer 2"), do NOT add `DV1001`/error diagnostics, do NOT restructure the generator to `ForAttributeWithMetadataName`, do NOT tag `SelectSingle`/`Insert`/`Update`.
- **Quality bars:** whole solution builds with **0 warnings**, stays **AOT-clean** (generated code is plain method calls, no reflection), full suite stays green.
- **`internal` visibility:** `DataVo.Generators` already has `[assembly: InternalsVisibleTo("DataVo.Generators.Tests")]` (`DataVo.Generators/Properties/AssemblyInfo.cs:3`), so new `internal` generator types are test-visible — keep them `internal`.

## File Structure

**Create:**
- `DataVo.Core/CompiledQueries/CompiledAccessPath.cs` — the access-path enum (public, AOT-safe).
- `DataVo.Generators/Sql/CompileTimeCatalog.cs` — value-equatable schema snapshot + lookup methods.
- `DataVo.Generators/Sql/DataVoDdlManifestParser.cs` — minimal regex DDL parser → `CompileTimeCatalog`.
- `DataVo.Generators.Tests/DataVoDdlManifestParserTests.cs` — parser/catalog unit tests.
- `DataVo.Tests/E2E/CompiledAccessPathTests.cs` — plan-tag unit tests, runtime honoring/parity/fallback tests, allocation microbenchmark.

**Modify:**
- `DataVo.Core/CompiledQueries/DataVoCompiledQueryPlan.cs` — add `AccessPath`/`ResolvedIndexName` props; add optional params to the private ctor and the `SelectMany` factory.
- `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs:203` — add the tagged-path branch at the top of `TryReadMatchingRowEntries`.
- `DataVo.Generators/DataVoQueryGenerator.cs` — wire the catalog provider; thread catalog into `EmitForMethod`/`GenerateMethod`/`GeneratePlan`; tag `SelectMany`.
- `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs` — extend `RunGenerator` to inject manifest + analyzer-config options; add tagged-emission tests.

---

### Task 1: `CompiledAccessPath` enum + plan tag

**Files:**
- Create: `DataVo.Core/CompiledQueries/CompiledAccessPath.cs`
- Modify: `DataVo.Core/CompiledQueries/DataVoCompiledQueryPlan.cs`
- Test: `DataVo.Tests/E2E/CompiledAccessPathTests.cs` (create)

**Interfaces:**
- Produces: `enum CompiledAccessPath { RuntimeResolve = 0, PrimaryKey = 1, SingleColumnIndex = 2 }` (namespace `DataVo.Core.CompiledQueries`); `DataVoCompiledQueryPlan.AccessPath` (`CompiledAccessPath`), `DataVoCompiledQueryPlan.ResolvedIndexName` (`string?`); `DataVoCompiledQueryPlan.SelectMany(string, IReadOnlyList<string>, string, string, CompiledAccessPath = RuntimeResolve, string? = null)`.

- [ ] **Step 1: Write the failing tests**

Create `DataVo.Tests/E2E/CompiledAccessPathTests.cs`:

```csharp
using DataVo.Core.CompiledQueries;

namespace DataVo.Tests.E2E;

public class CompiledAccessPathTests
{
    [Fact]
    public void SelectMany_DefaultAccessPath_IsRuntimeResolve()
    {
        var plan = DataVoCompiledQueryPlan.SelectMany("Players", ["Id", "Name"], "Name", "name");

        Assert.Equal(CompiledAccessPath.RuntimeResolve, plan.AccessPath);
        Assert.Null(plan.ResolvedIndexName);
    }

    [Fact]
    public void SelectMany_TaggedSingleColumnIndex_CarriesAccessPathAndIndexName()
    {
        var plan = DataVoCompiledQueryPlan.SelectMany(
            "Players", ["Id", "Name"], "Name", "name",
            accessPath: CompiledAccessPath.SingleColumnIndex,
            resolvedIndexName: "ix_players_name");

        Assert.Equal(CompiledAccessPath.SingleColumnIndex, plan.AccessPath);
        Assert.Equal("ix_players_name", plan.ResolvedIndexName);
    }

    [Fact]
    public void SelectMany_SingleColumnIndexWithoutIndexName_Throws()
    {
        Assert.Throws<ArgumentException>(() => DataVoCompiledQueryPlan.SelectMany(
            "Players", ["Id", "Name"], "Name", "name",
            accessPath: CompiledAccessPath.SingleColumnIndex,
            resolvedIndexName: null));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~CompiledAccessPathTests"`
Expected: COMPILE FAILURE — `CompiledAccessPath` does not exist and `SelectMany` has no `accessPath`/`resolvedIndexName` params, `AccessPath`/`ResolvedIndexName` are not members.

- [ ] **Step 3: Create the enum**

Create `DataVo.Core/CompiledQueries/CompiledAccessPath.cs`:

```csharp
namespace DataVo.Core.CompiledQueries;

/// <summary>
/// The access path a compiled query plan should use to locate matching rows. Plans authored by hand or
/// emitted without schema knowledge use <see cref="RuntimeResolve"/>; the source generator may pre-resolve a
/// plan to a faster path at compile time. A pre-resolved path is a bet about runtime state and must fail safe
/// (see <c>DataVoCompiledQuery.TryReadMatchingRowEntries</c>).
/// </summary>
public enum CompiledAccessPath
{
    /// <summary>Resolve the access path at runtime (primary-key / secondary-index / scan). The default.</summary>
    RuntimeResolve = 0,

    /// <summary>Reserved for a future compile-time primary-key fast path. Not emitted by the current generator.</summary>
    PrimaryKey = 1,

    /// <summary>Use the single-column secondary index named by <c>ResolvedIndexName</c>, resolved at compile time.</summary>
    SingleColumnIndex = 2,
}
```

- [ ] **Step 4: Add the tag to the plan**

In `DataVo.Core/CompiledQueries/DataVoCompiledQueryPlan.cs`, add two parameters with defaults to the **private constructor** signature (after the `assignments` parameter):

```csharp
    private DataVoCompiledQueryPlan(
        DataVoCompiledQueryKind kind,
        string tableName,
        IReadOnlyList<string> projectedColumns,
        string? whereColumn,
        string? whereParameterName,
        IReadOnlyList<string> insertColumns,
        IReadOnlyList<string> insertParameterNames,
        IReadOnlyDictionary<string, string> assignments,
        CompiledAccessPath accessPath = CompiledAccessPath.RuntimeResolve,
        string? resolvedIndexName = null)
    {
```

Inside that constructor body, after the existing `Assignments = assignments;` line, add:

```csharp
        AccessPath = accessPath;
        ResolvedIndexName = resolvedIndexName;
```

After the existing `public IReadOnlyDictionary<string, string> Assignments { get; }` property, add:

```csharp
    /// <summary>Gets the access path pre-resolved at compile time, or <see cref="CompiledAccessPath.RuntimeResolve"/>.</summary>
    public CompiledAccessPath AccessPath { get; }

    /// <summary>Gets the index name resolved at compile time when <see cref="AccessPath"/> is <see cref="CompiledAccessPath.SingleColumnIndex"/>; otherwise null.</summary>
    public string? ResolvedIndexName { get; }
```

Replace the entire `SelectMany` factory method with this version (adds the two optional params, validation, and passes them through):

```csharp
    /// <summary>
    /// Creates a plan that returns all rows matching an equality predicate. When <paramref name="accessPath"/>
    /// is <see cref="CompiledAccessPath.SingleColumnIndex"/>, the runtime routes directly through
    /// <paramref name="resolvedIndexName"/>, falling back to runtime resolution if that index is missing.
    /// </summary>
    public static DataVoCompiledQueryPlan SelectMany(
        string tableName,
        IReadOnlyList<string> projectedColumns,
        string whereColumn,
        string parameterName,
        CompiledAccessPath accessPath = CompiledAccessPath.RuntimeResolve,
        string? resolvedIndexName = null)
    {
        if (accessPath == CompiledAccessPath.SingleColumnIndex && string.IsNullOrWhiteSpace(resolvedIndexName))
        {
            throw new ArgumentException(
                "A SingleColumnIndex access path requires a resolved index name.",
                nameof(resolvedIndexName));
        }

        return new DataVoCompiledQueryPlan(
            DataVoCompiledQueryKind.SelectMany,
            tableName,
            projectedColumns ?? throw new ArgumentNullException(nameof(projectedColumns)),
            RequireIdentifier(whereColumn, nameof(whereColumn)),
            RequireIdentifier(parameterName, nameof(parameterName)),
            [],
            [],
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
            accessPath,
            resolvedIndexName);
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~CompiledAccessPathTests"`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add DataVo.Core/CompiledQueries/CompiledAccessPath.cs DataVo.Core/CompiledQueries/DataVoCompiledQueryPlan.cs DataVo.Tests/E2E/CompiledAccessPathTests.cs
git commit -m "feat(query): add CompiledAccessPath tag to compiled SelectMany plans

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 2: Runtime honors the tag (with safe fallback)

**Files:**
- Modify: `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs:203` (`TryReadMatchingRowEntries`)
- Test: `DataVo.Tests/E2E/CompiledAccessPathTests.cs` (append)

**Interfaces:**
- Consumes: `DataVoCompiledQueryPlan.AccessPath`, `DataVoCompiledQueryPlan.ResolvedIndexName` (Task 1); existing `ReadRowsViaIndex(context, plan, databaseName, indexName, expectedKey)` and `DataVoCompiledQuery.SelectMany(context, plan, parameters, mapper)`.
- Produces: behavior — a `SingleColumnIndex`-tagged plan reads through `plan.ResolvedIndexName` and falls through to runtime resolution on `IndexException`/empty.

- [ ] **Step 1: Write the failing tests**

Append to `DataVo.Tests/E2E/CompiledAccessPathTests.cs`. First add these `using` directives at the top of the file (below the existing `using DataVo.Core.CompiledQueries;`):

```csharp
using System.Reflection;
using DataVo.Core;
using DataVo.Core.BTree.Core;
using DataVo.Core.Indexing;
using DataVo.Core.StorageEngine.Config;
```

Then add these members inside the `CompiledAccessPathTests` class:

```csharp
    [Fact]
    public void TaggedSingleColumnIndex_ReturnsSameRowsAsRuntimeResolve()
    {
        using var context = CreateContext();
        SeedPlayers(context);
        context.Execute("CREATE INDEX ix_players_name ON Players (Name)");

        IReadOnlyList<PlayerProjection> tagged = QueryByName(
            context,
            DataVoCompiledQueryPlan.SelectMany(
                "Players", ["Id", "Name", "Level"], "Name", "name",
                accessPath: CompiledAccessPath.SingleColumnIndex,
                resolvedIndexName: "ix_players_name"),
            "Ada");

        IReadOnlyList<PlayerProjection> runtimeResolved = QueryByName(
            context,
            DataVoCompiledQueryPlan.SelectMany("Players", ["Id", "Name", "Level"], "Name", "name"),
            "Ada");

        Assert.Equal(
            tagged.OrderBy(p => p.Id),
            runtimeResolved.OrderBy(p => p.Id));
        Assert.Equal(
            new[] { new PlayerProjection(1, "Ada", 5), new PlayerProjection(3, "Ada", 9) },
            tagged.OrderBy(p => p.Id));
    }

    [Fact]
    public void TaggedSingleColumnIndex_RoutesThroughTheNamedIndex()
    {
        // Replace the named index with one that throws when searched. If the tagged branch consults it (as it
        // must), the InvalidOperationException propagates — proving the tag was honored, not silently scanned.
        using var context = CreateContext();
        SeedPlayers(context);
        context.Execute("CREATE INDEX ix_players_name ON Players (Name)");
        ReplaceIndexWithThrowingIndex(context, "Players", "ix_players_name");

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => QueryByName(
            context,
            DataVoCompiledQueryPlan.SelectMany(
                "Players", ["Id", "Name", "Level"], "Name", "name",
                accessPath: CompiledAccessPath.SingleColumnIndex,
                resolvedIndexName: "ix_players_name"),
            "Ada"));

        Assert.Equal("boom", ex.Message);
    }

    [Fact]
    public void TaggedWithNonexistentIndex_FallsBackToCorrectResults()
    {
        // The compile-time bet is wrong (no such index). IndexException must be caught and the query must fall
        // through to runtime resolution + scan, returning correct rows. Safety invariant.
        using var context = CreateContext();
        SeedPlayers(context);

        IReadOnlyList<PlayerProjection> players = QueryByName(
            context,
            DataVoCompiledQueryPlan.SelectMany(
                "Players", ["Id", "Name", "Level"], "Name", "name",
                accessPath: CompiledAccessPath.SingleColumnIndex,
                resolvedIndexName: "ix_does_not_exist"),
            "Ada");

        Assert.Equal(
            new[] { new PlayerProjection(1, "Ada", 5), new PlayerProjection(3, "Ada", 9) },
            players.OrderBy(p => p.Id));
    }

    private sealed class ThrowingIndex : IIndex
    {
        public void Insert(string key, long rowId) => throw new NotSupportedException();
        public void DeleteValues(List<long> rowIds) => throw new NotSupportedException();
        public List<long> Search(string key) => throw new InvalidOperationException("boom");
        public bool ContainsValue(long rowId) => throw new NotSupportedException();
        public void Save(string filePath) => throw new NotSupportedException();
    }

    private static IReadOnlyList<PlayerProjection> QueryByName(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string name)
    {
        return DataVoCompiledQuery.SelectMany(
            context,
            plan,
            [new DataVoCompiledQueryParameter("name", name)],
            static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!));
    }

    private static void SeedPlayers(DataVoContext context)
    {
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.BulkInsert(
            "Players",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 5 },
                new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Grace", ["Level"] = 8 },
                new Dictionary<string, object?> { ["Id"] = 3, ["Name"] = "Ada", ["Level"] = 9 }
            ]);
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string databaseName = $"AccessPath_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }

    private static string CurrentDatabase(DataVoContext context)
    {
        return context.Engine.Sessions.Get(context.SessionId)
            ?? throw new InvalidOperationException("Expected current database.");
    }

    private static void ReplaceIndexWithThrowingIndex(DataVoContext context, string tableName, string indexName)
    {
        FieldInfo cacheField = typeof(IndexManager).GetField("_cache", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var cache = (Dictionary<string, IIndexBase>)cacheField.GetValue(context.Engine.IndexManager)!;
        string databaseName = CurrentDatabase(context);
        string cacheKey = $"{databaseName}/{tableName}_{indexName}".ToLowerInvariant();
        cache[cacheKey] = new ThrowingIndex();
    }
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~CompiledAccessPathTests"`
Expected: `TaggedSingleColumnIndex_RoutesThroughTheNamedIndex` FAILS (no exception thrown — the tag is ignored, so the table is scanned and the throwing index is never consulted). The parity and fallback tests may pass coincidentally (untagged behavior already returns correct rows); the routing test is the one that pins the new behavior.

- [ ] **Step 3: Add the tagged-path branch**

In `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`, in `TryReadMatchingRowEntries` (begins at line 203), insert the following block at the very top of the method body — immediately before the existing `List<string> primaryKeys = context.Engine.Catalog.GetTablePrimaryKeys(...)` line:

```csharp
        // Compile-time fast path: a generator-resolved single-column index skips the per-call primary-key and
        // index catalog lookups below. A wrong/missing tag (IndexException) or an empty result falls through to
        // the runtime resolution, so correctness never depends on the compile-time bet being right.
        if (plan.AccessPath == CompiledAccessPath.SingleColumnIndex && plan.ResolvedIndexName is not null)
        {
            try
            {
                List<KeyValuePair<long, Dictionary<string, object?>>> tagged =
                    ReadRowsViaIndex(context, plan, databaseName, plan.ResolvedIndexName, expectedKey);

                if (tagged.Count > 0)
                {
                    return tagged;
                }
            }
            catch (IndexException)
            {
            }
        }

```

(The `IndexException` type is already imported in this file — it is used by the existing primary-key branch.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~CompiledAccessPathTests"`
Expected: PASS (6 tests: 3 from Task 1 + the 3 runtime tests).

- [ ] **Step 5: Run the existing compiled-query suite to confirm no regression**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~CompiledQueryRuntimeTests"`
Expected: PASS (all existing tests still green — untagged plans still default to `RuntimeResolve` and skip the new branch).

- [ ] **Step 6: Commit**

```bash
git add DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs DataVo.Tests/E2E/CompiledAccessPathTests.cs
git commit -m "feat(query): runtime honors SingleColumnIndex tag with safe fallthrough

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 3: Compile-time catalog + DDL manifest parser

**Files:**
- Create: `DataVo.Generators/Sql/CompileTimeCatalog.cs`
- Create: `DataVo.Generators/Sql/DataVoDdlManifestParser.cs`
- Test: `DataVo.Generators.Tests/DataVoDdlManifestParserTests.cs` (create)

**Interfaces:**
- Produces: `internal sealed class CompileTimeCatalog` with `static readonly CompileTimeCatalog Empty`, `internal static string Key(string table, string column)`, `bool TryResolveSingleColumnIndex(string table, string column, out string indexName)`, `bool IsPrimaryKey(string table, string column)`, value-based `Equals`/`GetHashCode`. `internal static class DataVoDdlManifestParser` with `static CompileTimeCatalog Parse(ImmutableArray<string> manifestTexts)`.

- [ ] **Step 1: Write the failing tests**

Create `DataVo.Generators.Tests/DataVoDdlManifestParserTests.cs`:

```csharp
using System.Collections.Immutable;
using DataVo.Generators.Sql;

namespace DataVo.Generators.Tests;

public class DataVoDdlManifestParserTests
{
    [Fact]
    public void Parse_SingleColumnIndex_Resolves()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create(
            "CREATE TABLE OrderItems (OrderItemId INT PRIMARY KEY, OrderId INT, Sku VARCHAR(50)); " +
            "CREATE INDEX ix_OrderItems_OrderId ON OrderItems (OrderId);"));

        Assert.True(catalog.TryResolveSingleColumnIndex("OrderItems", "OrderId", out string name));
        Assert.Equal("ix_OrderItems_OrderId", name);
    }

    [Fact]
    public void Parse_PrimaryKey_IsRecognized()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create(
            "CREATE TABLE OrderItems (OrderItemId INT PRIMARY KEY, OrderId INT);"));

        Assert.True(catalog.IsPrimaryKey("OrderItems", "OrderItemId"));
        Assert.False(catalog.IsPrimaryKey("OrderItems", "OrderId"));
    }

    [Fact]
    public void Parse_TableConstraintPrimaryKey_IsRecognized()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create(
            "CREATE TABLE OrderItems (OrderItemId INT, OrderId INT, PRIMARY KEY (OrderItemId));"));

        Assert.True(catalog.IsPrimaryKey("OrderItems", "OrderItemId"));
    }

    [Fact]
    public void Parse_CompositeIndex_IsIgnored()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create(
            "CREATE INDEX ix_multi ON OrderItems (OrderId, Sku);"));

        Assert.False(catalog.TryResolveSingleColumnIndex("OrderItems", "OrderId", out _));
    }

    [Fact]
    public void Parse_LookupIsCaseInsensitive()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create(
            "CREATE INDEX ix ON Players (Name);"));

        Assert.True(catalog.TryResolveSingleColumnIndex("players", "name", out string name));
        Assert.Equal("ix", name);
    }

    [Fact]
    public void Parse_EmptyOrUnrecognized_ReturnsEmptyCatalog()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create("SELECT 1;", ""));

        Assert.False(catalog.TryResolveSingleColumnIndex("T", "Col", out _));
        Assert.False(catalog.IsPrimaryKey("T", "Col"));
    }

    [Fact]
    public void Parse_EqualManifests_ProduceEqualCatalogs()
    {
        var texts = ImmutableArray.Create("CREATE INDEX ix ON Players (Name);");
        CompileTimeCatalog a = DataVoDdlManifestParser.Parse(texts);
        CompileTimeCatalog b = DataVoDdlManifestParser.Parse(texts);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj --filter "FullyQualifiedName~DataVoDdlManifestParserTests"`
Expected: COMPILE FAILURE — `CompileTimeCatalog` and `DataVoDdlManifestParser` do not exist.

- [ ] **Step 3: Create `CompileTimeCatalog`**

Create `DataVo.Generators/Sql/CompileTimeCatalog.cs`:

```csharp
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVo.Generators.Sql;

/// <summary>
/// Immutable, value-equatable compile-time view of the table schema relevant to access-path resolution:
/// which single columns are primary keys and which single columns are covered by a named secondary index.
/// Value equality (over a canonical signature) lets the Roslyn incremental catalog node cache between builds.
/// </summary>
internal sealed class CompileTimeCatalog : IEquatable<CompileTimeCatalog>
{
    public static readonly CompileTimeCatalog Empty = new(
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
        new HashSet<string>(StringComparer.OrdinalIgnoreCase));

    // Keyed "table|column" (case-insensitive). Value is the declared index name (original casing).
    private readonly Dictionary<string, string> _columnIndexes;
    private readonly HashSet<string> _primaryKeys;
    private readonly string _signature;

    public CompileTimeCatalog(Dictionary<string, string> columnIndexes, HashSet<string> primaryKeys)
    {
        _columnIndexes = columnIndexes;
        _primaryKeys = primaryKeys;
        _signature = BuildSignature(columnIndexes, primaryKeys);
    }

    public static string Key(string table, string column) => table + "|" + column;

    public bool TryResolveSingleColumnIndex(string table, string column, out string indexName)
        => _columnIndexes.TryGetValue(Key(table, column), out indexName!);

    public bool IsPrimaryKey(string table, string column)
        => _primaryKeys.Contains(Key(table, column));

    public bool Equals(CompileTimeCatalog? other) => other is not null && _signature == other._signature;

    public override bool Equals(object? obj) => Equals(obj as CompileTimeCatalog);

    public override int GetHashCode() => _signature.GetHashCode();

    private static string BuildSignature(Dictionary<string, string> columnIndexes, HashSet<string> primaryKeys)
    {
        var sb = new StringBuilder();
        foreach (KeyValuePair<string, string> pair in columnIndexes.OrderBy(p => p.Key, StringComparer.OrdinalIgnoreCase))
        {
            sb.Append("I:").Append(pair.Key.ToLowerInvariant()).Append('=').Append(pair.Value.ToLowerInvariant()).Append(';');
        }

        foreach (string pk in primaryKeys.OrderBy(p => p, StringComparer.OrdinalIgnoreCase))
        {
            sb.Append("P:").Append(pk.ToLowerInvariant()).Append(';');
        }

        return sb.ToString();
    }
}
```

- [ ] **Step 4: Create `DataVoDdlManifestParser`**

Create `DataVo.Generators/Sql/DataVoDdlManifestParser.cs`:

```csharp
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text.RegularExpressions;

namespace DataVo.Generators.Sql;

/// <summary>
/// Minimal regex parser for the DDL schema manifest, mirroring <see cref="DataVoQueryShapeParser"/>. Recognizes
/// single-column <c>CREATE TABLE … PRIMARY KEY</c> (inline or table-constraint) and single-column
/// <c>CREATE [UNIQUE] INDEX … ON t (col)</c>. Composite indexes/keys and unrecognized statements are ignored
/// (they degrade safely to <see cref="CompiledAccessPath"/> RuntimeResolve at emit time).
/// </summary>
internal static class DataVoDdlManifestParser
{
    private static readonly Regex CreateTableRegex = new(
        @"CREATE\s+TABLE\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)\s*\((?<body>.*)\)",
        RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);

    private static readonly Regex CreateIndexRegex = new(
        @"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s+ON\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(?<col>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex TableConstraintPrimaryKeyRegex = new(
        @"PRIMARY\s+KEY\s*\(\s*(?<col>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex InlinePrimaryKeyRegex = new(
        @"(?<col>[A-Za-z_][A-Za-z0-9_]*)\s+[^,]*?PRIMARY\s+KEY",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static CompileTimeCatalog Parse(ImmutableArray<string> manifestTexts)
    {
        var columnIndexes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var primaryKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (string text in manifestTexts)
        {
            if (string.IsNullOrWhiteSpace(text))
            {
                continue;
            }

            foreach (string raw in text.Split(';'))
            {
                string statement = raw.Trim();
                if (statement.Length == 0)
                {
                    continue;
                }

                Match index = CreateIndexRegex.Match(statement);
                if (index.Success)
                {
                    columnIndexes[CompileTimeCatalog.Key(index.Groups["table"].Value, index.Groups["col"].Value)] =
                        index.Groups["name"].Value;
                    continue;
                }

                Match table = CreateTableRegex.Match(statement);
                if (table.Success)
                {
                    string? pk = ResolvePrimaryKeyColumn(table.Groups["body"].Value);
                    if (pk is not null)
                    {
                        primaryKeys.Add(CompileTimeCatalog.Key(table.Groups["table"].Value, pk));
                    }
                }
            }
        }

        return columnIndexes.Count == 0 && primaryKeys.Count == 0
            ? CompileTimeCatalog.Empty
            : new CompileTimeCatalog(columnIndexes, primaryKeys);
    }

    private static string? ResolvePrimaryKeyColumn(string tableBody)
    {
        Match constraint = TableConstraintPrimaryKeyRegex.Match(tableBody);
        if (constraint.Success)
        {
            return constraint.Groups["col"].Value;
        }

        Match inline = InlinePrimaryKeyRegex.Match(tableBody);
        return inline.Success ? inline.Groups["col"].Value : null;
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj --filter "FullyQualifiedName~DataVoDdlManifestParserTests"`
Expected: PASS (7 tests).

- [ ] **Step 6: Commit**

```bash
git add DataVo.Generators/Sql/CompileTimeCatalog.cs DataVo.Generators/Sql/DataVoDdlManifestParser.cs DataVo.Generators.Tests/DataVoDdlManifestParserTests.cs
git commit -m "feat(generators): compile-time catalog + minimal DDL manifest parser

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 4: Wire the catalog into the generator + tag SelectMany

**Files:**
- Modify: `DataVo.Generators/DataVoQueryGenerator.cs`
- Modify: `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`

**Interfaces:**
- Consumes: `CompileTimeCatalog`, `DataVoDdlManifestParser.Parse` (Task 3); the emitted text references `global::DataVo.Core.CompiledQueries.CompiledAccessPath.SingleColumnIndex` and the `SelectMany(..., accessPath:, resolvedIndexName:)` factory (Task 1).
- Produces: generated `SelectMany` plan text carrying the tag when the catalog resolves the `WHERE` column to a single-column index; untagged otherwise. Extended `RunGenerator(string source, string? manifest = null, bool markAsManifest = true)` test helper.

- [ ] **Step 1: Write the failing tests**

In `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`, add these `using` directives at the top (below the existing ones):

```csharp
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Text;
```

Add these test methods inside the `DataVoQueryGeneratorTests` class:

```csharp
    private const string OrderItemsSource = """
        using System.Collections.Generic;
        using DataVo.Core;
        using DataVo.Core.CompiledQueries;

        public sealed record OrderItemRow(int OrderId, string Sku);

        public static partial class OrderQueries
        {
            [DataVoQuery("SELECT OrderId, Sku FROM OrderItems WHERE OrderId = @orderId")]
            public static partial IReadOnlyList<OrderItemRow> LoadItems(DataVoContext db, int orderId);
        }
        """;

    [Fact]
    public void Generator_WithManifestIndex_EmitsSingleColumnIndexTaggedSelectMany()
    {
        const string manifest = """
            CREATE TABLE OrderItems (OrderItemId INT PRIMARY KEY, OrderId INT, Sku VARCHAR(50));
            CREATE INDEX ix_OrderItems_OrderId ON OrderItems (OrderId);
            """;

        GeneratorDriverRunResult result = RunGenerator(OrderItemsSource, manifest);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("global::DataVo.Core.CompiledQueries.CompiledAccessPath.SingleColumnIndex", generated);
        Assert.Contains("resolvedIndexName: \"ix_OrderItems_OrderId\"", generated);
    }

    [Fact]
    public void Generator_WithoutManifest_EmitsUntaggedSelectMany()
    {
        GeneratorDriverRunResult result = RunGenerator(OrderItemsSource);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("DataVoCompiledQueryPlan.SelectMany", generated);
        Assert.DoesNotContain("CompiledAccessPath.SingleColumnIndex", generated);
    }

    [Fact]
    public void Generator_ManifestColumnNotIndexed_EmitsUntaggedSelectMany()
    {
        const string manifest = "CREATE TABLE OrderItems (OrderItemId INT PRIMARY KEY, OrderId INT, Sku VARCHAR(50));";

        GeneratorDriverRunResult result = RunGenerator(OrderItemsSource, manifest);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.DoesNotContain("CompiledAccessPath.SingleColumnIndex", generated);
    }

    [Fact]
    public void Generator_ManifestFileNotMarked_IsIgnored_EmitsUntagged()
    {
        const string manifest = "CREATE INDEX ix_OrderItems_OrderId ON OrderItems (OrderId);";

        GeneratorDriverRunResult result = RunGenerator(OrderItemsSource, manifest, markAsManifest: false);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.DoesNotContain("CompiledAccessPath.SingleColumnIndex", generated);
    }
```

Replace the existing `RunGenerator` method with this version (adds optional manifest injection + analyzer-config options), and add the three test-double types below it:

```csharp
    private static GeneratorDriverRunResult RunGenerator(string source, string? manifest = null, bool markAsManifest = true)
    {
        CSharpCompilation compilation = CSharpCompilation.Create(
            "GeneratorTest",
            [CSharpSyntaxTree.ParseText(source)],
            GetMetadataReferences(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var additionalTexts = new List<AdditionalText>();
        var fileOptions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (manifest is not null)
        {
            additionalTexts.Add(new InMemoryAdditionalText("schema.sql", manifest));
            if (markAsManifest)
            {
                fileOptions["build_metadata.AdditionalFiles.DataVoSchemaManifest"] = "true";
            }
        }

        var optionsProvider = new TestAnalyzerConfigOptionsProvider(new DictionaryOptions(fileOptions));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            [new DataVoQueryGenerator().AsSourceGenerator()],
            additionalTexts,
            parseOptions: null,
            optionsProvider: optionsProvider);
        driver = driver.RunGenerators(compilation);
        return driver.GetRunResult();
    }

    private sealed class InMemoryAdditionalText : AdditionalText
    {
        private readonly string _text;

        public InMemoryAdditionalText(string path, string text)
        {
            Path = path;
            _text = text;
        }

        public override string Path { get; }

        public override SourceText GetText(CancellationToken cancellationToken = default)
            => SourceText.From(_text, Encoding.UTF8);
    }

    private sealed class TestAnalyzerConfigOptionsProvider : AnalyzerConfigOptionsProvider
    {
        private static readonly DictionaryOptions EmptyOptions = new(new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
        private readonly AnalyzerConfigOptions _fileOptions;

        public TestAnalyzerConfigOptionsProvider(AnalyzerConfigOptions fileOptions) => _fileOptions = fileOptions;

        public override AnalyzerConfigOptions GlobalOptions => EmptyOptions;

        public override AnalyzerConfigOptions GetOptions(SyntaxTree tree) => EmptyOptions;

        public override AnalyzerConfigOptions GetOptions(AdditionalText textFile) => _fileOptions;
    }

    private sealed class DictionaryOptions : AnalyzerConfigOptions
    {
        private readonly Dictionary<string, string> _values;

        public DictionaryOptions(Dictionary<string, string> values) => _values = values;

        public override bool TryGetValue(string key, out string value) => _values.TryGetValue(key, out value!);
    }
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj --filter "FullyQualifiedName~DataVoQueryGeneratorTests"`
Expected: `Generator_WithManifestIndex_EmitsSingleColumnIndexTaggedSelectMany` FAILS (`DoesNotContain` → `Contains` assertion fails: the generator does not yet read the manifest, so no tag is emitted). The untagged tests pass.

- [ ] **Step 3: Wire the catalog provider in `Initialize`**

In `DataVo.Generators/DataVoQueryGenerator.cs`, add these `using` directives at the top (with the existing ones):

```csharp
using System.Collections.Immutable;
using DataVo.Generators.Sql;
using Microsoft.CodeAnalysis.Diagnostics;
```

(`using DataVo.Generators.Sql;` already exists — do not duplicate it.)

Replace the entire `Initialize` method with:

```csharp
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        IncrementalValuesProvider<MethodDeclarationSyntax> methods = context.SyntaxProvider
            .CreateSyntaxProvider(
                static (node, _) => node is MethodDeclarationSyntax method && method.AttributeLists.Count > 0,
                static (ctx, _) => (MethodDeclarationSyntax)ctx.Node)
            .Where(static method => method.Modifiers.Any(SyntaxKind.PartialKeyword));

        IncrementalValueProvider<Compilation> compilation = context.CompilationProvider;

        // Compile-time schema catalog built from AdditionalFiles flagged DataVoSchemaManifest="true".
        IncrementalValueProvider<CompileTimeCatalog> catalog = context.AdditionalTextsProvider
            .Combine(context.AnalyzerConfigOptionsProvider)
            .Where(static pair => IsSchemaManifest(pair.Left, pair.Right))
            .Select(static (pair, ct) => pair.Left.GetText(ct)?.ToString() ?? string.Empty)
            .Collect()
            .Select(static (texts, _) => DataVoDdlManifestParser.Parse(texts));

        context.RegisterSourceOutput(
            methods.Combine(compilation).Combine(catalog),
            static (spc, pair) => EmitForMethod(spc, pair.Left.Left, pair.Left.Right, pair.Right));
    }

    private static bool IsSchemaManifest(AdditionalText text, AnalyzerConfigOptionsProvider optionsProvider)
    {
        return optionsProvider.GetOptions(text)
                   .TryGetValue("build_metadata.AdditionalFiles.DataVoSchemaManifest", out string? value)
               && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }
```

- [ ] **Step 4: Thread the catalog through emission and tag SelectMany**

In the same file, change the `EmitForMethod` signature to accept the catalog and pass it to `GenerateMethod`. Replace the signature line:

```csharp
    private static void EmitForMethod(SourceProductionContext context, MethodDeclarationSyntax method, Compilation compilation)
```

with:

```csharp
    private static void EmitForMethod(SourceProductionContext context, MethodDeclarationSyntax method, Compilation compilation, CompileTimeCatalog catalog)
```

In `EmitForMethod`, replace the success-path call:

```csharp
        string source = GenerateMethod(symbol, model);
```

with:

```csharp
        string source = GenerateMethod(symbol, model, catalog);
```

Change the `GenerateMethod` signature:

```csharp
    private static string GenerateMethod(IMethodSymbol method, GeneratedQueryModel model)
```

to:

```csharp
    private static string GenerateMethod(IMethodSymbol method, GeneratedQueryModel model, CompileTimeCatalog catalog)
```

Inside `GenerateMethod`, replace the plan-field line:

```csharp
        builder.AppendLine($"    private static readonly global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan {planName} = {GeneratePlan(method, model)};");
```

with:

```csharp
        builder.AppendLine($"    private static readonly global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan {planName} = {GeneratePlan(method, model, catalog)};");
```

Change the `GeneratePlan` signature:

```csharp
    private static string GeneratePlan(IMethodSymbol method, GeneratedQueryModel model)
```

to:

```csharp
    private static string GeneratePlan(IMethodSymbol method, GeneratedQueryModel model, CompileTimeCatalog catalog)
```

In `GeneratePlan`, replace the `SelectMany` switch arm:

```csharp
            GeneratedExecutionShape.SelectMany => $"global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan.SelectMany(\"{model.TableName}\", new string[] {{ {QuoteList(model.ProjectedColumns)} }}, \"{model.WhereColumn}\", \"{model.WhereParameterName}\")",
```

with:

```csharp
            GeneratedExecutionShape.SelectMany => GenerateSelectManyPlan(model, catalog),
```

Add this helper method to the class (e.g., immediately after `GeneratePlan`):

```csharp
    private static string GenerateSelectManyPlan(GeneratedQueryModel model, CompileTimeCatalog catalog)
    {
        string baseArguments =
            $"\"{model.TableName}\", new string[] {{ {QuoteList(model.ProjectedColumns)} }}, \"{model.WhereColumn}\", \"{model.WhereParameterName}\"";

        if (catalog.TryResolveSingleColumnIndex(model.TableName, model.WhereColumn!, out string indexName))
        {
            return $"global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan.SelectMany({baseArguments}, accessPath: global::DataVo.Core.CompiledQueries.CompiledAccessPath.SingleColumnIndex, resolvedIndexName: \"{indexName}\")";
        }

        return $"global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan.SelectMany({baseArguments})";
    }
```

- [ ] **Step 5: Run the generator tests to verify they pass**

Run: `dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj --filter "FullyQualifiedName~DataVoQueryGeneratorTests"`
Expected: PASS (the four new tests + all pre-existing generator tests — the existing `Generator_EmitsSelectManyImplementation` still passes because, with no manifest, the catalog is `Empty` and `SelectMany` stays untagged).

- [ ] **Step 6: Commit**

```bash
git add DataVo.Generators/DataVoQueryGenerator.cs DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs
git commit -m "feat(generators): resolve single-column index from manifest and tag SelectMany plans

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

> **Consuming-project note (documentation, not a code task):** a real consumer enables this by adding to its `.csproj`:
> `<AdditionalFiles Include="schema.sql" DataVoSchemaManifest="true" />` and
> `<CompilerVisibleItemMetadata Include="AdditionalFiles" MetadataName="DataVoSchemaManifest" />`.
> The generator tests simulate that wiring via the injected `AnalyzerConfigOptionsProvider`.

---

### Task 5: Allocation microbenchmark (per-call catalog re-derivation removed)

**Files:**
- Test: `DataVo.Tests/E2E/CompiledAccessPathTests.cs` (append)

**Interfaces:**
- Consumes: the Task 2 helpers (`CreateContext`, `SeedPlayers`, `QueryByName`) already present in this file; `GC.GetAllocatedBytesForCurrentThread()`.

- [ ] **Step 1: Write the failing test**

Append this method inside the `CompiledAccessPathTests` class in `DataVo.Tests/E2E/CompiledAccessPathTests.cs`:

```csharp
    [Fact]
    public void TaggedPath_AllocatesLessPerCallThanRuntimeResolve()
    {
        // Both plans materialize the same rows through the same index; the only difference is that the tagged
        // path skips GetTablePrimaryKeys (a List<string>) and the GetTableIndexes catalog scan on every call.
        // Over many iterations that constant-factor re-derivation must show up as strictly lower allocation.
        const int iterations = 2_000;

        using var context = CreateContext();
        SeedPlayers(context);
        context.Execute("CREATE INDEX ix_players_name ON Players (Name)");

        DataVoCompiledQueryPlan tagged = DataVoCompiledQueryPlan.SelectMany(
            "Players", ["Id", "Name", "Level"], "Name", "name",
            accessPath: CompiledAccessPath.SingleColumnIndex,
            resolvedIndexName: "ix_players_name");
        DataVoCompiledQueryPlan runtimeResolve = DataVoCompiledQueryPlan.SelectMany(
            "Players", ["Id", "Name", "Level"], "Name", "name");

        // Warm up both paths so one-time allocations are excluded from the measurement.
        QueryByName(context, tagged, "Ada");
        QueryByName(context, runtimeResolve, "Ada");

        long runtimeBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++)
        {
            QueryByName(context, runtimeResolve, "Ada");
        }
        long runtimeBytes = GC.GetAllocatedBytesForCurrentThread() - runtimeBefore;

        long taggedBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++)
        {
            QueryByName(context, tagged, "Ada");
        }
        long taggedBytes = GC.GetAllocatedBytesForCurrentThread() - taggedBefore;

        Assert.True(
            taggedBytes < runtimeBytes,
            $"Expected tagged path to allocate less than RuntimeResolve over {iterations} calls; " +
            $"tagged={taggedBytes} B, runtime={runtimeBytes} B.");
    }
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~CompiledAccessPathTests.TaggedPath_AllocatesLessPerCallThanRuntimeResolve"`
Expected: PASS. (The runtime branch from Task 2 already makes the tagged path skip `GetTablePrimaryKeys` + `GetTableIndexes`, so it allocates strictly less.)

> If this assertion proves flaky on the CI machine (allocation noise), raise `iterations` to `10_000` before weakening the assertion — do not change `<` to a tolerance band without evidence the delta is genuinely within noise.

- [ ] **Step 3: Commit**

```bash
git add DataVo.Tests/E2E/CompiledAccessPathTests.cs
git commit -m "test(query): allocation microbenchmark proves tagged path skips catalog re-derivation

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 6: Full-suite verification

**Files:** none (verification + final commit only).

- [ ] **Step 1: Build the whole solution with warnings as errors**

Run: `dotnet build DataVo.sln -c Release -warnaserror`
Expected: `Build succeeded. 0 Warning(s) 0 Error(s)`. If any warning appears, fix it before continuing.

- [ ] **Step 2: Run the full test suite**

Run: `dotnet test DataVo.sln -c Release`
Expected: all test projects pass (the prior suite count plus the new tests: 3 plan-tag + 3 runtime + 1 alloc in `DataVo.Tests`, 7 parser + 4 generator in `DataVo.Generators.Tests`). Zero failures.

- [ ] **Step 3: Confirm no stray files / clean status**

Run: `git status -s`
Expected: only the pre-existing untracked `.DS_Store` / `test.md` entries; no uncommitted tracked changes.

- [ ] **Step 4: AOT-cleanliness note**

No action unless a publish check exists. The generated code is plain method calls into `DataVoCompiledQuery`/`DataVoCompiledQueryPlan` with no reflection, so the slice is AOT-clean by construction. If a native publish smoke target exists in the repo, run it; otherwise this is satisfied by the `-warnaserror` build above (which would surface AOT/trim analyzer warnings if the engine has them enabled).

The PoC is complete on `feature/roslyn-compile-time-access-path-poc`. Do **not** merge to `master` — the user merges manually (as with the HNSW work).

## Self-Review

**1. Spec coverage:**
- Manifest ingestion + `CompileTimeCatalog` → Task 3. ✓
- Access-path resolution for `SelectMany` single-column equality → Task 4 (`GenerateSelectManyPlan`). ✓
- Backward-compatible `CompiledAccessPath` tag on the plan → Task 1. ✓
- Runtime honoring, skipping catalog lookups → Task 2 (branch before `GetTablePrimaryKeys`). ✓
- Safe `IndexException`/empty fallthrough → Task 2 (`TaggedWithNonexistentIndex_FallsBackToCorrectResults`, `catch (IndexException)`). ✓
- Tests: parser unit (Task 3), generator string assertions (Task 4), runtime parity + fallback (Task 2), alloc microbench (Task 5). ✓
- Scope boundary (no Layer 2 / DV1001 / restructure / other shapes) honored — no task touches `ToParameterDictionary`/`BuildComparisonKey`, diagnostics, the `Initialize` pipeline beyond a `.Combine`, or `SelectSingle`/`Insert`/`Update`. ✓
- Quality bars (0 warnings, AOT, full suite) → Task 6. ✓

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to" — every code step contains complete code. ✓

**3. Type consistency:** `CompiledAccessPath` (Task 1) referenced identically in Tasks 2/4/5. `CompileTimeCatalog.TryResolveSingleColumnIndex(string, string, out string)` defined in Task 3, called in Tasks 3-test/4. `CompileTimeCatalog.Key(string, string)` defined in Task 3 and used by the parser in the same task. `DataVoDdlManifestParser.Parse(ImmutableArray<string>)` defined in Task 3, called in Task 4's `Initialize`. `GenerateSelectManyPlan(GeneratedQueryModel, CompileTimeCatalog)` defined and called in Task 4. `RunGenerator(string, string?, bool)` redefined once in Task 4 and used by its tests. `QueryByName`/`SeedPlayers`/`CreateContext` defined in Task 2, reused in Task 5. Emitted token `resolvedIndexName: "…"` matches the Task 4 assertion. ✓
