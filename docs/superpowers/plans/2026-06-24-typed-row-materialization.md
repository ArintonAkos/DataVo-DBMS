# Compile-Time Typed Row Materialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the source generator emit a strongly-typed, zero-boxing projector so generated `SelectSingle`/`SelectMany` queries return rows with zero per-row heap allocation (no `Dictionary<string,object?>` materialization, no projection dict, no boxing).

**Architecture:** A public `CompiledRowReader` ref struct (wrapping the internal `StoredRowView`) + a cached-static `CompiledRowMapper<T>` delegate let the generator emit a typed projector. A shared `TryReadMatchingStoredRows` finder delivers `StoredRow` to new `SelectManyTyped`/`SelectSingleTyped` executors, while the existing dict path is preserved by mapping the same finder's output through `MaterializeStoredRow`. Fail-fast: no runtime fallback.

**Tech Stack:** C# / .NET 10 (`DataVo.Core`), Roslyn incremental generator (`netstandard2.0`), xUnit.

**Spec:** `docs/superpowers/specs/2026-06-24-typed-row-materialization-design.md`
**Branch:** `feature/roslyn-compile-time-access-path-poc` (already checked out)

## Global Constraints

- **`DataVo.Generators` targets `netstandard2.0`**, references only `Microsoft.CodeAnalysis.CSharp`, and MUST NOT reference `DataVo.Core`. It emits the typed projector as text referencing public Core types.
- **`StoredRow`/`StoredRowView` are `internal`; `StoredRowView` is a `ref struct`.** The public reader has an **internal constructor** (only Core builds it). The mapper is a **custom delegate** (a `ref struct` param is illegal on `Func<>`).
- **Fail-fast, no runtime fallback.** Typed and dict paths are result-identical on valid data and fail-equivalent on invalid data (`CellValue.AsX()` throws on type mismatch / NULL-into-non-nullable, exactly as the dict path's casts do).
- **Generation-time fallback only.** Not a clean ctor-name match, or any ctor param type outside the supported set → emit today's dict mapper for that query. No diagnostic.
- **Storable SQL column types** (`InsertRowService.cs:248`): `INT`→Int32, `FLOAT`→Double, `BIT`→Boolean, `DATE`→Date, `VARCHAR`→String, `VECTOR`→float[]. **Int64/Decimal have no storable column type** — their getters/mappings are implemented and emission-tested, but end-to-end tests use the storable set.
- **Quality bars:** PoC code builds with 0 warnings, AOT-clean (delegate + ref struct + typed `CellValue` access; no reflection), full suite green.

## File Structure

**Create:**
- `DataVo.Core/CompiledQueries/CompiledRowReader.cs` — public `ref struct`, zero-boxing typed getters by column.
- `DataVo.Core/CompiledQueries/CompiledRowMapper.cs` — `public delegate T CompiledRowMapper<T>(CompiledRowReader)`.

**Modify:**
- `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs` — `TryReadMatchingStoredRows`/`ReadStoredRowsViaIndex` refactor; `SelectManyTyped`/`SelectSingleTyped`/`ExecuteSelectTyped`.
- `DataVo.Generators/DataVoQueryGenerator.cs` — typed emission (clean-ctor-match detection, type→getter, emit Map + delegate + Typed invocation; else dict fallback).
- `DataVo.Tests/E2E/CompiledAccessPathTests.cs` — typed parity + zero-allocation tests.
- `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs` — typed emission tests.
- `DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests.cs` — typed E2E (more types + NULL).

---

### Task 1: Refactor the finder to return `StoredRow` (no behavior change)

**Files:**
- Modify: `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs` (`TryReadMatchingRowEntries`, `ReadRowsViaIndex`)

**Interfaces:**
- Produces: `private static List<KeyValuePair<long, StoredRow>> TryReadMatchingStoredRows(DataVoContext, DataVoCompiledQueryPlan, string, string)` and `private static List<KeyValuePair<long, StoredRow>> ReadStoredRowsViaIndex(DataVoContext, DataVoCompiledQueryPlan, string, string, string)`. `TryReadMatchingRowEntries` keeps its existing signature/behavior, now delegating to the finder + `MaterializeStoredRow`.

This is a pure refactor: it extracts row-finding (returning `StoredRow`) from dict-materialization. The existing test suites are the regression guard.

- [ ] **Step 1: Replace `TryReadMatchingRowEntries` and `ReadRowsViaIndex` with the StoredRow finder**

In `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`, replace the entire body of `TryReadMatchingRowEntries` (the method starting `private static List<KeyValuePair<long, Dictionary<string, object?>>> TryReadMatchingRowEntries(`, through its closing brace) **and** the `ReadRowsViaIndex` method that follows it, with:

```csharp
    private static List<KeyValuePair<long, Dictionary<string, object?>>> TryReadMatchingRowEntries(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string expectedKey)
    {
        return TryReadMatchingStoredRows(context, plan, databaseName, expectedKey)
            .Select(static entry => new KeyValuePair<long, Dictionary<string, object?>>(
                entry.Key,
                MaterializeStoredRow(entry.Value)))
            .ToList();
    }

    // Shared finder: resolves matching rows via the compile-time tag, then primary key, then a single-column
    // secondary index, then a typed full scan — returning the StoredRow itself so callers choose how to read it
    // (dictionary materialization for the legacy path, typed projection for the compiled path). Behavior is
    // identical to the previous TryReadMatchingRowEntries; only the return element type changed (StoredRow
    // instead of an already-materialized dictionary).
    private static List<KeyValuePair<long, StoredRow>> TryReadMatchingStoredRows(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string expectedKey)
    {
        if (plan.AccessPath == CompiledAccessPath.SingleColumnIndex && plan.ResolvedIndexName is not null)
        {
            try
            {
                List<KeyValuePair<long, StoredRow>> tagged =
                    ReadStoredRowsViaIndex(context, plan, databaseName, plan.ResolvedIndexName, expectedKey);

                if (tagged.Count > 0)
                {
                    return tagged;
                }
            }
            catch (IndexException)
            {
            }
        }

        List<string> primaryKeys = context.Engine.Catalog.GetTablePrimaryKeys(plan.TableName, databaseName);
        bool isPrimaryKeyPredicate = primaryKeys.Contains(plan.WhereColumn!, StringComparer.OrdinalIgnoreCase);

        if (isPrimaryKeyPredicate)
        {
            string primaryKeyIndexName = $"_PK_{plan.TableName}";

            try
            {
                List<KeyValuePair<long, StoredRow>> matches =
                    ReadStoredRowsViaIndex(context, plan, databaseName, primaryKeyIndexName, expectedKey);

                if (matches.Count > 0)
                {
                    return matches;
                }
            }
            catch (IndexException ex) when (IsMissingPrimaryKeyIndex(ex, primaryKeyIndexName, plan.TableName))
            {
            }
        }
        else if (TryResolveSingleColumnIndex(context, plan.TableName, databaseName, plan.WhereColumn!, out string secondaryIndexName))
        {
            try
            {
                List<KeyValuePair<long, StoredRow>> matches =
                    ReadStoredRowsViaIndex(context, plan, databaseName, secondaryIndexName, expectedKey);

                if (matches.Count > 0)
                {
                    return matches;
                }
            }
            catch (IndexException)
            {
            }
        }

        Dictionary<long, StoredRow> scanned =
            context.Engine.StorageContext.GetTypedTableContents(plan.TableName, databaseName);

        string[] whereColumns = [plan.WhereColumn!];
        var scannedMatches = new List<KeyValuePair<long, StoredRow>>();
        foreach ((long rowId, StoredRow row) in scanned)
        {
            StoredRowView view = row.AsView();
            if (!view.Schema.TryGetOrdinal(plan.WhereColumn!, out _))
            {
                continue;
            }

            if (!string.Equals(
                    IndexKeyEncoder.BuildKeyString(view.Schema, view.Cells, whereColumns),
                    expectedKey,
                    StringComparison.Ordinal))
            {
                continue;
            }

            scannedMatches.Add(new KeyValuePair<long, StoredRow>(rowId, row));
        }

        return scannedMatches;
    }

    /// <summary>
    /// Reads the StoredRows whose IDs the named B-Tree index returns for <paramref name="expectedKey"/>.
    /// Shared by the tag, primary-key, and secondary-index access paths.
    /// </summary>
    private static List<KeyValuePair<long, StoredRow>> ReadStoredRowsViaIndex(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string indexName,
        string expectedKey)
    {
        List<long> ids =
        [
            .. context.Engine.IndexManager.FilterUsingIndex(expectedKey, indexName, plan.TableName, databaseName)
        ];

        Dictionary<long, StoredRow> indexedRows =
            context.Engine.StorageContext.GetTypedTableContents(ids, plan.TableName, databaseName);

        return ids
            .Where(indexedRows.ContainsKey)
            .Select(id => new KeyValuePair<long, StoredRow>(id, indexedRows[id]))
            .ToList();
    }
```

(The old tagged-path branch, the PK/secondary branches, the scan, and `ReadRowsViaIndex` are now folded into the two methods above. `MaterializeStoredRow`, `TryResolveSingleColumnIndex`, `IsMissingPrimaryKeyIndex` are unchanged and still present.)

- [ ] **Step 2: Build to confirm it compiles**

Run: `dotnet build DataVo.Core/DataVo.Core.csproj -c Debug 2>&1 | grep -E 'error|Build succeeded'`
Expected: `Build succeeded.` (If `error CS0103: ReadRowsViaIndex` appears, a caller of the old method remained — there are none beyond the three branches just replaced.)

- [ ] **Step 3: Run the existing compiled-query suites (regression guard)**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj -c Debug --filter "FullyQualifiedName~CompiledQueryRuntimeTests|FullyQualifiedName~CompiledAccessPathTests|FullyQualifiedName~SourceGeneratedCompiledQueryTests"`
Expected: PASS (all green — behavior is unchanged; only the internal finder was extracted).

- [ ] **Step 4: Commit**

```bash
git add DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs
git commit -m "refactor(query): extract TryReadMatchingStoredRows finder (StoredRow, not dict)

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 2: `CompiledRowReader`, `CompiledRowMapper<T>`, and the typed executors

**Files:**
- Create: `DataVo.Core/CompiledQueries/CompiledRowReader.cs`
- Create: `DataVo.Core/CompiledQueries/CompiledRowMapper.cs`
- Modify: `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`
- Test: `DataVo.Tests/E2E/CompiledAccessPathTests.cs`

**Interfaces:**
- Consumes: `TryReadMatchingStoredRows` (Task 1), `StoredRowView`, `CellValue`.
- Produces: `public readonly ref struct CompiledRowReader` with `internal CompiledRowReader(StoredRowView)` and getters `GetInt32/GetInt64/GetDouble/GetDecimal/GetBoolean/GetDate/GetVector(string)`, `GetString(string)→string?`, `GetInt32OrNull/GetInt64OrNull/GetDoubleOrNull/GetDecimalOrNull/GetBooleanOrNull/GetDateOrNull(string)`, `IsNull(string)`; `public delegate T CompiledRowMapper<T>(CompiledRowReader)`; `public static IReadOnlyList<T> DataVoCompiledQuery.SelectManyTyped<T>(DataVoContext, DataVoCompiledQueryPlan, IReadOnlyList<DataVoCompiledQueryParameter>, CompiledRowMapper<T>)`; `public static T? DataVoCompiledQuery.SelectSingleTyped<T>(…)`.

- [ ] **Step 1: Write the failing tests**

Append to `DataVo.Tests/E2E/CompiledAccessPathTests.cs` (inside the `CompiledAccessPathTests` class). These call the public typed API with hand-written mappers (the consumer's role), exercising the reader end-to-end:

```csharp
    public sealed record Hit(int Id, string Name, double Score);

    private static Hit MapHit(CompiledRowReader r) => new(r.GetInt32("Id"), r.GetString("Name")!, r.GetDouble("Score"));

    private static void SeedHits(DataVoContext context)
    {
        context.Execute("CREATE TABLE Hits (Id INT PRIMARY KEY, Name VARCHAR(50), Score FLOAT)");
        context.BulkInsert(
            "Hits",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Score"] = 1.5 },
                new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Grace", ["Score"] = 2.5 },
                new Dictionary<string, object?> { ["Id"] = 3, ["Name"] = "Ada", ["Score"] = 3.5 }
            ]);
    }

    [Fact]
    public void SelectManyTyped_ReturnsSameRowsAsDictPath()
    {
        using var context = CreateContext();
        SeedHits(context);
        context.Execute("CREATE INDEX ix_hits_name ON Hits (Name)");

        var plan = DataVoCompiledQueryPlan.SelectMany(
            "Hits", ["Id", "Name", "Score"], "Name", "name",
            accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: "ix_hits_name");

        IReadOnlyList<Hit> typed = DataVoCompiledQuery.SelectManyTyped<Hit>(
            context, plan, [new DataVoCompiledQueryParameter("name", "Ada")], MapHit);

        IReadOnlyList<Hit> dict = DataVoCompiledQuery.SelectMany<Hit>(
            context,
            DataVoCompiledQueryPlan.SelectMany("Hits", ["Id", "Name", "Score"], "Name", "name"),
            [new DataVoCompiledQueryParameter("name", "Ada")],
            static row => new Hit((int)row["Id"]!, (string)row["Name"]!, (double)row["Score"]!));

        Assert.Equal(dict.OrderBy(h => h.Id), typed.OrderBy(h => h.Id));
        Assert.Equal(new[] { new Hit(1, "Ada", 1.5), new Hit(3, "Ada", 3.5) }, typed.OrderBy(h => h.Id));
    }

    [Fact]
    public void SelectSingleTyped_ReturnsFirstMatchOrDefault()
    {
        using var context = CreateContext();
        SeedHits(context);

        Hit? hit = DataVoCompiledQuery.SelectSingleTyped<Hit>(
            context,
            DataVoCompiledQueryPlan.SelectSingle("Hits", ["Id", "Name", "Score"], "Id", "id"),
            [new DataVoCompiledQueryParameter("id", 2)],
            MapHit);
        Assert.Equal(new Hit(2, "Grace", 2.5), hit);

        Hit? none = DataVoCompiledQuery.SelectSingleTyped<Hit>(
            context,
            DataVoCompiledQueryPlan.SelectSingle("Hits", ["Id", "Name", "Score"], "Id", "id"),
            [new DataVoCompiledQueryParameter("id", 999)],
            MapHit);
        Assert.Null(none);
    }

    [Fact]
    public void SelectManyTyped_NullableColumn_ReadsNull()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Notes (Id INT PRIMARY KEY, Body VARCHAR(50))");
        context.BulkInsert(
            "Notes",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Body"] = null },
                new Dictionary<string, object?> { ["Id"] = 2, ["Body"] = "hello" }
            ]);

        IReadOnlyList<(int, string?)> rows = DataVoCompiledQuery.SelectManyTyped<(int, string?)>(
            context,
            DataVoCompiledQueryPlan.SelectMany("Notes", ["Id", "Body"], "Id", "id"),
            [new DataVoCompiledQueryParameter("id", 1)],
            static r => (r.GetInt32("Id"), r.GetString("Body")));

        Assert.Equal((1, (string?)null), Assert.Single(rows));
    }

    [Fact]
    public void SelectManyTyped_TypeMismatch_ThrowsFailFast()
    {
        using var context = CreateContext();
        SeedHits(context);

        // "Name" is a string column; reading it as int must throw (fail-fast), not silently fall back.
        Assert.ThrowsAny<InvalidOperationException>(() => DataVoCompiledQuery.SelectManyTyped<int>(
            context,
            DataVoCompiledQueryPlan.SelectMany("Hits", ["Id", "Name", "Score"], "Id", "id"),
            [new DataVoCompiledQueryParameter("id", 1)],
            static r => r.GetInt32("Name")));
    }

    [Fact]
    public void SelectManyTyped_AllocatesFarLessPerRowThanDictPath()
    {
        const int iterations = 2_000;
        using var context = CreateContext();
        // 1 row matching "m1", 8 rows matching "m8"; differencing isolates per-row allocation.
        context.Execute("CREATE TABLE Bench (Id INT PRIMARY KEY, Tag VARCHAR(20), Score FLOAT)");
        var seed = new List<Dictionary<string, object?>> { new() { ["Id"] = 1, ["Tag"] = "m1", ["Score"] = 1.0 } };
        for (int i = 0; i < 8; i++)
        {
            seed.Add(new Dictionary<string, object?> { ["Id"] = i + 2, ["Tag"] = "m8", ["Score"] = 2.0 });
        }
        context.BulkInsert("Bench", seed);
        context.Execute("CREATE INDEX ix_bench_tag ON Bench (Tag)");

        var typedPlan = DataVoCompiledQueryPlan.SelectMany(
            "Bench", ["Id", "Tag", "Score"], "Tag", "tag",
            accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: "ix_bench_tag");
        var dictPlan = DataVoCompiledQueryPlan.SelectMany(
            "Bench", ["Id", "Tag", "Score"], "Tag", "tag",
            accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: "ix_bench_tag");

        static IReadOnlyList<Hit> Typed(DataVoContext c, DataVoCompiledQueryPlan p, string tag)
            => DataVoCompiledQuery.SelectManyTyped<Hit>(c, p, [new DataVoCompiledQueryParameter("tag", tag)],
                static r => new Hit(r.GetInt32("Id"), r.GetString("Tag")!, r.GetDouble("Score")));
        static IReadOnlyList<Hit> Dict(DataVoContext c, DataVoCompiledQueryPlan p, string tag)
            => DataVoCompiledQuery.SelectMany<Hit>(c, p, [new DataVoCompiledQueryParameter("tag", tag)],
                static row => new Hit((int)row["Id"]!, (string)row["Tag"]!, (double)row["Score"]!));

        double typedPerRow = PerRow(() => Typed(context, typedPlan, "m1"), () => Typed(context, typedPlan, "m8"), iterations);
        double dictPerRow = PerRow(() => Dict(context, dictPlan, "m1"), () => Dict(context, dictPlan, "m8"), iterations);

        _output.WriteLine($"typed per-row : {typedPerRow:F0} B/row");
        _output.WriteLine($"dict  per-row : {dictPerRow:F0} B/row");
        _output.WriteLine($"reclaimed     : {dictPerRow - typedPerRow:F0} B/row");

        Assert.True(
            typedPerRow < dictPerRow && (dictPerRow - typedPerRow) > 1_000,
            $"Expected typed per-row far below dict per-row; typed={typedPerRow:F0}, dict={dictPerRow:F0}.");
    }

    private static double PerRow(Func<object> at1, Func<object> at8, int iterations)
    {
        at1(); at8(); // warm up
        long b1 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++) { at1(); }
        double bytes1 = (double)(GC.GetAllocatedBytesForCurrentThread() - b1) / iterations;
        long b8 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++) { at8(); }
        double bytes8 = (double)(GC.GetAllocatedBytesForCurrentThread() - b8) / iterations;
        return (bytes8 - bytes1) / 7.0;
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `dotnet build DataVo.Tests/DataVo.Tests.csproj -c Debug 2>&1 | grep -E 'error CS|Build succeeded' | head`
Expected: COMPILE FAILURE — `CompiledRowReader`, `CompiledRowMapper`, `SelectManyTyped`, `SelectSingleTyped` do not exist.

- [ ] **Step 3: Create `CompiledRowMapper<T>`**

Create `DataVo.Core/CompiledQueries/CompiledRowMapper.cs`:

```csharp
namespace DataVo.Core.CompiledQueries;

/// <summary>
/// Projects one stored row into <typeparamref name="T"/> via a <see cref="CompiledRowReader"/>. A custom
/// delegate (rather than <c>Func&lt;,&gt;</c>) because the reader is a <c>ref struct</c>, which cannot be a
/// generic type argument. Source-generated instances are cached static fields — no per-row allocation.
/// </summary>
public delegate T CompiledRowMapper<T>(CompiledRowReader reader);
```

- [ ] **Step 4: Create `CompiledRowReader`**

Create `DataVo.Core/CompiledQueries/CompiledRowReader.cs`:

```csharp
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.CompiledQueries;

/// <summary>
/// A public, zero-boxing reader over one stored row, addressed by column name. Wraps the internal
/// <see cref="StoredRowView"/> so source-generated projectors (which live in the consumer assembly and cannot
/// see internals) can read typed cells without materializing a dictionary or boxing. Getters fail fast: an
/// unknown column throws <see cref="KeyNotFoundException"/>; a type mismatch or NULL into a non-nullable getter
/// throws (mirroring the dictionary path's casts).
/// </summary>
public readonly ref struct CompiledRowReader
{
    private readonly StoredRowView _view;

    internal CompiledRowReader(StoredRowView view) => _view = view;

    /// <summary>Whether the named column holds SQL NULL.</summary>
    public bool IsNull(string column) => _view[column].IsNull;

    public int GetInt32(string column) => _view[column].AsInt32();
    public long GetInt64(string column) => _view[column].AsInt64();
    public double GetDouble(string column) => _view[column].AsDouble();
    public decimal GetDecimal(string column) => _view[column].AsDecimal();
    public bool GetBoolean(string column) => _view[column].AsBoolean();
    public DateOnly GetDate(string column) => _view[column].AsDate();
    public float[] GetVector(string column) => _view[column].AsVector();

    /// <summary>Reads a string column; SQL NULL returns <c>null</c>.</summary>
    public string? GetString(string column) => _view[column].AsString();

    public int? GetInt32OrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsInt32(); }
    public long? GetInt64OrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsInt64(); }
    public double? GetDoubleOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDouble(); }
    public decimal? GetDecimalOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDecimal(); }
    public bool? GetBooleanOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsBoolean(); }
    public DateOnly? GetDateOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDate(); }
}
```

- [ ] **Step 5: Add the typed executors to `DataVoCompiledQuery`**

In `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`, add these methods (e.g., immediately after the existing `SelectMany<TResult>` method):

```csharp
    /// <summary>
    /// Executes a select plan and returns every row projected by <paramref name="mapper"/> directly from typed
    /// cells — no dictionary materialization, no boxing. Behavior matches <see cref="SelectMany{TResult}"/>.
    /// </summary>
    public static IReadOnlyList<T> SelectManyTyped<T>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        CompiledRowMapper<T> mapper)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentNullException.ThrowIfNull(mapper);

        if (plan.Kind != DataVoCompiledQueryKind.SelectMany && plan.Kind != DataVoCompiledQueryKind.SelectSingle)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as SelectMany.");
        }

        return ExecuteSelectTyped(context, plan, parameters, mapper);
    }

    /// <summary>
    /// Executes a select plan and returns the first projected row, or <c>default</c> when none matches.
    /// </summary>
    public static T? SelectSingleTyped<T>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        CompiledRowMapper<T> mapper)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentNullException.ThrowIfNull(mapper);

        if (plan.Kind != DataVoCompiledQueryKind.SelectSingle)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as SelectSingle.");
        }

        IReadOnlyList<T> rows = ExecuteSelectTyped(context, plan, parameters, mapper);
        return rows.Count == 0 ? default : rows[0];
    }

    private static IReadOnlyList<T> ExecuteSelectTyped<T>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        CompiledRowMapper<T> mapper)
    {
        string databaseName = ResolveCurrentDatabase(context);
        Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
        object? expected = RequiredParameter(parameterDictionary, plan.WhereParameterName!);
        string expectedKey = BuildComparisonKey(plan.WhereColumn!, expected);

        List<KeyValuePair<long, StoredRow>> matches =
            TryReadMatchingStoredRows(context, plan, databaseName, expectedKey);

        var results = new T[matches.Count];
        for (int i = 0; i < matches.Count; i++)
        {
            results[i] = mapper(new CompiledRowReader(matches[i].Value.AsView()));
        }

        return results;
    }
```

- [ ] **Step 6: Run to verify they pass**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release --filter "FullyQualifiedName~CompiledAccessPathTests" --logger "console;verbosity=detailed" 2>&1 | grep -E 'per-row|reclaimed|Passed!|Failed!|\[FAIL\]'`
Expected: PASS (all `CompiledAccessPathTests` green), and the allocation lines print, e.g. `typed per-row : ~40 B/row`, `dict per-row : ~1800 B/row`, `reclaimed : >1000 B/row`.

- [ ] **Step 7: Commit**

```bash
git add DataVo.Core/CompiledQueries/CompiledRowReader.cs DataVo.Core/CompiledQueries/CompiledRowMapper.cs DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs DataVo.Tests/E2E/CompiledAccessPathTests.cs
git commit -m "feat(query): zero-boxing CompiledRowReader + SelectManyTyped/SelectSingleTyped

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 3: Generator emits the typed projector

**Files:**
- Modify: `DataVo.Generators/DataVoQueryGenerator.cs`
- Test: `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`

**Interfaces:**
- Consumes: `DataVoCompiledQuery.SelectManyTyped<T>`/`SelectSingleTyped<T>`, `CompiledRowReader`, `CompiledRowMapper<T>` (Task 2 — referenced as emitted text).
- Produces: generated source that, for a clean ctor-name match with supported param types, contains a `__DataVoMap_<Method>` static method built from `reader.GetX("Col")` calls, a cached `CompiledRowMapper<T>` field, and a `SelectManyTyped`/`SelectSingleTyped` invocation; otherwise the existing dict mapper.

- [ ] **Step 1: Write the failing tests**

Add to `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs` (inside the class):

```csharp
    [Fact]
    public void Generator_CleanCtorMatch_EmitsTypedSelectMany()
    {
        string source = """
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record PlayerProjection(int Id, string Name, int Level);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Name = @name")]
                public static partial IReadOnlyList<PlayerProjection> ByName(DataVoContext db, string name);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("global::DataVo.Core.CompiledQueries.CompiledRowReader", generated);
        Assert.Contains("reader.GetInt32(\"Id\")", generated);
        Assert.Contains("reader.GetString(\"Name\")!", generated);
        Assert.Contains("reader.GetInt32(\"Level\")", generated);
        Assert.Contains("global::DataVo.Core.CompiledQueries.CompiledRowMapper<global::PlayerProjection>", generated);
        Assert.Contains("DataVoCompiledQuery.SelectManyTyped<global::PlayerProjection>", generated);
    }

    [Fact]
    public void Generator_CleanCtorMatch_EmitsTypedSelectSingle()
    {
        string source = """
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record PlayerProjection(int Id, string Name, int Level);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
                public static partial PlayerProjection? Get(DataVoContext db, int id);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("DataVoCompiledQuery.SelectSingleTyped<global::PlayerProjection>", generated);
        Assert.Contains("global::DataVo.Core.CompiledQueries.CompiledRowReader", generated);
    }

    [Fact]
    public void Generator_NonNameMatchedCtor_FallsBackToDictMapper()
    {
        // Ctor params (a, b, c) do not match projected columns (Id, Name, Level) by name → not a clean match.
        string source = """
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record Misnamed(int a, string b, int c);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Name = @name")]
                public static partial IReadOnlyList<Misnamed> ByName(DataVoContext db, string name);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.DoesNotContain("CompiledRowReader", generated);
        Assert.DoesNotContain("SelectManyTyped", generated);
        Assert.Contains("DataVoCompiledQuery.SelectMany<global::Misnamed>", generated);
    }

    [Fact]
    public void Generator_UnsupportedCtorParamType_FallsBackToDictMapper()
    {
        // Guid is not a supported cell type → fall back to the dict mapper for the whole query.
        string source = """
            using System;
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record WithGuid(int Id, Guid Token);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Token FROM Sessions WHERE Id = @id")]
                public static partial IReadOnlyList<WithGuid> Get(DataVoContext db, int id);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.DoesNotContain("CompiledRowReader", generated);
        Assert.Contains("DataVoCompiledQuery.SelectMany<global::WithGuid>", generated);
    }

    [Fact]
    public void Generator_LongAndDecimalParams_EmitTypedGetters()
    {
        // Int64/Decimal have no storable column type, but the generator must still map them (emission only).
        string source = """
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record Money(long Id, decimal Amount);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Amount FROM Ledger WHERE Id = @id")]
                public static partial IReadOnlyList<Money> Get(DataVoContext db, long id);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("reader.GetInt64(\"Id\")", generated);
        Assert.Contains("reader.GetDecimal(\"Amount\")", generated);
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj -c Debug --filter "FullyQualifiedName~DataVoQueryGeneratorTests" 2>&1 | grep -E 'Passed:|Failed:|\[FAIL\]'`
Expected: the five new typed tests FAIL (still emitting the dict mapper); pre-existing tests pass.

- [ ] **Step 3: Add the typed-emission helpers to the generator**

In `DataVo.Generators/DataVoQueryGenerator.cs`, add these methods to the `DataVoQueryGenerator` class (e.g., after `MapperArguments`):

```csharp
    // Builds the per-column typed getter calls when the projection is a clean ctor-name match with supported
    // param types; returns null to signal "fall back to the dictionary mapper".
    private static string[]? TryBuildTypedGetters(ITypeSymbol rowType, string[] columns)
    {
        if (rowType is not INamedTypeSymbol named)
        {
            return null;
        }

        IMethodSymbol? constructor = named.InstanceConstructors
            .Where(static ctor => !ctor.IsImplicitlyDeclared)
            .OrderByDescending(static ctor => ctor.Parameters.Length)
            .FirstOrDefault();

        if (constructor is null ||
            constructor.Parameters.Length != columns.Length ||
            !constructor.Parameters.All(parameter => columns.Any(column => string.Equals(column, parameter.Name, StringComparison.OrdinalIgnoreCase))))
        {
            return null;
        }

        var getters = new string[constructor.Parameters.Length];
        for (int i = 0; i < constructor.Parameters.Length; i++)
        {
            IParameterSymbol parameter = constructor.Parameters[i];
            string column = columns.First(candidate => string.Equals(candidate, parameter.Name, StringComparison.OrdinalIgnoreCase));
            string? getter = TypedGetter(parameter.Type, column);
            if (getter is null)
            {
                return null;
            }

            getters[i] = getter;
        }

        return getters;
    }

    private static string? TypedGetter(ITypeSymbol type, string column)
    {
        if (type is INamedTypeSymbol nullable &&
            nullable.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T &&
            nullable.TypeArguments.Length == 1)
        {
            string? inner = ValueGetterName(nullable.TypeArguments[0]);
            return inner is null ? null : $"reader.{inner}OrNull(\"{column}\")";
        }

        string? valueGetter = ValueGetterName(type);
        if (valueGetter is not null)
        {
            return $"reader.{valueGetter}(\"{column}\")";
        }

        if (type.SpecialType == SpecialType.System_String)
        {
            return type.NullableAnnotation == NullableAnnotation.Annotated
                ? $"reader.GetString(\"{column}\")"
                : $"reader.GetString(\"{column}\")!";
        }

        if (type is IArrayTypeSymbol array && array.ElementType.SpecialType == SpecialType.System_Single)
        {
            return $"reader.GetVector(\"{column}\")";
        }

        return null;
    }

    private static string? ValueGetterName(ITypeSymbol type) => type.SpecialType switch
    {
        SpecialType.System_Int32 => "GetInt32",
        SpecialType.System_Int64 => "GetInt64",
        SpecialType.System_Double => "GetDouble",
        SpecialType.System_Decimal => "GetDecimal",
        SpecialType.System_Boolean => "GetBoolean",
        _ => type.ToDisplayString() == "System.DateOnly" ? "GetDate" : null,
    };

    private static string GenerateTypedInvocation(
        IMethodSymbol method,
        GeneratedQueryModel model,
        string planName,
        GeneratedExecutionShape shape,
        string rowTypeName,
        string mapperFieldName)
    {
        string dbParameter = method.Parameters[0].Name;
        string parameters = string.Join(
            ", ",
            GetSqlParameters(model).Select(name => $"new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter(\"{name}\", {FindMethodParameterName(method, name)})"));
        string typedMethod = shape == GeneratedExecutionShape.SelectMany ? "SelectManyTyped" : "SelectSingleTyped";

        return $"global::DataVo.Core.CompiledQueries.DataVoCompiledQuery.{typedMethod}<{rowTypeName}>({dbParameter}, {planName}, new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter[] {{ {parameters} }}, {mapperFieldName})";
    }
```

- [ ] **Step 4: Wire typed emission into `GenerateMethod`**

In `DataVo.Generators/DataVoQueryGenerator.cs`, replace the `GenerateMethod` method body with this version (it emits the typed members + typed invocation on a clean match, else the existing dict invocation):

```csharp
    private static string GenerateMethod(IMethodSymbol method, GeneratedQueryModel model, CompileTimeCatalog catalog)
    {
        string namespaceDeclaration = method.ContainingNamespace.IsGlobalNamespace
            ? string.Empty
            : $"namespace {method.ContainingNamespace.ToDisplayString()};";
        string containingType = GetContainingTypeDeclaration(method.ContainingType);
        string returnType = method.ReturnType.ToDisplayString(FullyQualifiedNullableFormat);
        string parameterList = string.Join(
            ", ",
            method.Parameters.Select(parameter => $"{parameter.Type.ToDisplayString(FullyQualifiedNullableFormat)} {parameter.Name}"));
        string planName = $"__DataVoPlan_{method.Name}";

        GeneratedExecutionShape shape = ResolveExecutionShape(method, model);
        ITypeSymbol? rowType = shape is GeneratedExecutionShape.SelectSingle or GeneratedExecutionShape.SelectMany
            ? GetSelectRowType(method)
            : null;
        string[]? typedGetters = rowType is null ? null : TryBuildTypedGetters(rowType, model.ProjectedColumns);

        var builder = new StringBuilder();
        builder.AppendLine("// <auto-generated />");
        builder.AppendLine("#nullable enable");
        if (namespaceDeclaration.Length > 0)
        {
            builder.AppendLine(namespaceDeclaration);
        }

        builder.AppendLine(containingType);
        builder.AppendLine("{");
        builder.AppendLine($"    private static readonly global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan {planName} = {GeneratePlan(method, model, catalog)};");

        string invocation;
        if (typedGetters is not null && rowType is not null)
        {
            string rowTypeName = rowType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            string mapName = $"__DataVoMap_{method.Name}";
            string mapperFieldName = $"__DataVoMapper_{method.Name}";
            builder.AppendLine($"    private static {rowTypeName} {mapName}(global::DataVo.Core.CompiledQueries.CompiledRowReader reader) => new {rowTypeName}({string.Join(", ", typedGetters)});");
            builder.AppendLine($"    private static readonly global::DataVo.Core.CompiledQueries.CompiledRowMapper<{rowTypeName}> {mapperFieldName} = {mapName};");
            invocation = GenerateTypedInvocation(method, model, planName, shape, rowTypeName, mapperFieldName);
        }
        else
        {
            invocation = GenerateInvocation(method, model, planName);
        }

        builder.AppendLine($"    public static partial {returnType} {method.Name}({parameterList})");
        builder.AppendLine("    {");
        builder.AppendLine($"        return {invocation};");
        builder.AppendLine("    }");
        builder.AppendLine("}");
        return builder.ToString();
    }
```

- [ ] **Step 5: Run the generator tests to verify they pass**

Run: `dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj -c Debug --filter "FullyQualifiedName~DataVoQueryGeneratorTests" 2>&1 | grep -E 'Passed!|Failed!|\[FAIL\]|Passed:'`
Expected: PASS (all generator tests, including the five new typed ones and the pre-existing dict/access-path ones).

- [ ] **Step 6: Run the source-generated E2E suite (the generator now emits typed code into DataVo.Tests)**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj -c Debug --filter "FullyQualifiedName~SourceGeneratedCompiledQueryTests"`
Expected: PASS — `GeneratedGameQueries.GetPlayer`/`GetPlayersByName` now emit the typed path (clean ctor match to `GeneratedPlayer`), compile against the Task-2 Core types, and return identical results.

- [ ] **Step 7: Commit**

```bash
git add DataVo.Generators/DataVoQueryGenerator.cs DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs
git commit -m "feat(generators): emit zero-boxing typed projector for clean-ctor-match selects

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 4: Source-generated E2E across more cell types + NULL

**Files:**
- Modify: `DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests.cs`

**Interfaces:**
- Consumes: the generator's typed emission (Task 3); storable column types `INT/FLOAT/BIT/DATE/VARCHAR`.

- [ ] **Step 1: Write the failing test (new generated typed query over multiple types + a NULL)**

In `DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests.cs`, add a projection + query to `GeneratedGameQueries` and a test. Add to the `GeneratedGameQueries` class:

```csharp
    [DataVoQuery("SELECT Id, Score, Active, Day, Note FROM Mixed WHERE Id = @id")]
    public static partial MixedRow? GetMixed(DataVoContext db, int id);
```

Add this record near `GeneratedPlayer` (top of the file, namespace scope):

```csharp
public sealed record MixedRow(int Id, double Score, bool Active, DateOnly Day, string? Note);
```

Add this test method to `SourceGeneratedCompiledQueryTests`:

```csharp
    [Fact]
    public void GeneratedTypedSelect_ReadsAllStorableTypesIncludingNull()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Mixed (Id INT PRIMARY KEY, Score FLOAT, Active BIT, Day DATE, Note VARCHAR(50))");
        context.BulkInsert(
            "Mixed",
            [
                new Dictionary<string, object?>
                {
                    ["Id"] = 7,
                    ["Score"] = 4.25,
                    ["Active"] = true,
                    ["Day"] = new DateOnly(2026, 6, 24),
                    ["Note"] = null
                }
            ]);

        MixedRow? row = GeneratedGameQueries.GetMixed(context, 7);

        Assert.Equal(new MixedRow(7, 4.25, true, new DateOnly(2026, 6, 24), null), row);
    }
```

- [ ] **Step 2: Run to verify it passes (the generated typed code compiles and executes)**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj -c Debug --filter "FullyQualifiedName~SourceGeneratedCompiledQueryTests.GeneratedTypedSelect_ReadsAllStorableTypesIncludingNull"`
Expected: PASS. (Proves the generator emitted `reader.GetInt32/GetDouble/GetBoolean/GetDate/GetString` for a real record, that the emitted typed code compiles end-to-end, and that NULL→`string?` works.)

> If `BIT`/`DATE` BulkInsert raises a storage type error, the engine doesn't accept that column type via the dictionary path; in that case drop the offending column from `Mixed`/`MixedRow` and note it — do not weaken the typed reader. (`INT`/`FLOAT`/`VARCHAR` are known-good from existing tests.)

- [ ] **Step 3: Commit**

```bash
git add DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests.cs
git commit -m "test(query): source-generated typed select across storable cell types + NULL

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 5: Full-suite verification

**Files:** none (verification + final commit only).

- [ ] **Step 1: Build the solution (PoC code warning-clean)**

Run: `dotnet build DataVo.sln -c Release 2>&1 | grep -E 'warning|error|Build succeeded' | grep -ivE 'xUnit2017'`
Expected: `Build succeeded.` with no warnings attributable to the new code. (`xUnit2017` in `TableValidationMetadataCacheTests` is a pre-existing, unrelated analyzer rule — ignore it.)

- [ ] **Step 2: Run the full test suite**

Run: `dotnet test DataVo.sln -c Release --nologo 2>&1 | grep -E 'Passed!|Failed!|Passed:|Failed:'`
Expected: all projects green (DataVo.Tests and DataVo.Generators.Tests; counts grown by the new tests). Zero failures.

- [ ] **Step 3: AOT smoke (the runtime change is in the AOT graph)**

Run: `dotnet publish DataVo.AotSmoke/DataVo.AotSmoke.csproj -c Release -r osx-arm64 --nologo 2>&1 | grep -iE 'IL[0-9]{4}|warning|error|Generating native' | head`
Expected: `Generating native code` with **no IL trim/AOT warnings**. Then run the binary:
Run: `./DataVo.AotSmoke/bin/Release/net10.0/DataVo.AotSmoke`
Expected: `ALL SMOKE CHECKS PASSED` (exit 0). (The reader/delegate/typed `CellValue` access is reflection-free; the smoke proves the AOT graph is clean.)

- [ ] **Step 4: Clean status**

Run: `git status -s`
Expected: only the pre-existing untracked `.DS_Store`/`test.md` — no uncommitted tracked changes.

Done. Do **not** merge to `master` — the user merges manually.

## Self-Review

**1. Spec coverage:**
- `CompiledRowReader` (public ref struct, internal ctor, zero-boxing getters) → Task 2 Step 4. ✓
- `CompiledRowMapper<T>` delegate → Task 2 Step 3. ✓
- `SelectManyTyped`/`SelectSingleTyped`/`ExecuteSelectTyped` → Task 2 Step 5. ✓
- `TryReadMatchingStoredRows` finder refactor (dict path preserved) → Task 1. ✓
- Generator typed emission + clean-ctor-match + type→getter + dict fallback → Task 3. ✓
- Fail-fast / no runtime fallback → Task 2 Step 1 (`SelectManyTyped_TypeMismatch_ThrowsFailFast`). ✓
- Generation-time fallback (non-match, unsupported type) → Task 3 (`Generator_NonNameMatchedCtor…`, `Generator_UnsupportedCtorParamType…`). ✓
- All supported types → getter (§6) → Task 2 reader + Task 3 `ValueGetterName`/`TypedGetter`; long/decimal emission-tested (`Generator_LongAndDecimalParams…`); int/double/bool/date/string + NULL E2E (Task 4). ✓
- Zero-allocation proof → Task 2 Step 1 (`SelectManyTyped_AllocatesFarLessPerRowThanDictPath`). ✓
- AOT-clean / 0 warnings / full suite → Task 5. ✓

**2. Placeholder scan:** No TBD/TODO/"handle edge cases" — every code step has complete code. The Task-4 note is a contingency with an explicit action, not a placeholder.

**3. Type consistency:** `CompiledRowReader`, `CompiledRowMapper<T>`, `SelectManyTyped`/`SelectSingleTyped`, `ExecuteSelectTyped`, `TryReadMatchingStoredRows`, `ReadStoredRowsViaIndex`, `TryBuildTypedGetters`, `TypedGetter`, `ValueGetterName`, `GenerateTypedInvocation` are referenced identically across tasks. Getter names (`GetInt32`/`GetInt64`/`GetDouble`/`GetDecimal`/`GetBoolean`/`GetDate`/`GetVector`/`GetString` + `…OrNull`) match between the reader (Task 2), the generator mapping (Task 3), and the assertions. The emitted tokens (`reader.GetInt32("Id")`, `reader.GetString("Name")!`, `SelectManyTyped<global::PlayerProjection>`, `CompiledRowMapper<global::PlayerProjection>`) match Task 3's generator output exactly.
