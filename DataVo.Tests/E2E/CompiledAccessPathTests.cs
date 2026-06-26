using System.Reflection;
using DataVo.Core;
using DataVo.Core.BTree.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Indexing;
using DataVo.Core.StorageEngine.Config;
using Xunit.Abstractions;

namespace DataVo.Tests.E2E;

public class CompiledAccessPathTests
{
    private readonly ITestOutputHelper _output;

    public CompiledAccessPathTests(ITestOutputHelper output) => _output = output;

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

    [Fact]
    public void SelectSingle_DefaultAccessPath_IsRuntimeResolve()
    {
        var plan = DataVoCompiledQueryPlan.SelectSingle("Players", ["Id", "Name"], "Name", "name");

        Assert.Equal(CompiledAccessPath.RuntimeResolve, plan.AccessPath);
        Assert.Null(plan.ResolvedIndexName);
    }

    [Fact]
    public void SelectSingle_TaggedSingleColumnIndex_CarriesAccessPathAndIndexName()
    {
        var plan = DataVoCompiledQueryPlan.SelectSingle(
            "Players", ["Id", "Name"], "Name", "name",
            accessPath: CompiledAccessPath.SingleColumnIndex,
            resolvedIndexName: "ix_players_name");

        Assert.Equal(CompiledAccessPath.SingleColumnIndex, plan.AccessPath);
        Assert.Equal("ix_players_name", plan.ResolvedIndexName);
    }

    [Fact]
    public void SelectSingle_SingleColumnIndexWithoutIndexName_Throws()
    {
        Assert.Throws<ArgumentException>(() => DataVoCompiledQueryPlan.SelectSingle(
            "Players", ["Id", "Name"], "Name", "name",
            accessPath: CompiledAccessPath.SingleColumnIndex,
            resolvedIndexName: null));
    }

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
    public void TaggedSingleColumnIndex_RoutesThroughTheNamedIndex_NotRuntimeResolution()
    {
        // Distinguishing test: inject a throwing index under a GHOST name that is NOT in the catalog and create
        // no real index on Name. Runtime resolution (GetOrLoadScalarIndex via GetTableIndexes) can never find
        // the ghost, so without the compile-time tag the query just scans and returns rows. Only the tagged
        // branch, which consults plan.ResolvedIndexName directly via the cache, reaches the ghost — and its
        // InvalidOperationException propagates (the branch catches only IndexException). A thrown "boom" therefore
        // proves the tag was honored, not that runtime resolution coincidentally routed to the same index.
        using var context = CreateContext();
        SeedPlayers(context);
        InjectThrowingIndex(context, "Players", "ix_ghost");

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => QueryByName(
            context,
            DataVoCompiledQueryPlan.SelectMany(
                "Players", ["Id", "Name", "Level"], "Name", "name",
                accessPath: CompiledAccessPath.SingleColumnIndex,
                resolvedIndexName: "ix_ghost"),
            "Ada"));

        Assert.Equal("boom", ex.Message);
    }

    [Fact]
    public void UntaggedRuntimeResolve_DoesNotReachGhostIndex()
    {
        // The negative half of the routing proof: the SAME ghost setup with a RuntimeResolve plan never touches
        // the ghost index (runtime resolution only sees catalog indexes), so it scans and returns rows.
        using var context = CreateContext();
        SeedPlayers(context);
        InjectThrowingIndex(context, "Players", "ix_ghost");

        IReadOnlyList<PlayerProjection> players = QueryByName(
            context,
            DataVoCompiledQueryPlan.SelectMany("Players", ["Id", "Name", "Level"], "Name", "name"),
            "Ada");

        Assert.Equal(
            new[] { new PlayerProjection(1, "Ada", 5), new PlayerProjection(3, "Ada", 9) },
            players.OrderBy(p => p.Id));
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

        _output.WriteLine($"RuntimeResolve: {runtimeBytes} B over {iterations} calls ({(double)runtimeBytes / iterations:F1} B/call)");
        _output.WriteLine($"Tagged:         {taggedBytes} B over {iterations} calls ({(double)taggedBytes / iterations:F1} B/call)");
        _output.WriteLine($"Reduction:      {runtimeBytes - taggedBytes} B total ({(double)(runtimeBytes - taggedBytes) / iterations:F1} B/call, {100.0 * (runtimeBytes - taggedBytes) / runtimeBytes:F1}%)");

        Assert.True(
            taggedBytes < runtimeBytes,
            $"Expected tagged path to allocate less than RuntimeResolve over {iterations} calls; " +
            $"tagged={taggedBytes} B, runtime={runtimeBytes} B.");
    }

    [Fact]
    public void SelectSingle_TaggedSingleColumnIndex_RoutesThroughTheNamedIndex()
    {
        // SelectSingle shares ExecuteSelect -> TryReadMatchingRowEntries, so it honors the tag identically to
        // SelectMany. Same ghost-index proof: only the tag can reach an index the runtime catalog cannot see.
        using var context = CreateContext();
        SeedPlayers(context);
        InjectThrowingIndex(context, "Players", "ix_ghost");

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => QuerySingleByName(
            context,
            DataVoCompiledQueryPlan.SelectSingle(
                "Players", ["Id", "Name", "Level"], "Name", "name",
                accessPath: CompiledAccessPath.SingleColumnIndex,
                resolvedIndexName: "ix_ghost"),
            "Ada"));

        Assert.Equal("boom", ex.Message);
    }

    private static PlayerProjection? QuerySingleByName(DataVoContext context, DataVoCompiledQueryPlan plan, string name)
        => DataVoCompiledQuery.SelectSingle(
            context, plan, [new DataVoCompiledQueryParameter("name", name)],
            static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!));

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
    public void SelectManyTyped_ReclaimsMaterializationLayer_ScalingWithColumnCount()
    {
        const int iterations = 2_000;
        using var context = CreateContext();

        // Narrow (3 cols) and wide (8 cols) tables, both with the SAME narrow 3-column projection. The dict
        // path materializes the full row and boxes every cell, so its reclaimable cost grows with total
        // columns; the typed path reads only the 3 projected columns. Reclaim therefore scales with column
        // count — proving the win is exactly the materialization + boxing layer. The shared row-fetch plumbing
        // remains in BOTH paths (the next track), so this test deliberately does NOT assert near-zero.
        context.Execute("CREATE TABLE Narrow (Id INT PRIMARY KEY, Tag VARCHAR(20), Score FLOAT)");
        context.Execute("CREATE TABLE Wide (Id INT PRIMARY KEY, Tag VARCHAR(20), Score FLOAT, C1 VARCHAR(20), C2 VARCHAR(20), C3 VARCHAR(20), C4 VARCHAR(20), C5 VARCHAR(20))");

        var narrowSeed = new List<Dictionary<string, object?>>();
        var wideSeed = new List<Dictionary<string, object?>>();
        for (int i = 0; i < 9; i++)
        {
            string tag = i == 0 ? "m1" : "m8";
            int id = i + 1;
            narrowSeed.Add(new Dictionary<string, object?> { ["Id"] = id, ["Tag"] = tag, ["Score"] = id + 0.5 });
            wideSeed.Add(new Dictionary<string, object?>
            {
                ["Id"] = id, ["Tag"] = tag, ["Score"] = id + 0.5,
                ["C1"] = "c1", ["C2"] = "c2", ["C3"] = "c3", ["C4"] = "c4", ["C5"] = "c5"
            });
        }
        context.BulkInsert("Narrow", narrowSeed);
        context.BulkInsert("Wide", wideSeed);
        context.Execute("CREATE INDEX ix_narrow_tag ON Narrow (Tag)");
        context.Execute("CREATE INDEX ix_wide_tag ON Wide (Tag)");

        double narrowReclaim = MaterializationReclaimPerRow(context, "Narrow", "ix_narrow_tag", iterations);
        double wideReclaim = MaterializationReclaimPerRow(context, "Wide", "ix_wide_tag", iterations);

        _output.WriteLine($"narrow (3 col) reclaim : {narrowReclaim:F0} B/row");
        _output.WriteLine($"wide   (8 col) reclaim : {wideReclaim:F0} B/row");

        Assert.True(narrowReclaim > 0, $"Typed must allocate strictly less than dict (narrow); reclaim={narrowReclaim:F0}.");
        Assert.True(
            wideReclaim > narrowReclaim,
            $"Reclaim must scale with column count (it is the materialization+boxing layer); narrow={narrowReclaim:F0}, wide={wideReclaim:F0}.");
    }

    // Per-row bytes the dict path allocates that the typed path does not — i.e. the materialization + boxing
    // layer reclaimed by typed projection. Both paths share the row-fetch plumbing, which cancels in the diff.
    private double MaterializationReclaimPerRow(DataVoContext context, string table, string indexName, int iterations)
    {
        var typedPlan = DataVoCompiledQueryPlan.SelectMany(
            table, ["Id", "Tag", "Score"], "Tag", "tag",
            accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: indexName);
        var dictPlan = DataVoCompiledQueryPlan.SelectMany(
            table, ["Id", "Tag", "Score"], "Tag", "tag",
            accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: indexName);

        IReadOnlyList<Hit> Typed(string tag)
            => DataVoCompiledQuery.SelectManyTyped<Hit>(context, typedPlan, [new DataVoCompiledQueryParameter("tag", tag)],
                static r => new Hit(r.GetInt32("Id"), r.GetString("Tag")!, r.GetDouble("Score")));
        IReadOnlyList<Hit> Dict(string tag)
            => DataVoCompiledQuery.SelectMany<Hit>(context, dictPlan, [new DataVoCompiledQueryParameter("tag", tag)],
                static row => new Hit((int)row["Id"]!, (string)row["Tag"]!, (double)row["Score"]!));

        double typedPerRow = PerRow(() => Typed("m1"), () => Typed("m8"), iterations);
        double dictPerRow = PerRow(() => Dict("m1"), () => Dict("m8"), iterations);
        _output.WriteLine($"{table}: typed={typedPerRow:F0} B/row, dict={dictPerRow:F0} B/row");
        return dictPerRow - typedPerRow;
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

    // Injects a search-throwing index directly into the IndexManager cache under the (db/table_index) key, with
    // no catalog registration. Mirrors the cache-key format the engine builds in GetCacheKey.
    private static void InjectThrowingIndex(DataVoContext context, string tableName, string indexName)
    {
        FieldInfo cacheField = typeof(IndexManager).GetField("_cache", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var cache = (Dictionary<string, IIndexBase>)cacheField.GetValue(context.Engine.IndexManager)!;
        string databaseName = CurrentDatabase(context);
        string cacheKey = $"{databaseName}/{tableName}_{indexName}".ToLowerInvariant();
        cache[cacheKey] = new ThrowingIndex();
    }
}
