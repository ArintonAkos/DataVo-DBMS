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
