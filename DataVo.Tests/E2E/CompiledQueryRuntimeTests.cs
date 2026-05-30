using System.Reflection;
using DataVo.Core;
using DataVo.Core.BTree.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Exceptions;
using DataVo.Core.Indexing;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public sealed record PlayerProjection(int Id, string Name, int Level);

public class CompiledQueryRuntimeTests
{
    private sealed class ThrowingIndex : IIndex
    {
        public void Insert(string key, long rowId) => throw new NotSupportedException();

        public void DeleteValues(List<long> rowIds) => throw new NotSupportedException();

        public List<long> Search(string key) => throw new InvalidOperationException("boom");

        public bool ContainsValue(long rowId) => throw new NotSupportedException();

        public void Save(string filePath) => throw new NotSupportedException();
    }

    [Fact]
    public void CompiledSelectSingle_ByPrimaryKey_ReturnsTypedResultWithoutSqlExecute()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.BulkInsert(
            "Players",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 5 }
            ]);

        var plan = DataVoCompiledQueryPlan.SelectSingle(
            tableName: "Players",
            projectedColumns: ["Id", "Name", "Level"],
            whereColumn: "Id",
            parameterName: "id");

        PlayerProjection? player = DataVoCompiledQuery.SelectSingle(
            context,
            plan,
            [new DataVoCompiledQueryParameter("id", 1)],
            static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!));

        Assert.Equal(new PlayerProjection(1, "Ada", 5), player);
    }

    [Fact]
    public void CompiledSelectMany_ByNonPrimaryKeyColumn_FallsBackToScanAndReturnsTypedResults()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.BulkInsert(
            "Players",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 5 },
                new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Grace", ["Level"] = 8 },
                new Dictionary<string, object?> { ["Id"] = 3, ["Name"] = "Ada", ["Level"] = 9 }
            ]);

        IReadOnlyList<PlayerProjection> players = DataVoCompiledQuery.SelectMany(
            context,
            DataVoCompiledQueryPlan.SelectMany(
                tableName: "Players",
                projectedColumns: ["Id", "Name", "Level"],
                whereColumn: "Name",
                parameterName: "name"),
            [new DataVoCompiledQueryParameter("name", "Ada")],
            static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!));

        Assert.Equal(
            [new PlayerProjection(1, "Ada", 5), new PlayerProjection(3, "Ada", 9)],
            players);
    }

    [Fact]
    public void CompiledSelectMany_ByNonPrimaryKeyColumn_DoesNotUsePrimaryKeyFastPathOnValueCollision()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.BulkInsert(
            "Players",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 5 },
                new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "1", ["Level"] = 8 }
            ]);

        IReadOnlyList<PlayerProjection> players = DataVoCompiledQuery.SelectMany(
            context,
            DataVoCompiledQueryPlan.SelectMany(
                tableName: "Players",
                projectedColumns: ["Id", "Name", "Level"],
                whereColumn: "Name",
                parameterName: "name"),
            [new DataVoCompiledQueryParameter("name", "1")],
            static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!));

        Assert.Equal(
            [new PlayerProjection(2, "1", 8)],
            players);
    }

    [Fact]
    public void CompiledSelectSingle_MissingPrimaryKeyIndex_FallsBackToScan()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.BulkInsert(
            "Players",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 5 }
            ]);
        context.Engine.IndexManager.DropIndex("_PK_Players", "Players", CurrentDatabase(context));

        PlayerProjection? player = DataVoCompiledQuery.SelectSingle(
            context,
            DataVoCompiledQueryPlan.SelectSingle(
                tableName: "Players",
                projectedColumns: ["Id", "Name", "Level"],
                whereColumn: "Id",
                parameterName: "id"),
            [new DataVoCompiledQueryParameter("id", 1)],
            static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!));

        Assert.Equal(new PlayerProjection(1, "Ada", 5), player);
    }

    [Fact]
    public void CompiledSelectSingle_UnexpectedIndexFailure_IsNotSwallowed()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.BulkInsert(
            "Players",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 5 }
            ]);
        ReplacePrimaryKeyIndexWithThrowingIndex(context, "Players");

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            DataVoCompiledQuery.SelectSingle(
                context,
                DataVoCompiledQueryPlan.SelectSingle(
                    tableName: "Players",
                    projectedColumns: ["Id", "Name", "Level"],
                    whereColumn: "Id",
                    parameterName: "id"),
                [new DataVoCompiledQueryParameter("id", 1)],
                static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!)));

        Assert.Equal("boom", ex.Message);
    }

    [Fact]
    public void CompiledInsert_InsertsRowAndReturnsRowId()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Telemetry (Id INT PRIMARY KEY, EventName VARCHAR(50), Frame INT)");

        IReadOnlyList<long> ids = DataVoCompiledQuery.Insert(
            context,
            DataVoCompiledQueryPlan.Insert(
                tableName: "Telemetry",
                columns: ["Id", "EventName", "Frame"],
                parameterNames: ["id", "eventName", "frame"]),
            [
                new DataVoCompiledQueryParameter("id", 1),
                new DataVoCompiledQueryParameter("eventName", "level_start"),
                new DataVoCompiledQueryParameter("frame", 10)
            ]);

        Assert.Equal([1L], ids);
    }

    [Fact]
    public void CompiledUpdate_UpdatesRowsAndReturnsAffectedCount()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Level INT)");
        context.Execute("INSERT INTO Players VALUES (1, 5)");

        var plan = DataVoCompiledQueryPlan.Update(
            tableName: "Players",
            assignments: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Level"] = "level"
            },
            whereColumn: "Id",
            whereParameterName: "id");

        int affected = DataVoCompiledQuery.Update(
            context,
            plan,
            [
                new DataVoCompiledQueryParameter("id", 1),
                new DataVoCompiledQueryParameter("level", 7)
            ]);

        Assert.Equal(1, affected);
        Assert.Equal(7, (int)context.Execute("SELECT Level FROM Players WHERE Id = 1").Single().Data.Single()["Level"]!);
    }

    [Fact]
    public void CompiledUpdate_DoesNotExecuteSqlParserPath()
    {
        using var context = CreateContext();
        context.Diagnostics.Enabled = true;
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Level INT)");
        context.Execute("INSERT INTO Players VALUES (1, 5)");

        var plan = DataVoCompiledQueryPlan.Update(
            tableName: "Players",
            assignments: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Level"] = "level"
            },
            whereColumn: "Id",
            whereParameterName: "id");

        context.Diagnostics.Clear();

        int affected = DataVoCompiledQuery.Update(
            context,
            plan,
            [
                new DataVoCompiledQueryParameter("id", 1),
                new DataVoCompiledQueryParameter("level", 7)
            ]);

        Assert.Equal(1, affected);
        Assert.Null(context.Diagnostics.LastQuery);
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string databaseName = $"Compiled_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }

    private static string CurrentDatabase(DataVoContext context)
    {
        return context.Engine.Sessions.Get(context.SessionId)
            ?? throw new InvalidOperationException("Expected current database.");
    }

    private static void ReplacePrimaryKeyIndexWithThrowingIndex(DataVoContext context, string tableName)
    {
        FieldInfo cacheField = typeof(IndexManager).GetField("_cache", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var cache = (Dictionary<string, IIndexBase>)cacheField.GetValue(context.Engine.IndexManager)!;
        string databaseName = CurrentDatabase(context);
        string cacheKey = $"{databaseName}/{tableName}_{"_PK_" + tableName}".ToLowerInvariant();
        cache[cacheKey] = new ThrowingIndex();
    }
}
