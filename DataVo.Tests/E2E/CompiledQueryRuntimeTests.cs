using System.Reflection;
using DataVo.Core;
using DataVo.Core.BTree.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Exceptions;
using DataVo.Core.Indexing;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.ValueConversion;

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
    public void CompiledSelectMany_ByIndexedNonPrimaryKeyColumn_RoutesThroughSecondaryIndex()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.BulkInsert(
            "Players",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 5 },
                new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Grace", ["Level"] = 8 }
            ]);
        context.Execute("CREATE INDEX ix_players_name ON Players (Name)");

        // Force the secondary index to throw if (and only if) it is consulted. Before the planner learns
        // to use secondary indexes it scans the table instead, never touching this index, so the throwing
        // index is never reached and nothing is thrown.
        ReplaceIndexWithThrowingIndex(context, "Players", "ix_players_name");

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            DataVoCompiledQuery.SelectMany(
                context,
                DataVoCompiledQueryPlan.SelectMany(
                    tableName: "Players",
                    projectedColumns: ["Id", "Name", "Level"],
                    whereColumn: "Name",
                    parameterName: "name"),
                [new DataVoCompiledQueryParameter("name", "Ada")],
                static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!)));

        Assert.Equal("boom", ex.Message);
    }

    [Fact]
    public void CompiledSelectMany_ByIndexedNonPrimaryKeyColumn_ReturnsSameRowsAsScan()
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
        context.Execute("CREATE INDEX ix_players_name ON Players (Name)");

        IReadOnlyList<PlayerProjection> players = DataVoCompiledQuery.SelectMany(
            context,
            DataVoCompiledQueryPlan.SelectMany(
                tableName: "Players",
                projectedColumns: ["Id", "Name", "Level"],
                whereColumn: "Name",
                parameterName: "name"),
            [new DataVoCompiledQueryParameter("name", "Ada")],
            static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!));

        // Index lookup returns the same rows the equivalent unindexed scan returns
        // (CompiledSelectMany_ByNonPrimaryKeyColumn_FallsBackToScanAndReturnsTypedResults), order-independent.
        Assert.Equal(
            new[] { new PlayerProjection(1, "Ada", 5), new PlayerProjection(3, "Ada", 9) }.OrderBy(p => p.Id),
            players.OrderBy(p => p.Id));
    }

    [Fact]
    public void CompiledSelectMany_ByIndexedNonPrimaryKeyColumn_NoMatchingKey_ReturnsEmpty()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.BulkInsert(
            "Players",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 5 }
            ]);
        context.Execute("CREATE INDEX ix_players_name ON Players (Name)");

        IReadOnlyList<PlayerProjection> players = DataVoCompiledQuery.SelectMany(
            context,
            DataVoCompiledQueryPlan.SelectMany(
                tableName: "Players",
                projectedColumns: ["Id", "Name", "Level"],
                whereColumn: "Name",
                parameterName: "name"),
            [new DataVoCompiledQueryParameter("name", "Nobody")],
            static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!));

        Assert.Empty(players);
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
    public void CompiledInsert_ConvertsRegisteredVectorParameters()
    {
        using var context = CreateContext();
        context.RegisterValueConverters(CreateTestVectorConverter());
        context.Execute("CREATE TABLE Samples (Id INT PRIMARY KEY, Position VECTOR(2))");

        IReadOnlyList<long> ids = DataVoCompiledQuery.Insert(
            context,
            DataVoCompiledQueryPlan.Insert(
                tableName: "Samples",
                columns: ["Id", "Position"],
                parameterNames: ["id", "position"]),
            [
                new DataVoCompiledQueryParameter("id", 1),
                new DataVoCompiledQueryParameter("position", new TestVector(1.5f, -4f))
            ]);

        Assert.Equal([1L], ids);
        Dictionary<string, object?> row = context.Execute("SELECT Position FROM Samples").Single().Data.Single();
        Assert.Equal(new TestVector(1.5f, -4f), context.Get<TestVector>(row, "Position"));
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
    public void CompiledUpdate_ConvertsRegisteredVectorAssignments()
    {
        using var context = CreateContext();
        context.RegisterValueConverters(CreateTestVectorConverter());
        context.Execute("CREATE TABLE Samples (Id INT PRIMARY KEY, Position VECTOR(2))");
        context.Execute("CREATE INDEX idx_samples_position ON Samples (Position) USING HNSW");
        context.BulkInsert("Samples",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Position"] = new[] { 0f, 0f } }
        ]);

        var plan = DataVoCompiledQueryPlan.Update(
            tableName: "Samples",
            assignments: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Position"] = "position"
            },
            whereColumn: "Id",
            whereParameterName: "id");

        int affected = DataVoCompiledQuery.Update(
            context,
            plan,
            [
                new DataVoCompiledQueryParameter("id", 1),
                new DataVoCompiledQueryParameter("position", new TestVector(-2f, 9f))
            ]);

        Assert.Equal(1, affected);
        Dictionary<string, object?> row = context.Execute("SELECT Position FROM Samples").Single().Data.Single();
        Assert.Equal(new TestVector(-2f, 9f), context.Get<TestVector>(row, "Position"));
        Assert.Equal(1, context.SearchNearest("Samples", "idx_samples_position", [-2f, 9f], topK: 1).Single()["Id"]);
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

    [Fact]
    public void CompiledUpdate_PublishesReactiveBeforeAndAfterImages()
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

        QueryChange? observed = null;
        using IDisposable subscription = context.Subscribe(
            "SELECT Id, Level FROM Players",
            change => observed = change);

        Assert.Equal(1, DataVoCompiledQuery.Update(
            context,
            plan,
            [
                new DataVoCompiledQueryParameter("id", 1),
                new DataVoCompiledQueryParameter("level", 7)
            ]));
        context.DispatchPendingNotifications();

        Assert.NotNull(observed);
        Assert.Single(observed!.UpdatedBefore);
        Assert.Single(observed.Updated);
        Assert.Equal(5, observed.UpdatedBefore[0]["Level"]);
        Assert.Equal(7, observed.Updated[0]["Level"]);
    }

    [Fact]
    public void CompiledUpdate_RejectsActiveTransactions()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Level INT)");
        context.Execute("INSERT INTO Players VALUES (1, 5)");
        context.Execute("BEGIN TRANSACTION");

        var plan = DataVoCompiledQueryPlan.Update(
            tableName: "Players",
            assignments: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Level"] = "level"
            },
            whereColumn: "Id",
            whereParameterName: "id");

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() => DataVoCompiledQuery.Update(
            context,
            plan,
            [
                new DataVoCompiledQueryParameter("id", 1),
                new DataVoCompiledQueryParameter("level", 7)
            ]));

        Assert.Contains("active transactions", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string databaseName = $"Compiled_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }

    private static DataVoValueConverter<TestVector> CreateTestVectorConverter() => new(
        "DataVo.Tests.CompiledQueryRuntime.TestVector.v1",
        value => CellValue.FromVector([value.X, value.Y]),
        value => value is float[] vector && vector.Length == 2
            ? new TestVector(vector[0], vector[1])
            : throw new ArgumentException("Expected VECTOR(2).", nameof(value)));

    private static string CurrentDatabase(DataVoContext context)
    {
        return context.Engine.Sessions.Get(context.SessionId)
            ?? throw new InvalidOperationException("Expected current database.");
    }

    private static void ReplacePrimaryKeyIndexWithThrowingIndex(DataVoContext context, string tableName)
    {
        ReplaceIndexWithThrowingIndex(context, tableName, "_PK_" + tableName);
    }

    private static void ReplaceIndexWithThrowingIndex(DataVoContext context, string tableName, string indexName)
    {
        string databaseName = CurrentDatabase(context);
        // Keep every IndexManager cache coherent before injecting the fault. In particular, an integer
        // primary-key fast lane must not keep bypassing the deliberately replaced scalar index.
        context.Engine.IndexManager.RemoveIndex((databaseName, tableName, indexName));

        FieldInfo cacheField = typeof(IndexManager).GetField("_cache", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var cache = (Dictionary<(string, string, string), IIndexBase>)cacheField.GetValue(context.Engine.IndexManager)!;
        var cacheKey = (databaseName, tableName, indexName);
        cache[cacheKey] = new ThrowingIndex();
    }

    private readonly record struct TestVector(float X, float Y);
}
