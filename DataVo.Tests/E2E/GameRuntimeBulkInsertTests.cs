using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class GameRuntimeBulkInsertTests
{
    [Fact]
    public void BulkInsert_InsertsRowsAndReturnsRowIdsInOrder()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Telemetry (Id INT PRIMARY KEY, EventName VARCHAR(50), Frame INT)");

        IReadOnlyList<long> rowIds = context.BulkInsert("Telemetry",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["EventName"] = "level_start", ["Frame"] = 10 },
            new Dictionary<string, object?> { ["Id"] = 2, ["EventName"] = "death", ["Frame"] = 42 }
        ]);

        Assert.Equal([1L, 2L], rowIds);

        List<Dictionary<string, object?>> rows = Select(context, "SELECT Id, EventName, Frame FROM Telemetry ORDER BY Id ASC");
        Assert.Equal(2, rows.Count);
        Assert.Equal("level_start", rows[0]["EventName"]);
        Assert.Equal("death", rows[1]["EventName"]);
    }

    [Fact]
    public void BulkInsert_AppliesDefaultsAndPrimaryKeyConstraints()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT DEFAULT 1)");

        IReadOnlyList<long> rowIds = context.BulkInsert("Players",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada" },
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Duplicate" }
        ]);

        Assert.Single(rowIds);

        List<Dictionary<string, object?>> rows = Select(context, "SELECT Id, Name, Level FROM Players");
        Assert.Single(rows);
        Assert.Equal("Ada", rows[0]["Name"]);
        Assert.Equal(1, (int)rows[0]["Level"]);
    }

    [Fact]
    public void BulkInsert_UpdatesScalarIndexes()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Events (Id INT PRIMARY KEY, Kind VARCHAR(50))");
        context.Execute("CREATE INDEX idx_kind ON Events (Kind)");

        context.BulkInsert("Events",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Kind"] = "spawn" },
            new Dictionary<string, object?> { ["Id"] = 2, ["Kind"] = "despawn" }
        ]);

        Assert.True(context.Engine.IndexManager.IndexContainsKey("despawn", "idx_kind", "Events", CurrentDatabase(context)));

        List<Dictionary<string, object?>> rows = Select(context, "SELECT Id FROM Events WHERE Kind = 'despawn'");

        Assert.Single(rows);
        Assert.Equal(2, (int)rows[0]["Id"]);
    }

    [Fact]
    public void BulkInsert_UpdatesVectorIndexes()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE UserEmbeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(50))");
        context.Execute("CREATE INDEX idx_emb ON UserEmbeddings (Emb) USING HNSW");

        context.BulkInsert("UserEmbeddings",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Emb"] = new float[] { 1f, 0f, 0f }, ["Label"] = "combat" },
            new Dictionary<string, object?> { ["Id"] = 2, ["Emb"] = new float[] { 0f, 1f, 0f }, ["Label"] = "builder" }
        ]);

        List<Dictionary<string, object?>> nearest = context.SearchNearest("UserEmbeddings", "idx_emb", "[0.9,0.1,0]", topK: 1);

        Assert.Single(nearest);
        Assert.Equal("combat", nearest[0]["Label"]);
    }

    [Fact]
    public void BulkInsert_WithoutSelectedDatabaseThrows()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

        Assert.Throws<InvalidOperationException>(() => context.BulkInsert("Events",
        [
            new Dictionary<string, object?> { ["Id"] = 1 }
        ]));
    }

    [Fact]
    public void BulkInsert_WithEmptyRowsAndNoSelectedDatabase_ReturnsEmptySet()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

        IReadOnlyList<long> rowIds = context.BulkInsert("Events", []);

        Assert.Empty(rowIds);
    }

    [Fact]
    public void BulkInsert_WithMissingPrimaryKeyIndex_FallsBackToRowScan()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50))");
        context.Execute("INSERT INTO Players (Id, Name) VALUES (1, 'Ada')");

        context.Engine.Catalog.DropIndex("_PK_Players", "Players", CurrentDatabase(context));
        context.Engine.IndexManager.DropIndex("_PK_Players", "Players", CurrentDatabase(context));

        IReadOnlyList<long> rowIds = context.BulkInsert("Players",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Duplicate" }
        ]);

        Assert.Empty(rowIds);

        List<Dictionary<string, object?>> rows = Select(context, "SELECT Id, Name FROM Players");
        Assert.Single(rows);
        Assert.Equal("Ada", rows[0]["Name"]);
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string dbName = $"GameBulk_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {dbName}");
        context.Execute($"USE {dbName}");
        return context;
    }

    private static List<Dictionary<string, object?>> Select(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Single();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return result.Data;
    }

    private static string CurrentDatabase(DataVoContext context)
    {
        return context.Engine.Sessions.Get(context.SessionId)
            ?? throw new InvalidOperationException("Expected a selected database for the test context.");
    }
}
