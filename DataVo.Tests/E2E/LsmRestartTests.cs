using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;
using DataVo.Tests.BrowserParity;

namespace DataVo.Tests.E2E;

/// <summary>
/// End-to-end restart coverage for Lsm through the public SQL API: the schema, rows, and every index
/// kind must come back when a new engine opens the same directory.
/// </summary>
[BrowserTranslateIgnore("Lsm persistence across engine restart is outside browser SQL parity")]
public sealed class LsmRestartTests
{
    private readonly string _path = TestStorageDirectories.Create("lsm_restart");

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void SchemaAndRows_SurviveRestart(bool strictFsync)
    {
        using (var first = Open(strictFsync))
        {
            Seed(first);
            AssertOk(first.Execute("INSERT INTO Players (Id, Name, Code) VALUES (1, 'Ada', 100)"));
            AssertOk(first.Execute("INSERT INTO Players (Id, Name, Code) VALUES (2, 'Grace', 200)"));
            AssertOk(first.Execute("UPDATE Players SET Name = 'Ada L.' WHERE Id = 1"));
            AssertOk(first.Execute("INSERT INTO Players (Id, Name, Code) VALUES (3, 'Temp', 300)"));
            AssertOk(first.Execute("DELETE FROM Players WHERE Id = 3"));
        }

        using var second = Reopen(strictFsync);
        List<Dictionary<string, object?>> rows = second.Execute("SELECT * FROM Players ORDER BY Id")[0].Data;
        Assert.Equal(2, rows.Count);
        Assert.Equal("Ada L.", rows[0]["Name"]?.ToString());
        Assert.Equal("Grace", rows[1]["Name"]?.ToString());
    }

    [Fact]
    public void ScalarIndexes_AreRebuilt_AfterRestart()
    {
        using (var first = Open())
        {
            Seed(first);
            AssertOk(first.Execute("CREATE INDEX idx_players_name ON Players (Name)"));
            AssertOk(first.Execute("INSERT INTO Players (Id, Name, Code) VALUES (1, 'Ada', 100)"));
            AssertOk(first.Execute("INSERT INTO Players (Id, Name, Code) VALUES (2, 'Ada', 200)"));
        }

        using var second = Reopen();
        Assert.Equal(2, second.Execute("SELECT * FROM Players WHERE Name = 'Ada'")[0].Data.Count);
        Assert.Single(second.Execute("SELECT * FROM Players WHERE Id = 2")[0].Data);

        // Primary-key and unique constraints are enforced against pre-restart rows.
        AssertRejected(second.Execute("INSERT INTO Players (Id, Name, Code) VALUES (1, 'Dup', 999)"));
        AssertRejected(second.Execute("INSERT INTO Players (Id, Name, Code) VALUES (9, 'Other', 100)"));
        AssertOk(second.Execute("INSERT INTO Players (Id, Name, Code) VALUES (3, 'Linus', 300)"));
    }

    [Fact]
    public void VectorIndex_IsRebuilt_AfterRestart()
    {
        using (var first = Open())
        {
            AssertOk(first.Execute("CREATE DATABASE LsmDb"));
            AssertOk(first.Execute("USE LsmDb"));
            AssertOk(first.Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3))"));
            AssertOk(first.Execute("CREATE INDEX idx_emb ON Embeddings (Emb) USING HNSW"));
            AssertOk(first.Execute("INSERT INTO Embeddings (Id, Emb) VALUES (1, '[1,0,0]')"));
            AssertOk(first.Execute("INSERT INTO Embeddings (Id, Emb) VALUES (2, '[0,1,0]')"));
        }

        using var second = Reopen();
        List<Dictionary<string, object?>> nearest = second.SearchNearest("Embeddings", "idx_emb", "[0.9,0.1,0]", topK: 1);
        Assert.Single(nearest);
        Assert.Equal(1, Convert.ToInt32(nearest[0]["Id"]));
    }

    [Fact]
    public void TransactionIds_ContinuePastRestart()
    {
        long highWaterMark;
        using (var first = Open())
        {
            Seed(first);
            AssertOk(first.Execute("BEGIN"));
            AssertOk(first.Execute("INSERT INTO Players (Id, Name, Code) VALUES (1, 'Ada', 100)"));
            AssertOk(first.Execute("COMMIT"));
            highWaterMark = first.Engine.TransactionIdAllocator.GetCurrentHighWaterMark();
        }

        using var second = Reopen();
        Assert.True(second.Engine.TransactionIdAllocator.GetCurrentHighWaterMark() >= highWaterMark);
        AssertOk(second.Execute("BEGIN"));
        AssertOk(second.Execute("INSERT INTO Players (Id, Name, Code) VALUES (2, 'Grace', 200)"));
        AssertOk(second.Execute("COMMIT"));
        Assert.Equal(2, second.Execute("SELECT * FROM Players")[0].Data.Count);
    }

    [Fact]
    public void RepeatedRestarts_AndDdl_StayConsistent()
    {
        using (var first = Open())
        {
            Seed(first);
            AssertOk(first.Execute("CREATE TABLE Scratch (Id INT PRIMARY KEY)"));
            AssertOk(first.Execute("INSERT INTO Players (Id, Name, Code) VALUES (1, 'Ada', 100)"));
        }

        using (var second = Reopen())
        {
            AssertOk(second.Execute("DROP TABLE Scratch"));
            AssertOk(second.Execute("INSERT INTO Players (Id, Name, Code) VALUES (2, 'Grace', 200)"));
        }

        using var third = Reopen();
        Assert.Equal(2, third.Execute("SELECT * FROM Players")[0].Data.Count);
        Assert.Contains(third.Execute("SELECT * FROM Scratch"), r => r.IsError);
        AssertOk(third.Execute("CREATE TABLE Scratch (Id INT PRIMARY KEY)"));
        Assert.Empty(third.Execute("SELECT * FROM Scratch")[0].Data);
    }

    private DataVoContext Open(bool strictFsync = true) =>
        new(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = _path, LsmStrictFsync = strictFsync });

    private DataVoContext Reopen(bool strictFsync = true)
    {
        DataVoContext context = Open(strictFsync);
        AssertOk(context.Execute("USE LsmDb"));
        return context;
    }

    private static void Seed(DataVoContext context)
    {
        AssertOk(context.Execute("CREATE DATABASE LsmDb"));
        AssertOk(context.Execute("USE LsmDb"));
        AssertOk(context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(40), Code INT UNIQUE)"));
    }

    private static void AssertOk(List<QueryResult> results)
    {
        QueryResult? error = results.FirstOrDefault(r => r.IsError);
        Assert.True(error is null, error is null ? null : string.Join("; ", error.Messages));
    }

    private static void AssertRejected(List<QueryResult> results) =>
        Assert.True(Assert.Single(results).IsError);
}
