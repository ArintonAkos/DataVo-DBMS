using DataVo.Core;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

/// <summary>
/// Snapshots of the durable LSM engine (Phase 7, LE-08): a snapshot captures every table's rows; restoring it
/// rewrites the tables in one durable write, rebuilds the scalar and vector indexes, survives a reopen, and is
/// refused (with nothing changed) when the schema moved on since the snapshot.
/// </summary>
public sealed class LsmSnapshotTests : IDisposable
{
    private readonly string _root = TestStorageDirectories.Create("lsm-snapshot");

    private DataVoContext Open()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DataDirectory = _root });
        Ok(context, "CREATE DATABASE IF NOT EXISTS Snap");
        Ok(context, "USE Snap");
        return context;
    }

    private static void CreateSchema(DataVoContext context)
    {
        Ok(context, "CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Team VARCHAR(10), Emb VECTOR(3))");
        Ok(context, "CREATE INDEX idx_team ON Players (Team)");
        Ok(context, "CREATE INDEX idx_emb ON Players (Emb) USING HNSW");
        Ok(context, "CREATE TABLE Events (Seq INT, Kind VARCHAR(10))"); // no primary key: a bag of rows
    }

    private static void Seed(DataVoContext context)
    {
        context.BulkInsert("Players",
        [
            Player(1, "Ann", "red", [1f, 0f, 0f]),
            Player(2, "Bob", "blue", [0f, 1f, 0f]),
            Player(3, "Cid", "red", [0f, 0f, 1f]),
        ]);
        Ok(context, "INSERT INTO Events VALUES (1, 'spawn'), (1, 'spawn'), (2, 'hit')");
    }

    [Fact]
    public void Restore_BringsBackRowsAndIndexes()
    {
        using DataVoContext context = Open();
        CreateSchema(context);
        Seed(context);
        DataVoSnapshot snapshot = context.CreateSnapshot();
        Assert.Equal(StorageMode.Lsm, snapshot.StorageMode);

        Ok(context, "UPDATE Players SET Team = 'blue' WHERE Id = 1");
        Ok(context, "DELETE FROM Players WHERE Id = 2");
        context.BulkInsert("Players", [Player(4, "Dee", "red", [1f, 1f, 0f])]);
        Ok(context, "DELETE FROM Events WHERE Seq = 1");

        context.RestoreSnapshot(snapshot);

        Assert.Equal(["1:Ann:red", "2:Bob:blue", "3:Cid:red"], Players(context));
        Assert.Equal(["1:spawn", "1:spawn", "2:hit"], Events(context));
        Assert.Equal(["1", "3"], Ids(context, "SELECT Id FROM Players WHERE Team = 'red' ORDER BY Id"));
        Assert.Equal(2, context.SearchNearest("Players", "idx_emb", "[0,0.9,0.1]", topK: 1).Single()["Id"]);

        // Keys follow the restored rows: 2 is taken again, 4 is free again.
        Assert.Throws<ConstraintViolationException>(() => context.BulkInsert("Players", [Player(2, "Dup", "red", [1f, 0f, 0f])]));
        context.BulkInsert("Players", [Player(4, "Dee", "red", [1f, 1f, 0f])]);
        Assert.Equal(4, Players(context).Count);
    }

    [Fact]
    public void Restore_IsDurableAcrossReopen()
    {
        DataVoSnapshot snapshot;
        using (DataVoContext context = Open())
        {
            CreateSchema(context);
            Seed(context);
            snapshot = context.CreateSnapshot();
            Ok(context, "DELETE FROM Players");
            Ok(context, "DELETE FROM Events");
            context.RestoreSnapshot(snapshot);
        }

        using DataVoContext reopened = Open();
        Assert.Equal(["1:Ann:red", "2:Bob:blue", "3:Cid:red"], Players(reopened));
        Assert.Equal(["1:spawn", "1:spawn", "2:hit"], Events(reopened));
        Assert.Equal(["2"], Ids(reopened, "SELECT Id FROM Players WHERE Team = 'blue'"));
    }

    [Fact]
    public void Restore_AfterASchemaChange_IsRefusedAndChangesNothing()
    {
        using DataVoContext context = Open();
        CreateSchema(context);
        Seed(context);
        DataVoSnapshot snapshot = context.CreateSnapshot();

        Ok(context, "DELETE FROM Players WHERE Id = 3");
        Ok(context, "CREATE TABLE Extra (Id INT PRIMARY KEY)");

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => context.RestoreSnapshot(snapshot));
        Assert.Contains("schema changed", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(["1:Ann:red", "2:Bob:blue"], Players(context));
    }

    [Fact]
    public void Snapshots_AreRefusedWhileATransactionIsOpen()
    {
        using DataVoContext context = Open();
        CreateSchema(context);
        DataVoSnapshot snapshot = context.CreateSnapshot();
        Ok(context, "BEGIN TRANSACTION");

        Assert.Throws<InvalidOperationException>(() => context.CreateSnapshot());
        Assert.Throws<InvalidOperationException>(() => context.RestoreSnapshot(snapshot));
        Ok(context, "ROLLBACK");
    }

    [Fact]
    public void InMemorySnapshot_CannotBeRestoredIntoLsm()
    {
        using var memory = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        Ok(memory, "CREATE DATABASE Snap");
        Ok(memory, "USE Snap");
        DataVoSnapshot inMemory = memory.CreateSnapshot();

        using DataVoContext context = Open();
        Assert.Throws<InvalidOperationException>(() => context.RestoreSnapshot(inMemory));
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private static Dictionary<string, object?> Player(int id, string name, string team, float[] embedding) => new()
    {
        ["Id"] = id,
        ["Name"] = name,
        ["Team"] = team,
        ["Emb"] = embedding,
    };

    private static List<string> Players(DataVoContext context) =>
        [.. Rows(context, "SELECT Id, Name, Team FROM Players ORDER BY Id").Select(row => $"{row["Id"]}:{row["Name"]}:{row["Team"]}")];

    private static List<string> Events(DataVoContext context) =>
        [.. Rows(context, "SELECT Seq, Kind FROM Events").Select(row => $"{row["Seq"]}:{row["Kind"]}").OrderBy(text => text, StringComparer.Ordinal)];

    private static List<string> Ids(DataVoContext context, string sql) =>
        [.. Rows(context, sql).Select(row => Convert.ToString(row["Id"], System.Globalization.CultureInfo.InvariantCulture)!)];

    private static List<Dictionary<string, object?>> Rows(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        return result.Data;
    }

    private static void Ok(DataVoContext context, string sql)
    {
        foreach (var result in context.Execute(sql))
        {
            Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        }
    }
}
