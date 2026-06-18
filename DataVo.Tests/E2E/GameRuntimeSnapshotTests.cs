using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;
using System.Reflection;
using System.Xml.Linq;

namespace DataVo.Tests.E2E;

public class GameRuntimeSnapshotTests
{
    [Fact]
    public void InMemorySnapshot_RestoreRevertsInsertedUpdatedAndDeletedRows()
    {
        using var context = CreateContext();

        ExecuteOk(context, "CREATE TABLE PlayerState (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        ExecuteOk(context, "INSERT INTO PlayerState VALUES (1, 'Ada', 5)");
        ExecuteOk(context, "INSERT INTO PlayerState VALUES (2, 'Bea', 8)");

        DataVoSnapshot snapshot = context.CreateSnapshot();

        ExecuteOk(context, "UPDATE PlayerState SET Level = 99 WHERE Id = 1");
        ExecuteOk(context, "DELETE FROM PlayerState WHERE Id = 2");
        ExecuteOk(context, "INSERT INTO PlayerState VALUES (3, 'Cai', 12)");

        context.RestoreSnapshot(snapshot);

        var rows = Select(context, "SELECT Id, Name, Level FROM PlayerState ORDER BY Id ASC");

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, (int)rows[0]["Id"]);
        Assert.Equal("Ada", rows[0]["Name"]);
        Assert.Equal(5, (int)rows[0]["Level"]);
        Assert.Equal(2, (int)rows[1]["Id"]);
        Assert.Equal("Bea", rows[1]["Name"]);
        Assert.Equal(8, (int)rows[1]["Level"]);
    }

    [Fact]
    public void InMemorySnapshot_RestorePreservesRowIdsAcrossTombstones()
    {
        using var context = CreateContext();

        ExecuteOk(context, "CREATE TABLE Events (Id INT PRIMARY KEY, Name VARCHAR(50))");
        ExecuteOk(context, "INSERT INTO Events VALUES (1, 'start')");
        ExecuteOk(context, "INSERT INTO Events VALUES (2, 'middle')");
        ExecuteOk(context, "DELETE FROM Events WHERE Id = 2");

        DataVoSnapshot snapshot = context.CreateSnapshot();

        ExecuteOk(context, "INSERT INTO Events VALUES (3, 'after')");
        context.RestoreSnapshot(snapshot);
        ExecuteOk(context, "INSERT INTO Events VALUES (4, 'restored')");

        var rows = Select(context, "SELECT Id, Name FROM Events ORDER BY Id ASC");

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, (int)rows[0]["Id"]);
        Assert.Equal("start", rows[0]["Name"]);
        Assert.Equal(4, (int)rows[1]["Id"]);
        Assert.Equal("restored", rows[1]["Name"]);
    }

    [Fact]
    public void InMemorySnapshot_RestoreKeepsPrimaryKeyIndexUsable()
    {
        using var context = CreateContext();

        ExecuteOk(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(50))");
        ExecuteOk(context, "INSERT INTO Items VALUES (1, 'Sword')");

        DataVoSnapshot snapshot = context.CreateSnapshot();

        ExecuteOk(context, "INSERT INTO Items VALUES (2, 'Shield')");
        context.RestoreSnapshot(snapshot);
        ExecuteOk(context, "INSERT INTO Items VALUES (2, 'Shield')");

        var rows = Select(context, "SELECT Name FROM Items WHERE Id = 2");

        Assert.Single(rows);
        Assert.Equal("Shield", rows[0]["Name"]);
    }

    [Fact]
    public void InMemorySnapshot_RestoreKeepsVectorIndexSearchUsable()
    {
        using var context = CreateContext();

        ExecuteOk(context, "CREATE TABLE UserEmbeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(50))");
        ExecuteOk(context, "INSERT INTO UserEmbeddings VALUES (1, '[1,0,0]', 'aggressive')");
        ExecuteOk(context, "INSERT INTO UserEmbeddings VALUES (2, '[0,1,0]', 'builder')");
        ExecuteOk(context, "CREATE INDEX idx_user_emb ON UserEmbeddings (Emb) USING HNSW");

        DataVoSnapshot snapshot = context.CreateSnapshot();

        ExecuteOk(context, "INSERT INTO UserEmbeddings VALUES (3, '[0,0,1]', 'explorer')");
        context.RestoreSnapshot(snapshot);

        List<Dictionary<string, object?>> results =
            context.SearchNearest("UserEmbeddings", "idx_user_emb", "[0.9,0.1,0]", topK: 1);

        Assert.Single(results);
        Assert.Equal("aggressive", results[0]["Label"]);
    }

    [Fact]
    public void InMemorySnapshot_RestoreRemovesVectorIndexesCreatedAfterSnapshot()
    {
        using var context = CreateContext();

        ExecuteOk(context, "CREATE TABLE UserEmbeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(50))");
        ExecuteOk(context, "INSERT INTO UserEmbeddings VALUES (1, '[1,0,0]', 'aggressive')");
        ExecuteOk(context, "INSERT INTO UserEmbeddings VALUES (2, '[0,1,0]', 'builder')");

        DataVoSnapshot snapshot = context.CreateSnapshot();

        ExecuteOk(context, "CREATE INDEX idx_user_emb ON UserEmbeddings (Emb) USING HNSW");

        List<Dictionary<string, object?>> indexedResults =
            context.SearchNearest("UserEmbeddings", "idx_user_emb", "[0.9,0.1,0]", topK: 1);

        Assert.Single(indexedResults);

        context.RestoreSnapshot(snapshot);

        IndexException ex = Assert.Throws<IndexException>(() =>
            context.SearchNearest("UserEmbeddings", "idx_user_emb", "[0.9,0.1,0]", topK: 1));

        Assert.Contains("does not exist", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InMemorySnapshot_FailedRestoreDoesNotExposePartialState()
    {
        using var context = CreateContext();

        string databaseName = context.Engine.Sessions.Get(context.SessionId)!;

        ExecuteOk(context, "CREATE TABLE UserEmbeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(50))");
        ExecuteOk(context, "INSERT INTO UserEmbeddings VALUES (1, '[1,0,0]', 'current')");
        ExecuteOk(context, "CREATE INDEX idx_user_emb ON UserEmbeddings (Emb) USING HNSW");

        DataVoSnapshot currentSnapshot = context.CreateSnapshot();
        DataVoSnapshot corruptedSnapshot = CreateCorruptedVectorSnapshot(currentSnapshot, databaseName);

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            context.RestoreSnapshot(corruptedSnapshot));

        Assert.Contains("Cannot rebuild vector index", ex.Message, StringComparison.OrdinalIgnoreCase);

        var rows = Select(context, "SELECT Id, Label FROM UserEmbeddings ORDER BY Id ASC");
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0]["Id"]);
        Assert.Equal("current", rows[0]["Label"]);

        List<Dictionary<string, object?>> results =
            context.SearchNearest("UserEmbeddings", "idx_user_emb", "[0.9,0.1,0]", topK: 1);

        Assert.Single(results);
        Assert.Equal("current", results[0]["Label"]);
    }

    [Fact]
    public void InMemorySnapshot_RestoreCanBeRepeatedForSimulationLoops()
    {
        using var context = CreateContext();

        ExecuteOk(context, "CREATE TABLE ScoreState (Id INT PRIMARY KEY, Score INT)");
        ExecuteOk(context, "INSERT INTO ScoreState VALUES (1, 10)");

        DataVoSnapshot snapshot = context.CreateSnapshot();

        for (int i = 0; i < 3; i++)
        {
            ExecuteOk(context, "UPDATE ScoreState SET Score = Score + 5 WHERE Id = 1");
            Assert.Equal(15, (int)Select(context, "SELECT Score FROM ScoreState WHERE Id = 1")[0]["Score"]);

            context.RestoreSnapshot(snapshot);
            Assert.Equal(10, (int)Select(context, "SELECT Score FROM ScoreState WHERE Id = 1")[0]["Score"]);
        }
    }

    [Fact]
    public void Snapshot_OnDiskStorageThrowsNotSupported()
    {
        string path = Path.Combine(Path.GetTempPath(), $"datavo_snapshot_disk_{Guid.NewGuid():N}");
        try
        {
            using var context = new DataVoContext(new DataVoConfig
            {
                StorageMode = StorageMode.Disk,
                DiskStoragePath = path
            });

            Assert.Throws<NotSupportedException>(() => context.CreateSnapshot());
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    [Fact]
    public void Snapshot_DuringActiveTransactionThrows()
    {
        using var context = CreateContext();

        ExecuteOk(context, "BEGIN TRANSACTION");

        Assert.Throws<InvalidOperationException>(() => context.CreateSnapshot());
    }

    [Fact]
    public void SnapshotAndRestore_RejectWhenAnotherSessionHasActiveTransaction()
    {
        using var context = CreateContext();

        DataVoSnapshot snapshot = context.CreateSnapshot();
        Guid otherSession = Guid.NewGuid();
        string databaseName = context.Engine.Sessions.Get(context.SessionId)!;

        ExecuteOk(context, $"USE {databaseName}", otherSession);
        ExecuteOk(context, "BEGIN TRANSACTION", otherSession);

        Assert.Throws<InvalidOperationException>(() => context.CreateSnapshot());
        Assert.Throws<InvalidOperationException>(() => context.RestoreSnapshot(snapshot));
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string dbName = $"GameRuntime_{Guid.NewGuid():N}";
        ExecuteOk(context, $"CREATE DATABASE {dbName}");
        ExecuteOk(context, $"USE {dbName}");
        return context;
    }

    private static void ExecuteOk(DataVoContext context, string sql)
    {
        ExecuteOk(context, sql, context.SessionId);
    }

    private static void ExecuteOk(DataVoContext context, string sql, Guid sessionId)
    {
        List<QueryResult> results = context.Execute(sql, sessionId).ToList();
        Assert.NotEmpty(results);
        Assert.All(results, result => Assert.False(result.IsError, string.Join(" | ", result.Messages)));
    }

    private static List<Dictionary<string, object?>> Select(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Single();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return result.Data;
    }

    private static DataVoSnapshot CreateCorruptedVectorSnapshot(DataVoSnapshot sourceSnapshot, string databaseName)
    {
        using var corruptSource = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk(corruptSource, $"CREATE DATABASE {databaseName}");
        ExecuteOk(corruptSource, $"USE {databaseName}");
        ExecuteOk(corruptSource, "CREATE TABLE UserEmbeddings (Id INT PRIMARY KEY, Emb VARCHAR(50), Label VARCHAR(50))");
        ExecuteOk(corruptSource, "INSERT INTO UserEmbeddings VALUES (1, 'not-a-vector', 'broken')");

        DataVoSnapshot corruptStorageSnapshot = corruptSource.CreateSnapshot();
        string corruptCatalogState = RewriteVectorColumnAsVarchar(sourceSnapshot.CatalogState, databaseName, "UserEmbeddings", "Emb");

        return CreateSnapshot(
            sourceSnapshot.StorageMode,
            corruptCatalogState,
            sourceSnapshot.SelectedDatabase,
            GetStorageSnapshot(corruptStorageSnapshot));
    }

    private static string RewriteVectorColumnAsVarchar(string catalogState, string databaseName, string tableName, string columnName)
    {
        XDocument doc = XDocument.Parse(catalogState);

        XElement attribute = doc
            .Descendants("Database")
            .Single(element => string.Equals((string?)element.Attribute("DatabaseName"), databaseName, StringComparison.OrdinalIgnoreCase))
            .Descendants("Table")
            .Single(element => string.Equals((string?)element.Attribute("TableName"), tableName, StringComparison.OrdinalIgnoreCase))
            .Descendants("Attribute")
            .Single(element => string.Equals((string?)element.Attribute("Name"), columnName, StringComparison.OrdinalIgnoreCase));

        attribute.SetAttributeValue("Type", "VARCHAR");
        attribute.SetAttributeValue("Length", 50);

        return doc.ToString(SaveOptions.DisableFormatting);
    }

    private static object GetStorageSnapshot(DataVoSnapshot snapshot)
    {
        PropertyInfo property = typeof(DataVoSnapshot).GetProperty(
            "StorageSnapshot",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

        return property.GetValue(snapshot)!;
    }

    private static DataVoSnapshot CreateSnapshot(
        StorageMode storageMode,
        string catalogState,
        string? selectedDatabase,
        object storageSnapshot)
    {
        ConstructorInfo constructor = typeof(DataVoSnapshot).GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single(info =>
            {
                ParameterInfo[] parameters = info.GetParameters();
                return parameters.Length == 4
                    && parameters[0].ParameterType == typeof(StorageMode)
                    && parameters[1].ParameterType == typeof(string)
                    && parameters[2].ParameterType == typeof(string)
                    && parameters[3].ParameterType.Name == "InMemoryStorageSnapshot";
            });

        return (DataVoSnapshot)constructor.Invoke([storageMode, catalogState, selectedDatabase, storageSnapshot]);
    }
}
