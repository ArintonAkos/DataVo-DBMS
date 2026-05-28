using DataVo.Core;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class RuntimeDiagnosticsTests
{
    [Fact]
    public void Diagnostics_WhenDisabled_DoesNotRecordQueries()
    {
        using var context = CreateContext(StorageMode.InMemory);
        context.Diagnostics.Enabled = false;

        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50))");

        Assert.Null(context.Diagnostics.LastQuery);
        Assert.Empty(context.Diagnostics.GetRecentQueries());
        Assert.Empty(context.Diagnostics.GetSlowQueries());
    }

    [Fact]
    public void Diagnostics_RecordsSelectStats()
    {
        using var context = CreateContext(StorageMode.InMemory);
        context.Diagnostics.Enabled = true;

        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50))");
        context.Execute("INSERT INTO Players VALUES (1, 'Ada')");
        context.Diagnostics.Clear();

        context.Execute("SELECT Id, Name FROM Players WHERE Id = 1");

        RuntimeQueryStats? stats = context.Diagnostics.LastQuery;
        Assert.NotNull(stats);
        Assert.Equal("SELECT", stats.Operation);
        Assert.Equal(StorageMode.InMemory, stats.StorageMode);
        Assert.Equal("Players", Assert.Single(stats.Tables));
        Assert.False(stats.IsError);
        Assert.Equal(1, stats.RowsReturned);
        Assert.True(stats.RowsRead >= 1 || stats.RowsScanned >= 1);
        Assert.True(stats.Elapsed >= TimeSpan.Zero);
        Assert.Contains(stats.IndexesUsed, index => index.Contains("_PK_Players", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Diagnostics_RecordsSlowQueriesInBoundedRing()
    {
        using var context = CreateContext(StorageMode.InMemory);
        context.Diagnostics.Enabled = true;
        context.Diagnostics.SlowQueryThreshold = TimeSpan.Zero;
        context.Diagnostics.RecentQueryCapacity = 2;
        context.Diagnostics.SlowQueryCapacity = 2;

        context.Execute("CREATE TABLE Events (Id INT PRIMARY KEY, Kind VARCHAR(50))");
        context.Execute("INSERT INTO Events VALUES (1, 'spawn')");
        context.Execute("SELECT * FROM Events");

        IReadOnlyList<RuntimeQueryStats> recent = context.Diagnostics.GetRecentQueries();
        IReadOnlyList<RuntimeQueryStats> slow = context.Diagnostics.GetSlowQueries();

        Assert.Equal(2, recent.Count);
        Assert.Equal(2, slow.Count);
        Assert.All(slow, item => Assert.True(item.Elapsed >= TimeSpan.Zero));
    }

    [Fact]
    public void Diagnostics_RecordsDiskStorageMode()
    {
        string path = Path.Combine(Path.GetTempPath(), $"datavo_diag_disk_{Guid.NewGuid():N}");
        try
        {
            using var context = CreateContext(StorageMode.Disk, path);
            context.Diagnostics.Enabled = true;

            context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(50))");
            context.Execute("SELECT * FROM Items");

            RuntimeQueryStats? stats = context.Diagnostics.LastQuery;
            Assert.NotNull(stats);
            Assert.Equal(StorageMode.Disk, stats.StorageMode);
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
    public void Diagnostics_RecordsVectorIndexSearch()
    {
        using var context = CreateContext(StorageMode.InMemory);
        context.Diagnostics.Enabled = true;

        context.Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(50))");
        context.Execute("CREATE INDEX idx_emb ON Embeddings (Emb) USING HNSW");
        context.BulkInsert(
            "Embeddings",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Emb"] = new float[] { 1f, 0f, 0f }, ["Label"] = "combat" },
                new Dictionary<string, object?> { ["Id"] = 2, ["Emb"] = new float[] { 0f, 1f, 0f }, ["Label"] = "builder" }
            ]);
        context.Diagnostics.Clear();

        context.SearchNearest("Embeddings", "idx_emb", "[0.9,0.1,0]", topK: 1);

        RuntimeQueryStats? stats = context.Diagnostics.LastQuery;
        Assert.NotNull(stats);
        Assert.True(stats.VectorIndexUsed);
        Assert.Equal(1, stats.VectorTopK);
        Assert.Contains("idx_emb", stats.IndexesUsed);
    }

    [Fact]
    public void Diagnostics_RecordsBulkInsertTransactionStateErrors()
    {
        using var context = CreateContext(StorageMode.InMemory);
        context.Diagnostics.Enabled = true;
        context.Execute("CREATE TABLE Events (Id INT PRIMARY KEY, Kind VARCHAR(50))");
        context.Execute("BEGIN TRANSACTION");
        context.Diagnostics.Clear();

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => context.BulkInsert(
            "Events",
            [new Dictionary<string, object?> { ["Id"] = 1, ["Kind"] = "spawn" }]));

        RuntimeQueryStats? stats = context.Diagnostics.LastQuery;
        Assert.NotNull(stats);
        Assert.True(stats.IsError);
        Assert.Contains("active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("active transaction", stats.ErrorMessage, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Diagnostics_RecordsBulkInsertInsertErrors()
    {
        using var context = CreateContext(StorageMode.InMemory);
        context.Diagnostics.Enabled = true;
        context.Diagnostics.Clear();

        Exception ex = Assert.ThrowsAny<Exception>(() => context.BulkInsert(
            "MissingTable",
            [new Dictionary<string, object?> { ["Id"] = 1 }]));

        RuntimeQueryStats? stats = context.Diagnostics.LastQuery;
        Assert.NotNull(stats);
        Assert.True(stats.IsError);
        Assert.False(string.IsNullOrWhiteSpace(ex.Message));
        Assert.Equal(ex.Message, stats.ErrorMessage);
    }

    [Fact]
    public void Diagnostics_RecordsSearchNearestMaterializationErrors()
    {
        using var context = CreateContext(StorageMode.InMemory);
        context.Diagnostics.Enabled = true;

        context.Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(50))");
        context.Execute("CREATE INDEX idx_emb ON Embeddings (Emb) USING HNSW");
        context.Engine.IndexManager.InsertIntoVectorIndex([1f, 0f, 0f], 999L, "idx_emb", "Embeddings", CurrentDatabase(context));
        context.Diagnostics.Clear();

        Exception ex = Assert.ThrowsAny<Exception>(() => context.SearchNearest("Embeddings", "idx_emb", [1f, 0f, 0f], topK: 1));

        RuntimeQueryStats? stats = context.Diagnostics.LastQuery;
        Assert.NotNull(stats);
        Assert.True(stats.IsError);
        Assert.Contains("idx_emb", stats.IndexesUsed);
        Assert.Equal(ex.Message, stats.ErrorMessage);
    }

    private static DataVoContext CreateContext(StorageMode mode, string? diskPath = null)
    {
        var context = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = diskPath
        });

        string databaseName = $"Diag_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }

    private static string CurrentDatabase(DataVoContext context)
    {
        return context.Engine.Sessions.Get(context.SessionId)
            ?? throw new InvalidOperationException("Expected a selected database for the test context.");
    }
}
