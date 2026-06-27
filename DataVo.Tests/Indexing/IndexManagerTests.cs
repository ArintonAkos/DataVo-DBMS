using DataVo.Core.Indexing;
using DataVo.Core.StorageEngine.Config;
using System.Reflection;

namespace DataVo.Tests.Indexing;

public class IndexManagerTests : IDisposable
{
    private sealed class FakeManagedIndex : IIndexBase
    {
        public string IndexType => "FAIL";
    }

    private sealed class FailingDeleteFactory : IIndexFactory
    {
        public string IndexType => "FAIL";

        public object CreateIndex(string indexName, string columnName, Dictionary<string, object> @params)
        {
            return new FakeManagedIndex();
        }

        public object LoadIndex(string filePath, IIndexPersistence persistence)
        {
            return new FakeManagedIndex();
        }
    }

    private sealed class FailingDeletePersistence : IIndexPersistence
    {
        public string FileExtension => ".fail";

        public void SaveIndex(object index, string filePath) { }

        public object LoadIndex(string filePath) => new object();

        public void Flush(object index) { }

        public bool FileExists(string filePath) => File.Exists(filePath);

        public void EnsureDirectory(string directoryPath)
        {
            Directory.CreateDirectory(directoryPath);
        }

        public bool TryDeleteFile(string filePath) => false;

        public bool TryDeleteDirectory(string directoryPath) => false;
    }

    private readonly string _testDir;
    private readonly IndexManager _manager;

    public IndexManagerTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"datavo_imv2_tests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        _manager = new IndexManager(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = _testDir }, _testDir);
    }

    [Fact]
    public void VectorIndex_CreateAndSearch_ReturnsNearestRows()
    {
        _manager.CreateVectorIndex(
            [
                (1L, new[] { 1f, 0f, 0f }),
                (2L, new[] { 0f, 1f, 0f })
            ],
            "idx_v2",
            "Embeddings",
            "DbV2",
            "cosine");

        List<long> rowIds = _manager.SearchVector([0.9f, 0.1f, 0f], 1, "idx_v2", "Embeddings", "DbV2");

        Assert.Single(rowIds);
        Assert.Equal(1L, rowIds[0]);
    }

    [Fact]
    public void VectorIndex_InsertAndDelete_UpdatesSearchResults()
    {
        _manager.CreateVectorIndex(
            [
                (1L, new[] { 1f, 0f, 0f })
            ],
            "idx_v2_mut",
            "Embeddings",
            "DbV2",
            "cosine");

        _manager.InsertIntoVectorIndex([0f, 1f, 0f], 2L, "idx_v2_mut", "Embeddings", "DbV2");
        List<long> nearestB = _manager.SearchVector([0.1f, 0.9f, 0f], 1, "idx_v2_mut", "Embeddings", "DbV2");

        Assert.Single(nearestB);
        Assert.Equal(2L, nearestB[0]);

        _manager.DeleteFromVectorIndex([2L], "idx_v2_mut", "Embeddings", "DbV2");
        List<long> nearestAfterDelete = _manager.SearchVector([0.1f, 0.9f, 0f], 1, "idx_v2_mut", "Embeddings", "DbV2");

        Assert.Single(nearestAfterDelete);
        Assert.Equal(1L, nearestAfterDelete[0]);
    }

    [Fact]
    public void SupportsVectorIndexType_ReturnsExpectedCapabilities()
    {
        Assert.True(_manager.SupportsVectorIndexType("HNSW"));
        Assert.False(_manager.SupportsVectorIndexType("BTREE"));
        Assert.False(_manager.SupportsVectorIndexType(""));
    }

    [Fact]
    public void VectorIndex_WithExplicitIndexType_RoundTrips()
    {
        _manager.CreateVectorIndex(
            [
                (10L, new[] { 1f, 0f, 0f }),
                (20L, new[] { 0f, 1f, 0f })
            ],
            "idx_v2_typed",
            "Embeddings",
            "DbV2",
            metric: "cosine",
            indexType: "HNSW");

        List<long> rowIds = _manager.SearchVector([0.9f, 0.1f, 0f], 1, "idx_v2_typed", "Embeddings", "DbV2", indexType: "HNSW");

        Assert.Single(rowIds);
        Assert.Equal(10L, rowIds[0]);
    }

    [Fact]
    public void DropIndex_ThrowsWhenPersistenceDeleteFails()
    {
        _manager.RegisterIndexType("FAIL", new FailingDeleteFactory(), new FailingDeletePersistence());

        string tableDirectory = Path.Combine(_testDir, "DbFail", "TblFail");
        Directory.CreateDirectory(tableDirectory);
        string failingPath = Path.Combine(tableDirectory, "idx_fail.fail");
        File.WriteAllText(failingPath, "payload");

        IOException ex = Assert.Throws<IOException>(() =>
            _manager.DropIndex("idx_fail", "TblFail", "DbFail"));

        Assert.Contains("Failed to delete index file", ex.Message);
    }

    [Fact]
    public void FlushInternal_ThrowsWhenPersistenceHandlerMissing()
    {
        var cacheField = typeof(IndexManager).GetField("_cache", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var metadataField = typeof(IndexManager).GetField("_metadata", BindingFlags.Instance | BindingFlags.NonPublic)!;

        var cache = (Dictionary<(string, string, string), IIndexBase>)cacheField.GetValue(_manager)!;
        var metadata = (Dictionary<(string, string, string), IndexMetadata>)metadataField.GetValue(_manager)!;

        var cacheKey = ("db", "table", "idx");
        cache[cacheKey] = new FakeManagedIndex();
        metadata[cacheKey] = new IndexMetadata
        {
            IndexName = "idx",
            DatabaseName = "db",
            TableName = "table",
            ColumnName = "col",
            IndexType = "UNREGISTERED",
            PersistenceFormat = "none"
        };

        var flushMethod = typeof(IndexManager).GetMethod("FlushInternal", BindingFlags.Instance | BindingFlags.NonPublic)!;

        var ex = Assert.Throws<TargetInvocationException>(() => flushMethod.Invoke(_manager, [cacheKey]));
        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Contains("No persistence handler registered", ex.InnerException!.Message);
    }

    [Fact]
    public async Task ScalarIndexMutations_CompleteWithoutBlocking()
    {
        _manager.CreateIndex([], "locksafe_idx", "Users", "Db");

        Task insertTask = Task.Run(() =>
            _manager.InsertIntoIndex("42", 101L, "locksafe_idx", "Users", "Db"));
        await insertTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.True(insertTask.IsCompletedSuccessfully, "InsertIntoIndex should not block.");

        Task deleteTask = Task.Run(() =>
            _manager.DeleteFromIndex([101L], "locksafe_idx", "Users", "Db"));
        await deleteTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.True(deleteTask.IsCompletedSuccessfully, "DeleteFromIndex should not block.");
    }

    public void Dispose()
    {
        _manager.Dispose();
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, recursive: true);
        }
    }
}
