using DataVo.Core.Indexing;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Indexing;

public class IndexManagerTests : IDisposable
{
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

    public void Dispose()
    {
        _manager.Dispose();
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, recursive: true);
        }
    }
}
