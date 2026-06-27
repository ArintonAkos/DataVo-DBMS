using DataVo.Core.Indexing.Flat;

namespace DataVo.Tests.Indexing;

public class FlatVectorIndexTests
{
    [Fact]
    public void SearchTopK_ReturnsExactCosineOrdering()
    {
        var index = new FlatVectorIndex { Metric = "cosine" };

        index.Insert(30, [0f, 1f, 0f]);
        index.Insert(10, [1f, 0f, 0f]);
        index.Insert(20, [0.8f, 0.2f, 0f]);

        List<long> nearest = index.SearchTopK([0.95f, 0.05f, 0f], 3);

        Assert.Equal([10L, 20L, 30L], nearest);
    }

    [Fact]
    public void InsertUpdateDeleteAndClear_UpdateSearchableRows()
    {
        var index = new FlatVectorIndex { Metric = "cosine" };

        index.Insert(1, [1f, 0f, 0f]);
        index.Insert(2, [0f, 1f, 0f]);
        index.Insert(1, [0f, 0f, 1f]);

        Assert.Equal([1L], index.SearchTopK([0f, 0f, 1f], 1));

        index.Delete([1]);

        Assert.Equal([2L], index.SearchTopK([0f, 0f, 1f], 1));

        index.Clear();

        Assert.Empty(index.SearchTopK([0f, 1f, 0f], 1));
        Assert.Equal(0, index.Count);
    }

    [Fact]
    public void Factory_CreatesEuclideanFlatIndex()
    {
        var factory = new FlatVectorIndexFactory();
        var index = Assert.IsType<FlatVectorIndex>(factory.CreateIndex(
            "idx",
            "Emb",
            new Dictionary<string, object> { ["metric"] = "euclidean" }));

        index.Insert(1, [10f, 0f]);
        index.Insert(2, [1f, 1f]);

        Assert.Equal("euclidean", index.Metric);
        Assert.Equal([2L], index.SearchTopK([1f, 0f], 1));
    }

    [Fact]
    public void Insert_AfterReserve_DoesNotAllocatePerVectorStorage()
    {
        var index = new FlatVectorIndex { Metric = "cosine" };
        index.Reserve(expectedCount: 4, vectorDimension: 3);

        float[] vector = [1f, 0f, 0f];

        long before = GC.GetAllocatedBytesForCurrentThread();
        index.Insert(1, vector);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allocated <= 128, $"Expected reserved flat insert to avoid per-vector allocation, allocated {allocated} bytes.");
        vector[0] = 0f;
        vector[1] = 1f;

        Assert.Equal([1L], index.SearchTopK([1f, 0f, 0f], 1));
    }

    [Fact]
    public void SearchTopK_UsesReservedSlabAfterDeleteReuse()
    {
        var index = new FlatVectorIndex { Metric = "cosine" };
        index.Reserve(expectedCount: 2, vectorDimension: 3);

        index.Insert(1, [1f, 0f, 0f]);
        index.Insert(2, [0f, 1f, 0f]);
        index.Delete([1]);
        index.Insert(3, [0f, 0f, 1f]);

        Assert.Equal([3L, 2L], index.SearchTopK([0f, 0f, 1f], 2));
    }
}
