using DataVo.Core.Indexing;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.BTree;

/// <summary>
/// L3 hot-path proof: a warm scalar point lookup through <see cref="IndexManager.FilterUsingIndex"/>
/// must be allocation-free — no List/HashSet copy of the row ids and no per-call cache-key string.
/// </summary>
public class FilterUsingIndexAllocationTests : IDisposable
{
    private readonly string _dir;
    private readonly IndexManager _manager;
    private const string Table = "t";
    private readonly string _db = $"db_{Guid.NewGuid():N}";

    public FilterUsingIndexAllocationTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"datavo_fui_{Guid.NewGuid()}");
        Directory.CreateDirectory(_dir);
        _manager = new IndexManager(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = _dir }, _dir);

        var values = new Dictionary<string, List<long>>();
        for (int i = 0; i < 1000; i++) values[$"k{i}"] = [i];
        _manager.CreateIndex(values, "idx", Table, _db);
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    [Fact]
    public void FilterUsingIndex_WarmPointLookup_DoesNotAllocate()
    {
        for (int i = 0; i < 1000; i++) _ = _manager.FilterUsingIndex("k500", "idx", Table, _db);

        const int n = 100_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++) _ = _manager.FilterUsingIndex("k500", "idx", Table, _db);
        long perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        Assert.True(perCall <= 16, $"FilterUsingIndex warm point lookup {perCall} B exceeds 16 B (row-id copy or cache-key string not eliminated)");
    }

    [Fact]
    public void FilterUsingIndex_PointLookup_ReturnsExpectedRowId()
    {
        Assert.Equal(new[] { 500L }, _manager.FilterUsingIndex("k500", "idx", Table, _db));
        Assert.Empty(_manager.FilterUsingIndex("nope", "idx", Table, _db));
    }
}
