using DataVo.Core.BTree;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.BTree;

public class IndexManagerTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _testDb = $"test_db_{Guid.NewGuid():N}";
    private const string TestTable = "test_table";
    private readonly IndexManager _manager;

    public IndexManagerTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"datavo_im_tests_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDir);
        _manager = new IndexManager(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = _testDir }, _testDir);
    }

    public void Dispose()
    {
        // Clean up the IndexManager cache (use reflection to clear singleton state)
        // Since IndexManager is a singleton, tests can interfere. We'll test via public API.
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, recursive: true);
    }

    [Fact]
    public void CreateIndex_WithEmptyData_CreatesIndex()
    {
        var emptyData = new Dictionary<string, List<long>>();

        // This should not throw
        _manager.CreateIndex(emptyData, "test_idx", TestTable, _testDb);
    }

    [Fact]
    public void CreateIndex_WithData_CanFilter()
    {
        var data = new Dictionary<string, List<long>>
        {
            { "42", new List<long> { 1, 2 } },
            { "43", new List<long> { 3 } }
        };

        _manager.CreateIndex(data, "filter_idx", TestTable, _testDb);

        var result = _manager.FilterUsingIndex("42", "filter_idx", TestTable, _testDb);
        Assert.Equal(2, result.Count);
        Assert.Contains(1L, result);
        Assert.Contains(2L, result);
    }

    [Fact]
    public void InsertIntoIndex_AddsEntryToExistingIndex()
    {
        _manager.CreateIndex([], "insert_idx", TestTable, _testDb);

        _manager.InsertIntoIndex("1", 99L, "insert_idx", TestTable, _testDb);

        var result = _manager.FilterUsingIndex("1", "insert_idx", TestTable, _testDb);
        Assert.Single(result);
        Assert.Contains(99L, result);
    }

    [Fact]
    public void IndexContainsRow_ReturnsTrueForExistingValue()
    {
        var data = new Dictionary<string, List<long>>
        {
            { "99", new List<long> { 1 } }
        };
        _manager.CreateIndex(data, "contains_idx", TestTable, _testDb);

        Assert.True(_manager.IndexContainsRow(1L, "contains_idx", TestTable, _testDb));
        Assert.False(_manager.IndexContainsRow(2L, "contains_idx", TestTable, _testDb));
    }

    [Fact]
    public void DeleteFromIndex_RemovesRowIds()
    {
        var data = new Dictionary<string, List<long>>
        {
            { "10", new List<long> { 1, 2 } },
            { "20", new List<long> { 3 } }
        };
        _manager.CreateIndex(data, "delete_idx", TestTable, _testDb);

        _manager.DeleteFromIndex([1L, 3L], "delete_idx", TestTable, _testDb);

        var resultA = _manager.FilterUsingIndex("10", "delete_idx", TestTable, _testDb);
        Assert.Single(resultA);
        Assert.Contains(2L, resultA);

        var resultB = _manager.FilterUsingIndex("20", "delete_idx", TestTable, _testDb);
        Assert.Empty(resultB);
    }

    [Fact]
    public void DropIndex_RemovesIndex()
    {
        var data = new Dictionary<string, List<long>>
        {
            { "50", new List<long> { 1 } }
        };
        _manager.CreateIndex(data, "drop_idx", TestTable, _testDb);

        _manager.DropIndex("drop_idx", TestTable, _testDb);

        // After dropping, trying to filter should throw because the index no longer exists
        Assert.Throws<Exception>(() =>
            _manager.FilterUsingIndex("50", "drop_idx", TestTable, _testDb));
    }
}
