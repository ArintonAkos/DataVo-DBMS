using DataVo.Core.BTree;

namespace DataVo.Tests.BTree;

public class IndexManagerTests : IDisposable
{
    private readonly string _testDir;
    private const string TestDb = "test_db";
    private const string TestTable = "test_table";

    public IndexManagerTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"datavo_im_tests_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDir);
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
        var manager = IndexManager.Instance;
        var emptyData = new Dictionary<string, List<long>>();

        // This should not throw
        manager.CreateIndex(emptyData, "test_idx", TestTable, TestDb);
    }

    [Fact]
    public void CreateIndex_WithData_CanFilter()
    {
        var manager = IndexManager.Instance;
        var data = new Dictionary<string, List<long>>
        {
            { "42", new List<long> { 1, 2 } },
            { "43", new List<long> { 3 } }
        };

        manager.CreateIndex(data, "filter_idx", TestTable, TestDb);

        var result = manager.FilterUsingIndex("42", "filter_idx", TestTable, TestDb);
        Assert.Equal(2, result.Count);
        Assert.Contains(1L, result);
        Assert.Contains(2L, result);
    }

    [Fact]
    public void InsertIntoIndex_AddsEntryToExistingIndex()
    {
        var manager = IndexManager.Instance;
        manager.CreateIndex([], "insert_idx", TestTable, TestDb);

        manager.InsertIntoIndex("1", 99L, "insert_idx", TestTable, TestDb);

        var result = manager.FilterUsingIndex("1", "insert_idx", TestTable, TestDb);
        Assert.Single(result);
        Assert.Contains(99L, result);
    }

    [Fact]
    public void IndexContainsRow_ReturnsTrueForExistingValue()
    {
        var manager = IndexManager.Instance;
        var data = new Dictionary<string, List<long>>
        {
            { "99", new List<long> { 1 } }
        };
        manager.CreateIndex(data, "contains_idx", TestTable, TestDb);

        Assert.True(manager.IndexContainsRow(1L, "contains_idx", TestTable, TestDb));
        Assert.False(manager.IndexContainsRow(2L, "contains_idx", TestTable, TestDb));
    }

    [Fact]
    public void DeleteFromIndex_RemovesRowIds()
    {
        var manager = IndexManager.Instance;
        var data = new Dictionary<string, List<long>>
        {
            { "10", new List<long> { 1, 2 } },
            { "20", new List<long> { 3 } }
        };
        manager.CreateIndex(data, "delete_idx", TestTable, TestDb);

        manager.DeleteFromIndex([1L, 3L], "delete_idx", TestTable, TestDb);

        var resultA = manager.FilterUsingIndex("10", "delete_idx", TestTable, TestDb);
        Assert.Single(resultA);
        Assert.Contains(2L, resultA);

        var resultB = manager.FilterUsingIndex("20", "delete_idx", TestTable, TestDb);
        Assert.Empty(resultB);
    }

    [Fact]
    public void DropIndex_RemovesIndex()
    {
        var manager = IndexManager.Instance;
        var data = new Dictionary<string, List<long>>
        {
            { "50", new List<long> { 1 } }
        };
        manager.CreateIndex(data, "drop_idx", TestTable, TestDb);

        manager.DropIndex("drop_idx", TestTable, TestDb);

        // After dropping, trying to filter should throw because the index no longer exists
        Assert.Throws<Exception>(() =>
            manager.FilterUsingIndex("50", "drop_idx", TestTable, TestDb));
    }
}
