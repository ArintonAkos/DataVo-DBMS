using Server.Server.BTree;

namespace Server.Tests;

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
        var emptyData = new Dictionary<string, List<string>>();

        // This should not throw
        manager.CreateIndex(emptyData, "test_idx", TestTable, TestDb);
    }

    [Fact]
    public void CreateIndex_WithData_CanFilter()
    {
        var manager = IndexManager.Instance;
        var data = new Dictionary<string, List<string>>
        {
            { "alice", new List<string> { "row1", "row2" } },
            { "bob", new List<string> { "row3" } }
        };

        manager.CreateIndex(data, "filter_idx", TestTable, TestDb);

        var result = manager.FilterUsingIndex("alice", "filter_idx", TestTable, TestDb);
        Assert.Equal(2, result.Count);
        Assert.Contains("row1", result);
        Assert.Contains("row2", result);
    }

    [Fact]
    public void InsertIntoIndex_AddsEntryToExistingIndex()
    {
        var manager = IndexManager.Instance;
        manager.CreateIndex(new Dictionary<string, List<string>>(), "insert_idx", TestTable, TestDb);

        manager.InsertIntoIndex("keyA", "rowX", "insert_idx", TestTable, TestDb);

        var result = manager.FilterUsingIndex("keyA", "insert_idx", TestTable, TestDb);
        Assert.Single(result);
        Assert.Contains("rowX", result);
    }

    [Fact]
    public void IndexContainsRow_ReturnsTrueForExistingValue()
    {
        var manager = IndexManager.Instance;
        var data = new Dictionary<string, List<string>>
        {
            { "keyA", new List<string> { "row1" } }
        };
        manager.CreateIndex(data, "contains_idx", TestTable, TestDb);

        Assert.True(manager.IndexContainsRow("keyA", "contains_idx", TestTable, TestDb));
        Assert.False(manager.IndexContainsRow("nonexistent", "contains_idx", TestTable, TestDb));
    }

    [Fact]
    public void DeleteFromIndex_RemovesRowIds()
    {
        var manager = IndexManager.Instance;
        var data = new Dictionary<string, List<string>>
        {
            { "keyA", new List<string> { "row1", "row2" } },
            { "keyB", new List<string> { "row3" } }
        };
        manager.CreateIndex(data, "delete_idx", TestTable, TestDb);

        manager.DeleteFromIndex(new List<string> { "row1", "row3" }, "delete_idx", TestTable, TestDb);

        var resultA = manager.FilterUsingIndex("keyA", "delete_idx", TestTable, TestDb);
        Assert.Single(resultA);
        Assert.Contains("row2", resultA);

        var resultB = manager.FilterUsingIndex("keyB", "delete_idx", TestTable, TestDb);
        Assert.Empty(resultB);
    }

    [Fact]
    public void DropIndex_RemovesIndex()
    {
        var manager = IndexManager.Instance;
        var data = new Dictionary<string, List<string>>
        {
            { "keyA", new List<string> { "row1" } }
        };
        manager.CreateIndex(data, "drop_idx", TestTable, TestDb);

        manager.DropIndex("drop_idx", TestTable, TestDb);

        // After dropping, trying to filter should throw because the index no longer exists
        Assert.Throws<Exception>(() =>
            manager.FilterUsingIndex("keyA", "drop_idx", TestTable, TestDb));
    }
}
