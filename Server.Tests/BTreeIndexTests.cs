using Server.Server.BTree;

namespace Server.Tests;

public class BTreeIndexTests
{
    [Fact]
    public void Insert_SingleEntry_CanBeSearched()
    {
        var index = new BTreeIndex(3);
        index.Insert("alice", "row1");

        var result = index.Search("alice");
        Assert.Single(result);
        Assert.Equal("row1", result[0]);
    }

    [Fact]
    public void Search_NonExistentKey_ReturnsEmpty()
    {
        var index = new BTreeIndex(3);
        index.Insert("alice", "row1");

        var result = index.Search("bob");
        Assert.Empty(result);
    }

    [Fact]
    public void Insert_DuplicateKeys_AccumulatesValues()
    {
        var index = new BTreeIndex(3);
        index.Insert("color", "red_row");
        index.Insert("color", "blue_row");
        index.Insert("color", "green_row");

        var result = index.Search("color");
        Assert.Equal(3, result.Count);
        Assert.Contains("red_row", result);
        Assert.Contains("blue_row", result);
        Assert.Contains("green_row", result);
    }

    [Fact]
    public void ContainsValue_WorksCorrectly()
    {
        var index = new BTreeIndex(3);
        index.Insert("alice", "row1");

        Assert.True(index.ContainsValue("alice"));
        Assert.False(index.ContainsValue("bob"));
    }

    [Fact]
    public void Delete_RemovesSpecificValue()
    {
        var index = new BTreeIndex(3);
        index.Insert("alice", "row1");
        index.Insert("alice", "row2");

        index.Delete("alice", "row1");

        var result = index.Search("alice");
        Assert.Single(result);
        Assert.Equal("row2", result[0]);
    }

    [Fact]
    public void Delete_LastValue_RemovesKey()
    {
        var index = new BTreeIndex(3);
        index.Insert("alice", "row1");

        index.Delete("alice", "row1");

        Assert.False(index.ContainsValue("alice"));
        Assert.Empty(index.Search("alice"));
    }

    [Fact]
    public void DeleteValues_RemovesMultipleRowIds()
    {
        var index = new BTreeIndex(3);
        index.Insert("key1", "row1");
        index.Insert("key1", "row2");
        index.Insert("key2", "row3");
        index.Insert("key3", "row1"); // row1 is also under key3

        index.DeleteValues(new List<string> { "row1", "row3" });

        // key1 should only have row2
        var result1 = index.Search("key1");
        Assert.Single(result1);
        Assert.Equal("row2", result1[0]);

        // key2 should be gone (only had row3)
        Assert.Empty(index.Search("key2"));

        // key3 should be gone (only had row1)
        Assert.Empty(index.Search("key3"));
    }

    [Fact]
    public void Insert_ManyEntries_CausesSplitsAndSearchWorks()
    {
        // Use small degree to force many splits
        var index = new BTreeIndex(2);

        for (int i = 0; i < 100; i++)
        {
            index.Insert($"key_{i:D4}", $"row_{i}");
        }

        // Verify all entries can be found
        for (int i = 0; i < 100; i++)
        {
            var result = index.Search($"key_{i:D4}");
            Assert.Single(result);
            Assert.Equal($"row_{i}", result[0]);
        }
    }

    [Fact]
    public void Insert_ManyDuplicateKeys_AllValuesRetrieved()
    {
        var index = new BTreeIndex(2);
        int count = 50;

        for (int i = 0; i < count; i++)
        {
            index.Insert("same_key", $"row_{i}");
        }

        var result = index.Search("same_key");
        Assert.Equal(count, result.Count);

        for (int i = 0; i < count; i++)
        {
            Assert.Contains($"row_{i}", result);
        }
    }
}
