using DataVo.Core.BTree;

namespace DataVo.Tests.BTree;

public class JsonBTreeIndexTests
{
    [Fact]
    public void Insert_SingleEntry_CanBeSearched()
    {
        var index = new JsonBTreeIndex(3);
        index.Insert("alice", 1L);

        var result = index.Search("alice");
        Assert.Single(result);
        Assert.Equal(1L, result[0]);
    }

    [Fact]
    public void Search_NonExistentKey_ReturnsEmpty()
    {
        var index = new JsonBTreeIndex(3);
        index.Insert("alice", 1L);

        var result = index.Search("bob");
        Assert.Empty(result);
    }

    [Fact]
    public void Insert_DuplicateKeys_AccumulatesValues()
    {
        var index = new JsonBTreeIndex(3);
        index.Insert("color", 1L);
        index.Insert("color", 2L);
        index.Insert("color", 3L);

        var result = index.Search("color");
        Assert.Equal(3, result.Count);
        Assert.Contains(1L, result);
        Assert.Contains(2L, result);
        Assert.Contains(3L, result);
    }

    [Fact]
    public void ContainsValue_WorksCorrectly()
    {
        var index = new JsonBTreeIndex(3);
        index.Insert("alice", 1L);

        Assert.True(index.ContainsValue(1L));
        Assert.False(index.ContainsValue(99L));
    }

    [Fact]
    public void Delete_RemovesSpecificValue()
    {
        var index = new JsonBTreeIndex(3);
        index.Insert("alice", 1L);
        index.Insert("alice", 2L);

        index.Delete("alice", 1L);

        var result = index.Search("alice");
        Assert.Single(result);
        Assert.Equal(2L, result[0]);
    }

    [Fact]
    public void Delete_LastValue_RemovesKey()
    {
        var index = new JsonBTreeIndex(3);
        index.Insert("alice", 1L);

        index.Delete("alice", 1L);

        Assert.False(index.ContainsValue(1L));
        Assert.Empty(index.Search("alice"));
    }

    [Fact]
    public void DeleteValues_RemovesMultipleRowIds()
    {
        var index = new JsonBTreeIndex(3);
        index.Insert("key1", 1L);
        index.Insert("key1", 2L);
        index.Insert("key2", 3L);
        index.Insert("key3", 1L); // 1L is also under key3

        index.DeleteValues([1L, 3L]);

        // key1 should only have 2L
        var result1 = index.Search("key1");
        Assert.Single(result1);
        Assert.Equal(2L, result1[0]);

        // key2 should be gone (only had 3L)
        Assert.Empty(index.Search("key2"));

        // key3 should be gone (only had 1L)
        Assert.Empty(index.Search("key3"));
    }

    [Fact]
    public void Insert_ManyEntries_CausesSplitsAndSearchWorks()
    {
        // Use small degree to force many splits
        var index = new JsonBTreeIndex(2);

        for (int i = 0; i < 100; i++)
        {
            index.Insert($"key_{i:D4}", i);
        }

        // Verify all entries can be found
        for (int i = 0; i < 100; i++)
        {
            var result = index.Search($"key_{i:D4}");
            Assert.Single(result);
            Assert.Equal(i, result[0]);
        }
    }

    [Fact]
    public void Insert_ManyDuplicateKeys_AllValuesRetrieved()
    {
        var index = new JsonBTreeIndex(2);
        int count = 50;

        for (int i = 0; i < count; i++)
        {
            index.Insert("same_key", i);
        }

        var result = index.Search("same_key");
        Assert.Equal(count, result.Count);

        for (int i = 0; i < count; i++)
        {
            Assert.Contains(i, result);
        }
    }
}
