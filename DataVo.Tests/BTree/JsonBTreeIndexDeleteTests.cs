using DataVo.Core.BTree;

namespace DataVo.Tests.BTree;

/// <summary>Row-id removal edits the tree in place (no full rebuild) and keeps lookups exact.</summary>
public sealed class JsonBTreeIndexDeleteTests
{
    [Fact]
    public void DeleteValues_RemovesRowIdsAcrossKeys_AndEmptyKeysAreAbsent()
    {
        JsonBTreeIndex index = Build(keys: 500, rowsPerKey: 3);

        index.DeleteValues([1, 2, 3, 1500]); // key "k0000" loses all its rows; "k0499" loses one

        Assert.False(index.ContainsKey("k0000"));
        Assert.Empty(index.SearchReadOnly("k0000"));
        Assert.Equal([1498L, 1499L], index.SearchReadOnly("k0499"));
        Assert.False(index.ContainsValue(2));
        Assert.True(index.ContainsKey("k0250"));
    }

    [Fact]
    public void RemoveValue_RemovesExactlyOneMapping()
    {
        JsonBTreeIndex index = Build(keys: 500, rowsPerKey: 2);

        Assert.True(index.RemoveValue("k0100", 201));
        Assert.False(index.RemoveValue("k0100", 201));
        Assert.False(index.RemoveValue("missing", 1));

        Assert.Equal([202L], index.SearchReadOnly("k0100"));
        Assert.True(index.RemoveValue("k0100", 202));
        Assert.False(index.ContainsKey("k0100"));
    }

    [Fact]
    public void ManyRemovals_TriggerCompaction_WithoutLosingLiveEntries()
    {
        JsonBTreeIndex index = Build(keys: 10_000, rowsPerKey: 1);

        for (int i = 1; i <= 9_000; i++)
        {
            index.RemoveValue($"k{i - 1:D4}", i);
        }

        for (int i = 9_001; i <= 10_000; i++)
        {
            Assert.Equal([(long)i], index.SearchReadOnly($"k{i - 1:D4}"));
        }

        Assert.False(index.ContainsKey("k0042"));
    }

    [Fact]
    public void PublishedValueList_IsNotMutatedByDeletes()
    {
        JsonBTreeIndex index = Build(keys: 10, rowsPerKey: 3);
        IReadOnlyList<long> before = index.SearchReadOnly("k0005");

        index.DeleteValues([before[0]]);

        Assert.Equal(3, before.Count); // a reader holding the old list keeps a consistent snapshot
        Assert.Equal(2, index.SearchReadOnly("k0005").Count);
    }

    private static JsonBTreeIndex Build(int keys, int rowsPerKey)
    {
        var index = new JsonBTreeIndex(minDegree: 4);
        long rowId = 1;
        for (int k = 0; k < keys; k++)
        {
            for (int r = 0; r < rowsPerKey; r++)
            {
                index.Insert($"k{k:D4}", rowId++);
            }
        }

        return index;
    }
}
