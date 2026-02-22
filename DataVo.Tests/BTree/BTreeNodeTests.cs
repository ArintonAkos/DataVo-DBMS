using DataVo.Core.BTree;

namespace DataVo.Tests.BTree;

public class BTreeNodeTests
{
    [Fact]
    public void InsertAndSearch_SingleKey_ReturnsValue()
    {
        var node = new BTreeNode<string, string>(3, isLeaf: true);
        node.InsertNonFull("alice", "row1");

        var result = node.Search("alice");
        Assert.Single(result);
        Assert.Equal("row1", result[0]);
    }

    [Fact]
    public void Search_NonExistentKey_ReturnsEmpty()
    {
        var node = new BTreeNode<string, string>(3, isLeaf: true);
        node.InsertNonFull("alice", "row1");

        var result = node.Search("bob");
        Assert.Empty(result);
    }

    [Fact]
    public void InsertDuplicateKey_AppendsValue()
    {
        var node = new BTreeNode<string, string>(3, isLeaf: true);
        node.InsertNonFull("alice", "row1");
        node.InsertNonFull("alice", "row2");

        var result = node.Search("alice");
        Assert.Equal(2, result.Count);
        Assert.Contains("row1", result);
        Assert.Contains("row2", result);
    }

    [Fact]
    public void InsertMultipleKeys_MaintainsSortedOrder()
    {
        var node = new BTreeNode<string, string>(10, isLeaf: true);
        node.InsertNonFull("charlie", "row3");
        node.InsertNonFull("alice", "row1");
        node.InsertNonFull("bob", "row2");

        // Keys should be sorted
        Assert.Equal("alice", node.Keys[0]);
        Assert.Equal("bob", node.Keys[1]);
        Assert.Equal("charlie", node.Keys[2]);
    }

    [Fact]
    public void ContainsKey_ReturnsCorrectly()
    {
        var node = new BTreeNode<string, string>(3, isLeaf: true);
        node.InsertNonFull("alice", "row1");

        Assert.True(node.ContainsKey("alice"));
        Assert.False(node.ContainsKey("bob"));
    }

    [Fact]
    public void CollectAll_ReturnsAllEntries()
    {
        var node = new BTreeNode<string, string>(10, isLeaf: true);
        node.InsertNonFull("alice", "row1");
        node.InsertNonFull("bob", "row2");
        node.InsertNonFull("charlie", "row3");

        var results = new List<KeyValuePair<string, List<string>>>();
        node.CollectAll(results);

        Assert.Equal(3, results.Count);
        Assert.Equal("alice", results[0].Key);
        Assert.Equal("bob", results[1].Key);
        Assert.Equal("charlie", results[2].Key);
    }

    [Fact]
    public void IsFull_WorksCorrectly()
    {
        // t=2 means max keys = 2*2-1 = 3
        var node = new BTreeNode<string, string>(2, isLeaf: true);

        Assert.False(node.IsFull);

        node.InsertNonFull("a", "1");
        node.InsertNonFull("b", "2");
        node.InsertNonFull("c", "3");

        Assert.True(node.IsFull);
    }
}
