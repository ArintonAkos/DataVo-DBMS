using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

public abstract class UnionTestsBase : SqlExecutionTestsBase
{
    protected UnionTestsBase(DataVoConfig config, string testDbName) : base(config, testDbName)
    {
        Seed();
    }

    private void Seed()
    {
        Execute("CREATE TABLE Developers (Id INT PRIMARY KEY, Name VARCHAR(50))");
        Execute("CREATE TABLE Designers (Id INT PRIMARY KEY, Name VARCHAR(50))");

        Execute("INSERT INTO Developers VALUES (1, 'Alice')");
        Execute("INSERT INTO Developers VALUES (2, 'Bob')");
        Execute("INSERT INTO Designers VALUES (10, 'Bob')");
        Execute("INSERT INTO Designers VALUES (11, 'Cara')");
    }

    [Fact]
    public void Select_Union_RemovesDuplicates()
    {
        var result = ExecuteAndReturn("SELECT Name FROM Developers UNION SELECT Name FROM Designers");

        Assert.False(result.IsError);
        var names = result.Data.Select(row => row["Name"]?.ToString()).OrderBy(name => name).ToList();
        Assert.Equal(["Alice", "Bob", "Cara"], names);
    }

    [Fact]
    public void Select_UnionAll_PreservesDuplicates()
    {
        var result = ExecuteAndReturn("SELECT Name FROM Developers UNION ALL SELECT Name FROM Designers");

        Assert.False(result.IsError);
        var names = result.Data.Select(row => row["Name"]?.ToString()).OrderBy(name => name).ToList();
        Assert.Equal(["Alice", "Bob", "Bob", "Cara"], names);
    }

    [Fact]
    public void Select_Union_UsesFirstSelectColumnNames()
    {
        var result = ExecuteAndReturn("SELECT Name AS PersonName FROM Developers UNION SELECT Name FROM Designers");

        Assert.False(result.IsError);
        Assert.All(result.Data, row => Assert.True(row.ContainsKey("PersonName")));
    }

    [Fact]
    public void Select_Union_WithMismatchedColumnCounts_ReturnsError()
    {
        var result = ExecuteAndReturn("SELECT Id, Name FROM Developers UNION SELECT Name FROM Designers");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("same number of columns", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Select_ChainedUnionAndUnionAll_AppliesLeftAssociativeSemantics()
    {
        var result = ExecuteAndReturn("SELECT Name FROM Developers UNION ALL SELECT Name FROM Designers UNION SELECT Name FROM Developers");

        Assert.False(result.IsError);
        var names = result.Data.Select(row => row["Name"]?.ToString()).OrderBy(name => name).ToList();
        Assert.Equal(["Alice", "Bob", "Cara"], names);
    }

    [Fact]
    public void Select_Union_SupportsCompoundOrderByAndLimit()
    {
        var result = ExecuteAndReturn("SELECT Name FROM Developers UNION SELECT Name FROM Designers ORDER BY Name DESC LIMIT 2");

        Assert.False(result.IsError);
        var names = result.Data.Select(row => row["Name"]?.ToString()).ToList();
        Assert.Equal(["Cara", "Bob"], names);
    }

    [Fact]
    public void Select_ParenthesizedTopLevelCompoundQuery_IsRejectedExplicitly()
    {
        var result = ExecuteAndReturn("(SELECT Name FROM Developers UNION SELECT Name FROM Designers)");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("Parenthesized SELECT or compound queries are not supported yet", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Select_ParenthesizedUnionBranch_IsRejectedExplicitly()
    {
        var result = ExecuteAndReturn("SELECT Name FROM Developers UNION (SELECT Name FROM Designers)");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("Parenthesized UNION branches are not supported yet", StringComparison.OrdinalIgnoreCase));
    }
}

public class UnionTestsMemory : UnionTestsBase
{
    public UnionTestsMemory() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "UnionDB_Mem") { }
}

public class UnionTestsDisk : UnionTestsBase
{
    public UnionTestsDisk() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "UnionDB_Disk" }, "UnionDB_Disk") { }
}