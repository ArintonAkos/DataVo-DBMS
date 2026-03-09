using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

public abstract class PredicateTestsBase : SqlExecutionTestsBase
{
    protected PredicateTestsBase(DataVoConfig config, string testDbName) : base(config, testDbName)
    {
        SeedProducts();
    }

    private void SeedProducts()
    {
        Execute("CREATE TABLE Products (Id INT PRIMARY KEY, Name VARCHAR(50), Price INT, Category VARCHAR(20))");
        Execute("INSERT INTO Products VALUES (1, 'Alpha', 10, 'standard')");
        Execute("INSERT INTO Products VALUES (2, 'Alps', 20, 'standard')");
        Execute("INSERT INTO Products VALUES (3, 'Beta', 30, 'standard')");
        Execute("INSERT INTO Products VALUES (4, 'Gamma', 40, 'standard')");
        Execute("INSERT INTO Products VALUES (5, 'Delta', 50, 'standard')");
    }

    [Fact]
    public void Select_In_FiltersMatchingRows()
    {
        var result = ExecuteAndReturn("SELECT * FROM Products WHERE Id IN (1, 3, 5)");

        var ids = result.Data.Select(row => (int)row["Id"]).OrderBy(id => id).ToList();
        Assert.Equal([1, 3, 5], ids);
    }

    [Fact]
    public void Select_Between_IsInclusive()
    {
        var result = ExecuteAndReturn("SELECT * FROM Products WHERE Price BETWEEN 20 AND 40");

        var ids = result.Data.Select(row => (int)row["Id"]).OrderBy(id => id).ToList();
        Assert.Equal([2, 3, 4], ids);
    }

    [Fact]
    public void Select_Like_SupportsPercentAndUnderscoreWildcards()
    {
        var result = ExecuteAndReturn("SELECT * FROM Products WHERE Name LIKE 'Al%' OR Name LIKE '_eta'");

        var names = result.Data.Select(row => row["Name"]?.ToString()).OrderBy(name => name).ToList();
        Assert.Equal(["Alpha", "Alps", "Beta"], names);
    }

    [Fact]
    public void Update_CanUseInPredicate()
    {
        Execute("UPDATE Products SET Category = 'priority' WHERE Id IN (2, 4)");

        var result = ExecuteAndReturn("SELECT * FROM Products WHERE Category = 'priority'");
        var ids = result.Data.Select(row => (int)row["Id"]).OrderBy(id => id).ToList();

        Assert.Equal([2, 4], ids);
    }

    [Fact]
    public void Delete_CanUseBetweenAndLikePredicates()
    {
        Execute("DELETE FROM Products WHERE Price BETWEEN 20 AND 30 OR Name LIKE 'G%'");

        var result = ExecuteAndReturn("SELECT * FROM Products");
        var ids = result.Data.Select(row => (int)row["Id"]).OrderBy(id => id).ToList();

        Assert.Equal([1, 5], ids);
    }
}

public class PredicateTestsMemory : PredicateTestsBase
{
    public PredicateTestsMemory() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "PredicateDB_Mem") { }
}

public class PredicateTestsDisk : PredicateTestsBase
{
    public PredicateTestsDisk() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "PredicateDB_Disk" }, "PredicateDB_Disk") { }
}