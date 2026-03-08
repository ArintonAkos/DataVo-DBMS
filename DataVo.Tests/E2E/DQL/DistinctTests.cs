using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

public abstract class DistinctTestsBase : SqlExecutionTestsBase
{
    protected DistinctTestsBase(DataVoConfig config, string testDbName) : base(config, testDbName)
    {
        EnsureTableExists();
    }

    protected void EnsureTableExists()
    {
        Execute($"CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(50), Role VARCHAR(20), Age INT);");

        // Insert duplicated rows structurally identical across specific columns
        Execute("INSERT INTO Users (Id, Name, Role, Age) VALUES (1, 'Alice', 'Admin', 30);");
        Execute("INSERT INTO Users (Id, Name, Role, Age) VALUES (2, 'Bob', 'User', 25);");
        Execute("INSERT INTO Users (Id, Name, Role, Age) VALUES (3, 'Alice', 'Admin', 30);");
        Execute("INSERT INTO Users (Id, Name, Role, Age) VALUES (4, 'Charlie', 'User', 35);");
        Execute("INSERT INTO Users (Id, Name, Role, Age) VALUES (5, 'David', 'Admin', 40);");
        Execute("INSERT INTO Users (Id, Name, Role, Age) VALUES (6, 'Alice', 'User', 30);");
        Execute("INSERT INTO Users (Id, Name, Role, Age) VALUES (7, 'Bob', 'User', 25);");
    }

    [Fact]
    public void SelectDistinct_SingleColumn_DeduplicatesProperly()
    {
        var result = ExecuteAndReturn("SELECT DISTINCT Role FROM Users");

        Assert.NotNull(result);
        Assert.Equal(2, result.Data.Count);

        var roles = result.Data.Select(r => r["Role"]?.ToString()).OrderBy(x => x).ToList();
        Assert.Equal(["Admin", "User"], roles);
    }

    [Fact]
    public void SelectDistinct_MultipleColumns_DeduplicatesUniquePairs()
    {
        var result = ExecuteAndReturn("SELECT DISTINCT Name, Role FROM Users");

        Assert.NotNull(result);
        Assert.Equal(5, result.Data.Count);

        // Alice/Admin, Bob/User, Charlie/User, David/Admin, Alice/User
        var pairs = result.Data.Select(r => $"{r["Name"]}-{r["Role"]}").OrderBy(x => x).ToList();

        Assert.Contains("Alice-Admin", pairs);
        Assert.Contains("Alice-User", pairs);
        Assert.Contains("Bob-User", pairs);
        Assert.Contains("Charlie-User", pairs);
        Assert.Contains("David-Admin", pairs);
    }

    [Fact]
    public void SelectDistinct_TableWildcard_DeduplicatesFullRow()
    {
        // For *. DISTINCT doesn't deduplicate anything if PK (Id) is always unique
        var result = ExecuteAndReturn("SELECT DISTINCT * FROM Users");

        Assert.NotNull(result);
        // We inserted 7 unique IDs natively.
        Assert.Equal(7, result.Data.Count);
    }
}

public class DistinctTestsDisk : DistinctTestsBase
{
    public DistinctTestsDisk() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_distinct" }, "DistinctDB_Disk") { }
}

public class DistinctTestsMemory : DistinctTestsBase
{
    public DistinctTestsMemory() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "DistinctDB_Mem") { }
}
