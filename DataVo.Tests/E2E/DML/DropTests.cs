using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;


public abstract class DropTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void DropTests_DropExistingTable_DropsSuccessfully()
    {
        Execute("CREATE TABLE Users (Id INT, Name VARCHAR, Age INT)");
        var result = ExecuteAndReturn("DROP TABLE Users");

        Assert.False(result.IsError);
        Assert.Empty(result.Data);
    }

    [Fact]
    public void DropTests_DropNonExistingTable_ThrowsError()
    {
        var result = ExecuteAndReturn("DROP TABLE NonExistingTable");

        Assert.True(result.IsError);
        Assert.Empty(result.Data);
    }

    [Fact]
    public void DropDatabase_ThenRecreate_DataIsGone()
    {
        Execute("CREATE TABLE Products (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("INSERT INTO Products VALUES (1, 'Widget')");

        // Verify data exists
        var before = ExecuteAndReturn("SELECT * FROM Products");
        Assert.Single(before.Data);

        // Drop and recreate
        Execute($"DROP DATABASE {TestDb}");
        Execute($"CREATE DATABASE {TestDb}");
        Execute($"USE {TestDb}");

        // Table should not exist anymore
        Execute("CREATE TABLE Products (Id INT PRIMARY KEY, Name VARCHAR)");
        var after = ExecuteAndReturn("SELECT * FROM Products");
        Assert.Empty(after.Data);
    }

    [Fact]
    public void DropDatabase_CleansIndexes_NoStalePKViolation()
    {
        Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("INSERT INTO Users VALUES (1, 'Alice')");

        // Drop and recreate — PK index cache must be cleared
        Execute($"DROP DATABASE {TestDb}");
        Execute($"CREATE DATABASE {TestDb}");
        Execute($"USE {TestDb}");

        Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)");

        // Same PK value should work — no stale index violation
        Execute("INSERT INTO Users VALUES (1, 'Bob')");

        var result = ExecuteAndReturn("SELECT * FROM Users");
        Assert.Single(result.Data);
        Assert.Equal("Bob", result.Data.First()["Name"]);
    }

    [Fact]
    public void DropDatabase_NonExistent_ThrowsError()
    {
        var result = ExecuteAndReturn("DROP DATABASE NoSuchDatabase");
        Assert.Contains(result.Messages, m => m.Contains("does not exist"));
    }
}


// --- Multiplexed XUnit Executions ---

[Collection("SequentialStorageTests")]
public class InMemoryDropTests : DropTestsBase
{
    public InMemoryDropTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "DropDB_Mem") { }
}

[Collection("SequentialStorageTests")]
public class DiskDropTests : DropTestsBase
{
    public DiskDropTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_drop" }, "DropDB_Disk") { }
}
