using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;


public abstract class CreateTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void CreateTable_SimpleTable_CreatesSuccessfully()
    {
        var result = ExecuteAndReturn("CREATE TABLE Users (Id INT, Name VARCHAR, Age INT)");

        Assert.False(result.IsError);
        Assert.Empty(result.Data);
    }

    [Fact]
    public void CreateTable_WithPrimaryKey_CreatesSuccessfully()
    {
        var result = ExecuteAndReturn("CREATE TABLE Orders (OrderId INT PRIMARY KEY, OrderDate DATE, Amount DOUBLE)");

        Assert.False(result.IsError);
        Assert.Empty(result.Data);
    }

    [Fact]
    public void CreateTable_WithMultipleColumns_CreatesSuccessfully()
    {
        var result = ExecuteAndReturn("CREATE TABLE Products (ProductId INT, ProductName VARCHAR, Price DOUBLE, InStock INT)");

        Assert.False(result.IsError);
        Assert.Empty(result.Data);
    }
}

// --- Multiplexed XUnit Executions ---

[Collection("SequentialStorageTests")]
public class InMemoryCreateTests : CreateTestsBase
{
    public InMemoryCreateTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "CreateDB_Mem") { }
}

[Collection("SequentialStorageTests")]
public class DiskCreateTests : CreateTestsBase
{
    public DiskCreateTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_create" }, "CreateDB_Disk") { }
}
