using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;

public abstract class InsertTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void Insert_SingleRow_PersistsCorrectly()
    {
        Execute("CREATE TABLE Products (Id INT, Name VARCHAR, Price FLOAT)");
        Execute("INSERT INTO Products (Id, Name, Price) VALUES (1, 'Laptop', 999.99)");

        var result = ExecuteAndReturn("SELECT * FROM Products");

        Assert.False(result.IsError);
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);

        var row = result.Data.First();
        Assert.Equal(1, row["Id"]);
        Assert.Equal("Laptop", row["Name"]);
        Assert.Equal(999.99f, row["Price"]);
    }

    [Fact]
    public void Insert_MultipleRows_MaintainsSequence()
    {
        Execute("CREATE TABLE Products (Id INT, Name VARCHAR, Price FLOAT)");
        Execute("INSERT INTO Products (Id, Name, Price) VALUES (10, 'Mouse', 25.0)");
        Execute("INSERT INTO Products (Id, Name, Price) VALUES (20, 'Keyboard', 75.5)");

        var result = ExecuteAndReturn("SELECT * FROM Products");

        Assert.Equal(2, result.Data.Count);
    }
}

// --- Multiplexed XUnit Executions ---

[Collection("SequentialStorageTests")]
public class InMemoryInsertTests : InsertTestsBase
{
    public InMemoryInsertTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "InsertDB_Mem") { }
}

[Collection("SequentialStorageTests")]
public class DiskInsertTests : InsertTestsBase
{
    public DiskInsertTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_insert" }, "InsertDB_Disk") { }
}
