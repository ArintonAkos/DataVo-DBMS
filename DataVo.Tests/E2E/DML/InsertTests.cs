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

    [Fact]
    public void Insert_MultipleRows_SameStatement_PersistsAll()
    {
        Execute("CREATE TABLE Products (Id INT, Name VARCHAR, Price FLOAT)");
        Execute("INSERT INTO Products (Id, Name, Price) VALUES (100, 'Monitor', 199.99), (200, 'Printer', 149.49), (300, 'Scanner', 89.99)");

        var result = ExecuteAndReturn("SELECT * FROM Products");

        Assert.Equal(3, result.Data.Count);
    }

    [Fact]
    public void Insert_MultipleRows_SameStatement_MultiLine_PersistsAll()
    {
        Execute("CREATE TABLE Products (Id INT, Name VARCHAR, Price FLOAT)");
        Execute(@"INSERT INTO Products (Id, Name, Price) VALUES 
                    (1000, 'Desk Lamp', 45.99), 
                    (2000, 'Office Chair', 129.99), 
                    (3000, 'Bookshelf', 89.99)");

        var result = ExecuteAndReturn("SELECT * FROM Products");

        Assert.Equal(3, result.Data.Count);
    }

    [Fact]
    public void Insert_WithoutSpecifyingColumns_AssumesAll()
    {
        Execute("CREATE TABLE Products (Id INT, Name VARCHAR, Price FLOAT)");
        Execute("INSERT INTO Products VALUES (500, 'Desk', 299.99)");
    }

    [Fact]
    public void Insert_MismatchedColumnsAndValues_Throws()
    {
        Execute("CREATE TABLE Products (Id INT, Name VARCHAR, Price FLOAT)");
        var ex = Assert.Throws<Exception>(() => Execute("INSERT INTO Products (Id, Name) VALUES (1, 'Chair', 89.99)"));
        Assert.Contains("The number of values provided in a row must be the same as the number of columns", ex.Message);
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
