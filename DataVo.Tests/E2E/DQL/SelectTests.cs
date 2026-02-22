using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

public abstract class SelectTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void Select_NoWhereClause_ReturnsAllRows()
    {
        Execute("CREATE TABLE Users (Id INT, Name VARCHAR, Age INT)");
        Execute("INSERT INTO Users (Id, Name, Age) VALUES (1, 'Alice', 30)");
        Execute("INSERT INTO Users (Id, Name, Age) VALUES (2, 'Bob', 25)");
        Execute("INSERT INTO Users (Id, Name, Age) VALUES (3, 'Charlie', 35)");

        var result = ExecuteAndReturn("SELECT * FROM Users");

        Assert.False(result.IsError);
        Assert.NotNull(result.Data);
        Assert.Equal(3, result.Data.Count);
    }

    [Fact]
    public void Select_WithWhereClause_FiltersCorrectly()
    {
        Execute("CREATE TABLE Employees (EmpId INT, Department VARCHAR, Salary FLOAT)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (100, 'HR', 50000.0)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (101, 'Engineering', 120000.0)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (102, 'Engineering', 90000.0)");

        var result = ExecuteAndReturn("SELECT * FROM Employees WHERE Department = 'Engineering'");

        Assert.False(result.IsError);
        Assert.Equal(2, result.Data.Count);
        Assert.All(result.Data, row => Assert.Equal("Engineering", row["Department"]));
    }
}

// --- Multiplexed XUnit Executions ---

[Collection("SequentialStorageTests")]
public class InMemorySelectTests : SelectTestsBase
{
    public InMemorySelectTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "SelectDB_Mem") { }
}

[Collection("SequentialStorageTests")]
public class DiskSelectTests : SelectTestsBase
{
    public DiskSelectTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_select" }, "SelectDB_Disk") { }
}
