using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

public abstract class SubqueryTestsBase : SqlExecutionTestsBase
{
    protected SubqueryTestsBase(DataVoConfig config, string testDbName) : base(config, testDbName)
    {
        Seed();
    }

    private void Seed()
    {
        Execute("CREATE TABLE Employees (Id INT PRIMARY KEY, Name VARCHAR(50), DeptId INT)");
        Execute("CREATE TABLE ActiveDepartments (DeptId INT PRIMARY KEY)");

        Execute("INSERT INTO Employees VALUES (1, 'Alice', 10)");
        Execute("INSERT INTO Employees VALUES (2, 'Bob', 20)");
        Execute("INSERT INTO Employees VALUES (3, 'Cara', 30)");

        Execute("INSERT INTO ActiveDepartments VALUES (10)");
        Execute("INSERT INTO ActiveDepartments VALUES (30)");
    }

    [Fact]
    public void Select_InSubquery_FiltersRows()
    {
        var result = ExecuteAndReturn("SELECT Name FROM Employees WHERE DeptId IN (SELECT DeptId FROM ActiveDepartments)");

        Assert.False(result.IsError);
        var names = result.Data.Select(row => row["Name"]?.ToString()).OrderBy(name => name).ToList();
        Assert.Equal(["Alice", "Cara"], names);
    }

    [Fact]
    public void Update_InSubquery_FiltersRows()
    {
        Execute("UPDATE Employees SET Name = 'Selected' WHERE DeptId IN (SELECT DeptId FROM ActiveDepartments)");

        var result = ExecuteAndReturn("SELECT Name FROM Employees WHERE DeptId IN (SELECT DeptId FROM ActiveDepartments)");
        Assert.False(result.IsError);
        Assert.All(result.Data, row => Assert.Equal("Selected", row["Name"]));
    }

    [Fact]
    public void Delete_InSubquery_FiltersRows()
    {
        Execute("DELETE FROM Employees WHERE DeptId IN (SELECT DeptId FROM ActiveDepartments)");

        var result = ExecuteAndReturn("SELECT Id FROM Employees");
        Assert.False(result.IsError);
        Assert.Single(result.Data);
        Assert.Equal(2, result.Data[0]["Id"]);
    }

    [Fact]
    public void InSubquery_MustReturnSingleColumn()
    {
        var result = ExecuteAndReturn("SELECT Name FROM Employees WHERE DeptId IN (SELECT Id, Name FROM Employees)");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("exactly one column", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void CorrelatedInSubquery_IsRejectedExplicitly()
    {
        var result = ExecuteAndReturn("SELECT e.Name FROM Employees e WHERE e.DeptId IN (SELECT a.DeptId FROM ActiveDepartments a WHERE a.DeptId = e.DeptId)");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("Correlated subqueries are not supported yet", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Select_ExistsSubquery_FiltersRows()
    {
        var result = ExecuteAndReturn("SELECT Name FROM Employees WHERE EXISTS (SELECT DeptId FROM ActiveDepartments WHERE DeptId = 10)");

        Assert.False(result.IsError);
        Assert.Equal(3, result.Data.Count);
    }

    [Fact]
    public void Select_NotExistsSubquery_FiltersRows()
    {
        var result = ExecuteAndReturn("SELECT Name FROM Employees WHERE NOT EXISTS (SELECT DeptId FROM ActiveDepartments WHERE DeptId = 999)");

        Assert.False(result.IsError);
        Assert.Equal(3, result.Data.Count);
    }

    [Fact]
    public void Delete_ExistsSubquery_FiltersRows()
    {
        Execute("DELETE FROM Employees WHERE EXISTS (SELECT DeptId FROM ActiveDepartments WHERE DeptId = 10)");

        var result = ExecuteAndReturn("SELECT Id FROM Employees");
        Assert.False(result.IsError);
        Assert.Empty(result.Data);
    }

    [Fact]
    public void Update_NotExistsSubquery_CanPreventMutation()
    {
        Execute("UPDATE Employees SET Name = 'Updated' WHERE NOT EXISTS (SELECT DeptId FROM ActiveDepartments WHERE DeptId = 10)");

        var result = ExecuteAndReturn("SELECT Name FROM Employees ORDER BY Id");
        Assert.False(result.IsError);
        Assert.Equal(["Alice", "Bob", "Cara"], result.Data.Select(row => row["Name"]?.ToString()).ToList());
    }

    [Fact]
    public void CorrelatedExistsSubquery_IsRejectedExplicitly()
    {
        var result = ExecuteAndReturn("SELECT e.Name FROM Employees e WHERE EXISTS (SELECT a.DeptId FROM ActiveDepartments a WHERE a.DeptId = e.DeptId)");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("Correlated subqueries are not supported yet", StringComparison.OrdinalIgnoreCase));
    }
}

public class SubqueryTestsMemory : SubqueryTestsBase
{
    public SubqueryTestsMemory() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "SubqueryDB_Mem") { }
}

public class SubqueryTestsDisk : SubqueryTestsBase
{
    public SubqueryTestsDisk() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "SubqueryDB_Disk" }, "SubqueryDB_Disk") { }
}