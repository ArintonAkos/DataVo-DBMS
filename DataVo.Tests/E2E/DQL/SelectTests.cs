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
    public void Select_WithQualifiedStar_ReturnsOnlyThatTableColumns()
    {
        Execute("CREATE TABLE Departments (DeptId INT, DeptName VARCHAR)");
        Execute("CREATE TABLE Employees (EmpId INT, Name VARCHAR, DeptId INT)");

        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (1, 'Engineering')");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (10, 'Alice', 1)");

        var result = ExecuteAndReturn("SELECT Employees.* FROM Employees JOIN Departments ON Employees.DeptId = Departments.DeptId");

        Assert.False(result.IsError);
        Assert.Single(result.Data);
        Assert.True(result.Data[0].ContainsKey("Employees.EmpId") || result.Data[0].ContainsKey("EmpId"));
        Assert.True(result.Data[0].ContainsKey("Employees.Name") || result.Data[0].ContainsKey("Name"));
        Assert.False(result.Data[0].ContainsKey("Departments.DeptName"));
        Assert.False(result.Data[0].ContainsKey("DeptName"));
    }

    [Fact]
    public void Select_WithGlobalStar_OnJoin_ReturnsColumnsFromAllTables()
    {
        Execute("CREATE TABLE Departments (DeptId INT, DeptName VARCHAR)");
        Execute("CREATE TABLE Employees (EmpId INT, Name VARCHAR, DeptId INT)");

        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (1, 'Engineering')");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (10, 'Alice', 1)");

        var result = ExecuteAndReturn("SELECT * FROM Employees JOIN Departments ON Employees.DeptId = Departments.DeptId");

        Assert.False(result.IsError);
        Assert.Single(result.Data);
        Assert.True(result.Data[0].ContainsKey("Departments.DeptName"));
        Assert.True(result.Data[0].ContainsKey("Employees.Name"));
    }

    [Fact]
    public void Select_WithWhereClause_FiltersCorrectly()
    {
        Execute("CREATE TABLE Employees (EmpId INT, Department VARCHAR, Salary FLOAT)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (100, 'HR', 50000.0)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (101, 'Engineering', 120000.0)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (102, 'Engineering', 90000.0)");

        var result = ExecuteAndReturn("SELECT * FROM Employees WHERE Salary >= 90000");

        Assert.False(result.IsError);
        Assert.Equal(2, result.Data.Count);
        Assert.All(result.Data, row => Assert.True((float)row["Salary"] >= 90000f));
    }

    [Fact]
    public void Select_WithWhereClause_NumericComparison_FiltersCorrectly()
    {
        Execute("CREATE TABLE Scores (Id INT, Name VARCHAR, Points INT)");
        Execute("INSERT INTO Scores (Id, Name, Points) VALUES (1, 'Alice', 42)");
        Execute("INSERT INTO Scores (Id, Name, Points) VALUES (2, 'Bob', 70)");
        Execute("INSERT INTO Scores (Id, Name, Points) VALUES (3, 'Charlie', 55)");

        var result = ExecuteAndReturn("SELECT * FROM Scores WHERE Points >= 55");

        Assert.False(result.IsError);
        Assert.Equal(2, result.Data.Count);
        Assert.All(result.Data, row => Assert.True((int)row["Points"] >= 55));
    }

    [Fact]
    public void Select_WithJoin_ReturnsMatchedRows()
    {
        Execute("CREATE TABLE Departments (DeptId INT, DeptName VARCHAR)");
        Execute("CREATE TABLE Employees (EmpId INT, Name VARCHAR, DeptId INT)");

        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (1, 'Engineering')");
        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (2, 'HR')");

        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (10, 'Alice', 1)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (11, 'Bob', 1)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (12, 'Chris', 2)");

        var result = ExecuteAndReturn("SELECT Employees.Name, Departments.DeptName FROM Employees JOIN Departments ON Employees.DeptId = Departments.DeptId");

        Assert.False(result.IsError);
        Assert.Equal(3, result.Data.Count);
    }

    [Fact]
    public void Select_WithJoin_AmbiguousUnqualifiedOnColumn_Throws()
    {
        Execute("CREATE TABLE Departments (DeptId INT, DeptName VARCHAR)");
        Execute("CREATE TABLE Employees (EmpId INT, Name VARCHAR, DeptId INT)");

        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (1, 'Engineering')");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (10, 'Alice', 1)");

        var ex = Assert.Throws<Exception>(() =>
            ExecuteAndReturn("SELECT Employees.Name FROM Employees JOIN Departments ON DeptId = DeptId"));

        Assert.Contains("Binding Error", ex.Message);
    }

    [Fact]
    public void Select_WithJoin_UnknownAliasInOnColumn_Throws()
    {
        Execute("CREATE TABLE Departments (DeptId INT, DeptName VARCHAR)");
        Execute("CREATE TABLE Employees (EmpId INT, Name VARCHAR, DeptId INT)");

        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (1, 'Engineering')");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (10, 'Alice', 1)");

        var ex = Assert.Throws<Exception>(() =>
            ExecuteAndReturn("SELECT Employees.Name FROM Employees JOIN Departments ON UnknownAlias.DeptId = Departments.DeptId"));

        Assert.Contains("Binding Error", ex.Message);
    }

    [Fact]
    public void Select_WithGroupBy_ReturnsGroupedRows()
    {
        Execute("CREATE TABLE Employees (EmpId INT, Department VARCHAR, Salary FLOAT)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (100, 'HR', 50000.0)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (101, 'Engineering', 120000.0)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (102, 'Engineering', 90000.0)");

        var result = ExecuteAndReturn("SELECT Department FROM Employees GROUP BY Department");

        Assert.False(result.IsError);
        Assert.Equal(2, result.Data.Count);
    }

    [Fact]
    public void Select_WithGroupByAndHaving_FiltersGroups()
    {
        Execute("CREATE TABLE Employees (EmpId INT, Department VARCHAR, Salary FLOAT)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (100, 'HR', 50000.0)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (101, 'Engineering', 120000.0)");
        Execute("INSERT INTO Employees (EmpId, Department, Salary) VALUES (102, 'Engineering', 90000.0)");

        var result = ExecuteAndReturn("SELECT Department FROM Employees GROUP BY Department HAVING Department = 'Engineering'");

        Assert.False(result.IsError);
        Assert.Single(result.Data);
    }

    [Fact]
    public void Select_WithOrderBy_SortsRows()
    {
        Execute("CREATE TABLE Scores (Id INT, Name VARCHAR, Points INT)");
        Execute("INSERT INTO Scores (Id, Name, Points) VALUES (1, 'Alice', 42)");
        Execute("INSERT INTO Scores (Id, Name, Points) VALUES (2, 'Bob', 70)");
        Execute("INSERT INTO Scores (Id, Name, Points) VALUES (3, 'Charlie', 55)");

        var result = ExecuteAndReturn("SELECT Name, Points FROM Scores ORDER BY Points DESC");

        Assert.False(result.IsError);
        Assert.Equal("Bob", result.Data[0]["Name"]);
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
