using System.ComponentModel;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

public abstract class JoinTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void JoinTests_SimpleJoin_ReturnsCorrectData()
    {
        Execute("CREATE TABLE Departments (DeptId INT, DeptName VARCHAR)");
        Execute("CREATE TABLE Employees (EmpId INT, Name VARCHAR, DeptId INT)");

        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (1, 'Engineering')");
        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (2, 'HR')");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (10, 'Alice', 1)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (11, 'Bob', 1)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (12, 'Charlie', 2)");

        var result = ExecuteAndReturn("SELECT Employees.Name, Departments.DeptName FROM Employees JOIN Departments ON Employees.DeptId = Departments.DeptId");

        Assert.False(result.IsError);
        Assert.NotNull(result.Data);
        Assert.Equal(3, result.Data.Count);
        Assert.Contains(result.Data, row => row["Employees.Name"].ToString() == "Alice" && row["Departments.DeptName"].ToString() == "Engineering");
        Assert.Contains(result.Data, row => row["Employees.Name"].ToString() == "Bob" && row["Departments.DeptName"].ToString() == "Engineering");
        Assert.Contains(result.Data, row => row["Employees.Name"].ToString() == "Charlie" && row["Departments.DeptName"].ToString() == "HR");
    }

    [Fact]
    public void JoinTests_LeftJoin_ReturnsCorrectData()
    {
        Execute("CREATE TABLE Departments (DeptId INT, DeptName VARCHAR)");
        Execute("CREATE TABLE Employees (EmpId INT, Name VARCHAR, DeptId INT)");

        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (1, 'Engineering')");
        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (2, 'HR')");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (10, 'Alice', 1)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (11, 'Bob', 1)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (12, 'Charlie', 2)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (13, 'David', 3)");

        var result = ExecuteAndReturn("SELECT Employees.Name, Departments.DeptName FROM Employees LEFT JOIN Departments ON Employees.DeptId = Departments.DeptId");

        Assert.False(result.IsError);
        Assert.NotNull(result.Data);
        Assert.Equal(4, result.Data.Count);
    }

    [Fact]
    public void JoinTests_RightJoin_ReturnsCorrectData()
    {
        Execute("CREATE TABLE Departments (DeptId INT, DeptName VARCHAR)");
        Execute("CREATE TABLE Employees (EmpId INT, Name VARCHAR, DeptId INT)");

        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (1, 'Engineering')");
        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (2, 'HR')");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (10, 'Alice', 1)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (11, 'Bob', 1)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (12, 'Charlie', 2)");

        var result = ExecuteAndReturn("SELECT Employees.Name, Departments.DeptName FROM Employees RIGHT JOIN Departments ON Employees.DeptId = Departments.DeptId");

        Assert.False(result.IsError);
        Assert.NotNull(result.Data);
        Assert.Equal(2, result.Data.Count);
    }

    [Fact(Skip = "Not implemented yet")]
    public void JoinTests_FullJoin_ReturnsCorrectData()
    {
        Execute("CREATE TABLE Departments (DeptId INT, DeptName VARCHAR)");
        Execute("CREATE TABLE Employees (EmpId INT, Name VARCHAR, DeptId INT)");

        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (1, 'Engineering')");
        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (2, 'HR')");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (10, 'Alice', 1)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (11, 'Bob', 1)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (12, 'Charlie', 2)");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (13, 'David', 3)");

        var result = ExecuteAndReturn("SELECT Employees.Name, Departments.DeptName FROM Employees FULL JOIN Departments ON Employees.DeptId = Departments.DeptId");

        Assert.False(result.IsError);
        Assert.NotNull(result.Data);
        Assert.Equal(4, result.Data.Count);
    }

    [Fact]
    public void JoinTests_CrossJoin_ReturnsCartesianProduct()
    {
        Execute("CREATE TABLE A (Id INT)");
        Execute("CREATE TABLE B (Id INT)");

        Execute("INSERT INTO A (Id) VALUES (1)");
        Execute("INSERT INTO A (Id) VALUES (2)");
        Execute("INSERT INTO B (Id) VALUES (10)");
        Execute("INSERT INTO B (Id) VALUES (20)");

        var result = ExecuteAndReturn("SELECT A.Id, B.Id FROM A CROSS JOIN B");

        Assert.False(result.IsError);
        Assert.NotNull(result.Data);
        Assert.Equal(4, result.Data.Count);
        Assert.Contains(result.Data, row => row["A.Id"].ToString() == "1" && row["B.Id"].ToString() == "10");
        Assert.Contains(result.Data, row => row["A.Id"].ToString() == "1" && row["B.Id"].ToString() == "20");
        Assert.Contains(result.Data, row => row["A.Id"].ToString() == "2" && row["B.Id"].ToString() == "10");
        Assert.Contains(result.Data, row => row["A.Id"].ToString() == "2" && row["B.Id"].ToString() == "20");
    }

    [Fact]
    public void JoinTests_JoinWithAmbiguousColumn_ThrowsError()
    {
        Execute("CREATE TABLE Departments (DeptId INT, DeptName VARCHAR)");
        Execute("CREATE TABLE Employees (EmpId INT, Name VARCHAR, DeptId INT)");

        Execute("INSERT INTO Departments (DeptId, DeptName) VALUES (1, 'Engineering')");
        Execute("INSERT INTO Employees (EmpId, Name, DeptId) VALUES (10, 'Alice', 1)");

        var result = ExecuteAndReturn("SELECT Employees.Name FROM Employees JOIN Departments ON DeptId = DeptId");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("Binding Error"));
    }
}


// --- Multiplexed XUnit Executions ---

[Collection("SequentialStorageTests")]
public class InMemoryJoinTests : JoinTestsBase
{
    public InMemoryJoinTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "JoinDB_Mem") { }
}

[Collection("SequentialStorageTests")]
public class DiskJoinTests : JoinTestsBase
{
    public DiskJoinTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_join" }, "JoinDB_Disk") { }
}
