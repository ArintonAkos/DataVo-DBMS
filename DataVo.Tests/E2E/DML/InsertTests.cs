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

    [Fact]
    public void Insert_DuplicatePK_IsRejected()
    {
        Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("INSERT INTO Users VALUES (1, 'Alice')");

        // Second insert with same PK should report violation, not throw
        var result = ExecuteAndReturn("INSERT INTO Users VALUES (1, 'Bob')");
        Assert.Contains(result.Messages, m => m.Contains("Primary key violation"));

        // Only Alice should be in the table
        var selectResult = ExecuteAndReturn("SELECT * FROM Users");
        Assert.Single(selectResult.Data);
        Assert.Equal("Alice", selectResult.Data.First()["Name"]);
    }

    [Fact]
    public void Insert_DuplicateCompositePK_IsRejected()
    {
        Execute("CREATE TABLE Enrollments (StudentId INT PRIMARY KEY, CourseId INT PRIMARY KEY, Grade VARCHAR)");
        Execute("INSERT INTO Enrollments VALUES (1, 101, 'A')");

        // Same composite key should be rejected
        var result = ExecuteAndReturn("INSERT INTO Enrollments VALUES (1, 101, 'B')");
        Assert.Contains(result.Messages, m => m.Contains("Primary key violation"));

        // Different composite key should succeed
        Execute("INSERT INTO Enrollments VALUES (1, 102, 'B')");

        var selectResult = ExecuteAndReturn("SELECT * FROM Enrollments");
        Assert.Equal(2, selectResult.Data.Count);
    }

    [Fact]
    public void Insert_FKConstraint_ValidReference_Succeeds()
    {
        Execute("CREATE TABLE Departments (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("INSERT INTO Departments VALUES (1, 'Engineering')");

        Execute("CREATE TABLE Employees (Id INT PRIMARY KEY, Name VARCHAR, DeptId INT REFERENCES Departments(Id))");
        // FK references existing department — should succeed
        Execute("INSERT INTO Employees VALUES (1, 'Alice', 1)");

        var result = ExecuteAndReturn("SELECT * FROM Employees");
        Assert.Single(result.Data);
    }

    [Fact]
    public void Insert_FKConstraint_InvalidReference_IsRejected()
    {
        Execute("CREATE TABLE Departments (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("INSERT INTO Departments VALUES (1, 'Engineering')");

        Execute("CREATE TABLE Employees (Id INT PRIMARY KEY, Name VARCHAR, DeptId INT REFERENCES Departments(Id))");
        // FK references non-existent department — should be rejected
        var result = ExecuteAndReturn("INSERT INTO Employees VALUES (1, 'Bob', 999)");
        Assert.Contains(result.Messages, m => m.Contains("Foreign key violation"));

        var selectResult = ExecuteAndReturn("SELECT * FROM Employees");
        Assert.Empty(selectResult.Data);
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
