using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;

public abstract class UpdateTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void Update_SingleRow_Succeeds()
    {
        Execute("CREATE TABLE Employees (Id INT PRIMARY KEY, Name VARCHAR, Salary INT)");
        Execute("INSERT INTO Employees VALUES (1, 'Alice', 50000)");
        Execute("INSERT INTO Employees VALUES (2, 'Bob', 45000)");

        var res = ExecuteAndReturn("UPDATE Employees SET Salary = 55000 WHERE Id = 1");
        Assert.Contains(res.Messages, m => m.Contains("Rows affected: 1"));

        var result = ExecuteAndReturn("SELECT * FROM Employees WHERE Id = 1");
        Assert.Single(result.Data);
        Assert.Equal(55000, result.Data[0]["Salary"]);
    }

    [Fact]
    public void Update_MultiRow_Succeeds()
    {
        Execute("CREATE TABLE Inventory (Id INT PRIMARY KEY, Category VARCHAR, Count INT)");
        Execute("INSERT INTO Inventory VALUES (1, 'Widget', 10)");
        Execute("INSERT INTO Inventory VALUES (2, 'Widget', 5)");
        Execute("INSERT INTO Inventory VALUES (3, 'Gadget', 20)");

        var res = ExecuteAndReturn("UPDATE Inventory SET Count = 0 WHERE Category = 'Widget'");
        Assert.Contains(res.Messages, m => m.Contains("Rows affected: 2"));

        var result = ExecuteAndReturn("SELECT * FROM Inventory");
        Assert.Equal(3, result.Data.Count);
        Assert.Equal(0, result.Data.First(r => (int)r["Id"] == 1)["Count"]);
        Assert.Equal(0, result.Data.First(r => (int)r["Id"] == 2)["Count"]);
        Assert.Equal(20, result.Data.First(r => (int)r["Id"] == 3)["Count"]);
    }

    [Fact]
    public void Update_NoWhereClause_UpdatesAllRows()
    {
        Execute("CREATE TABLE Stats (Id INT PRIMARY KEY, Score INT)");
        Execute("INSERT INTO Stats VALUES (1, 10)");
        Execute("INSERT INTO Stats VALUES (2, 20)");

        var res = ExecuteAndReturn("UPDATE Stats SET Score = 0");
        Assert.Contains(res.Messages, m => m.Contains("Rows affected: 2"));

        var result = ExecuteAndReturn("SELECT * FROM Stats");
        Assert.All(result.Data, row => Assert.Equal(0, row["Score"]));
    }

    [Fact]
    public void Update_IndexedColumn_UpdatesIndexCorrectly()
    {
        Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Email VARCHAR UNIQUE)");
        Execute("INSERT INTO Users VALUES (1, 'old@email.com')");

        // Update the indexed column
        Execute("UPDATE Users SET Email = 'new@email.com' WHERE Id = 1");

        // Old index value should return empty
        var oldSearch = ExecuteAndReturn("SELECT * FROM Users WHERE Email = 'old@email.com'");
        Assert.Empty(oldSearch.Data);

        // New index value should return the row
        var newSearch = ExecuteAndReturn("SELECT * FROM Users WHERE Email = 'new@email.com'");
        Assert.Single(newSearch.Data);
        Assert.Equal(1, newSearch.Data[0]["Id"]);
    }

    [Fact]
    public void Update_PrimaryKey_Duplicate_IsRejected()
    {
        Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Val INT)");
        Execute("INSERT INTO Items VALUES (1, 100)");
        Execute("INSERT INTO Items VALUES (2, 200)");

        // Try to update row 2 to have PK 1
        var res = ExecuteAndReturn("UPDATE Items SET Id = 1 WHERE Id = 2");
        Assert.Contains(res.Messages, m => m.Contains("Constraint violation"));

        // Both rows should remain intact
        var result = ExecuteAndReturn("SELECT * FROM Items");
        Assert.Equal(2, result.Data.Count);
        Assert.Contains(result.Data, r => (int)r["Id"] == 1);
        Assert.Contains(result.Data, r => (int)r["Id"] == 2);
    }

    [Fact]
    public void Update_ForeignKey_InvalidParent_IsRejected()
    {
        Execute("CREATE TABLE Depts (Id INT PRIMARY KEY)");
        Execute("CREATE TABLE Emps (Id INT PRIMARY KEY, DeptId INT REFERENCES Depts(Id))");
        
        Execute("INSERT INTO Depts VALUES (1)");
        Execute("INSERT INTO Emps VALUES (10, 1)");

        // Attempt to update DeptId to a non-existent parent
        var res = ExecuteAndReturn("UPDATE Emps SET DeptId = 99 WHERE Id = 10");
        Assert.Contains(res.Messages, m => m.Contains("Foreign key violation"));
        
        // Emps row should remain unchanged
        var emps = ExecuteAndReturn("SELECT * FROM Emps");
        Assert.Equal(1, emps.Data[0]["DeptId"]);
    }

    [Fact]
    public void Update_PrimaryKey_WithChildren_Restrict_IsRejected()
    {
        Execute("CREATE TABLE Master (Id INT PRIMARY KEY)");
        Execute("CREATE TABLE Detail (Id INT PRIMARY KEY, MasterId INT REFERENCES Master(Id))");

        Execute("INSERT INTO Master VALUES (1)");
        Execute("INSERT INTO Detail VALUES (10, 1)");

        // Attempt to update Master PK. Because there is a child row, RESTRICT should block it.
        var res = ExecuteAndReturn("UPDATE Master SET Id = 2 WHERE Id = 1");
        Assert.Contains(res.Messages, m => m.Contains("Foreign key violation: Cannot update"));

        // Master row should remain 1
        var masters = ExecuteAndReturn("SELECT * FROM Master");
        Assert.Equal(1, masters.Data[0]["Id"]);
    }

    [Fact]
    public void Update_EmptyTable_NoOp()
    {
        Execute("CREATE TABLE Empties (Id INT PRIMARY KEY, Val INT)");
        var res = ExecuteAndReturn("UPDATE Empties SET Val = 1");
        Assert.Contains(res.Messages, m => m.Contains("Rows affected: 0"));
    }

    [Fact]
    public void Update_Benchmark_1KRows()
    {
        Execute("CREATE TABLE Bench (Id INT PRIMARY KEY, Val INT, Str VARCHAR)");
        
        // Insert 1000 rows
        for (int i = 1; i <= 1000; i++)
        {
            Execute($"INSERT INTO Bench VALUES ({i}, {i}, 'Row{i}')");
        }

        var watch = System.Diagnostics.Stopwatch.StartNew();
        
        // Update all 1000 rows, modifying a non-indexed and indexed column
        var res = ExecuteAndReturn("UPDATE Bench SET Val = 9999");
        
        watch.Stop();
        
        Assert.Contains(res.Messages, m => m.Contains("Rows affected: 1000"));
        Assert.True(watch.ElapsedMilliseconds < 5000, "Update of 1k rows took longer than 5 seconds!");
    }
}

// --- Multiplexed XUnit Executions ---

[Collection("SequentialStorageTests")]
public class InMemoryUpdateTests : UpdateTestsBase
{
    public InMemoryUpdateTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "UpdateDB_Mem") { }
}

[Collection("SequentialStorageTests")]
public class DiskUpdateTests : UpdateTestsBase
{
    public DiskUpdateTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_update" }, "UpdateDB_Disk") { }
}
