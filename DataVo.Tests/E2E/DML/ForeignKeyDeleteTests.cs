using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;

public abstract class ForeignKeyDeleteTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void Delete_Parent_WithChildren_Restrict_IsRejected()
    {
        Execute("CREATE TABLE Departments (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("CREATE TABLE Employees (Id INT PRIMARY KEY, DeptId INT REFERENCES Departments(Id), Name VARCHAR)");

        Execute("INSERT INTO Departments VALUES (1, 'Engineering')");
        Execute("INSERT INTO Departments VALUES (2, 'Sales')");
        Execute("INSERT INTO Employees VALUES (10, 1, 'Alice')");
        Execute("INSERT INTO Employees VALUES (20, 1, 'Bob')");

        // Delete department 1 should be REJECTED — 2 employees reference it
        var result = ExecuteAndReturn("DELETE FROM Departments WHERE Id = 1");
        Assert.Contains(result.Messages, m => m.Contains("Foreign key violation"));

        // Department 1 should still exist
        var depts = ExecuteAndReturn("SELECT * FROM Departments");
        Assert.Equal(2, depts.Data.Count);
    }

    [Fact]
    public void Delete_Parent_WithChildren_Cascade_DeletesChildren()
    {
        Execute("CREATE TABLE Teams (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("CREATE TABLE Members (Id INT PRIMARY KEY, TeamId INT REFERENCES Teams(Id) ON DELETE CASCADE, Name VARCHAR)");

        Execute("INSERT INTO Teams VALUES (1, 'Alpha')");
        Execute("INSERT INTO Teams VALUES (2, 'Beta')");
        Execute("INSERT INTO Members VALUES (10, 1, 'Alice')");
        Execute("INSERT INTO Members VALUES (20, 1, 'Bob')");
        Execute("INSERT INTO Members VALUES (30, 2, 'Charlie')");

        // Delete team 1 — should CASCADE and delete Alice + Bob
        Execute("DELETE FROM Teams WHERE Id = 1");

        // Only team 2 remains
        var teams = ExecuteAndReturn("SELECT * FROM Teams");
        Assert.Single(teams.Data);
        Assert.Equal("Beta", teams.Data.First()["Name"]);

        // Only Charlie remains
        var members = ExecuteAndReturn("SELECT * FROM Members");
        Assert.Single(members.Data);
        Assert.Equal("Charlie", members.Data.First()["Name"]);
    }

    [Fact]
    public void Delete_Parent_NoChildren_Succeeds()
    {
        Execute("CREATE TABLE Categories (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("CREATE TABLE Products (Id INT PRIMARY KEY, CatId INT REFERENCES Categories(Id), Name VARCHAR)");

        Execute("INSERT INTO Categories VALUES (1, 'Electronics')");
        Execute("INSERT INTO Categories VALUES (2, 'Books')");
        Execute("INSERT INTO Products VALUES (10, 1, 'Laptop')");

        // Delete category 2 — no products reference it, should succeed
        Execute("DELETE FROM Categories WHERE Id = 2");

        var cats = ExecuteAndReturn("SELECT * FROM Categories");
        Assert.Single(cats.Data);
        Assert.Equal("Electronics", cats.Data.First()["Name"]);
    }

    [Fact]
    public void Delete_Cascade_MultiLevel()
    {
        Execute("CREATE TABLE Schools (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("CREATE TABLE Classes (Id INT PRIMARY KEY, SchoolId INT REFERENCES Schools(Id) ON DELETE CASCADE, Name VARCHAR)");
        Execute("CREATE TABLE Students (Id INT PRIMARY KEY, ClassId INT REFERENCES Classes(Id) ON DELETE CASCADE, Name VARCHAR)");

        Execute("INSERT INTO Schools VALUES (1, 'Hogwarts')");
        Execute("INSERT INTO Classes VALUES (10, 1, 'Potions')");
        Execute("INSERT INTO Classes VALUES (20, 1, 'Charms')");
        Execute("INSERT INTO Students VALUES (100, 10, 'Harry')");
        Execute("INSERT INTO Students VALUES (200, 20, 'Hermione')");

        // Delete school → should cascade to classes → should cascade to students
        Execute("DELETE FROM Schools WHERE Id = 1");

        var schools = ExecuteAndReturn("SELECT * FROM Schools");
        Assert.Empty(schools.Data);

        var classes = ExecuteAndReturn("SELECT * FROM Classes");
        Assert.Empty(classes.Data);

        var students = ExecuteAndReturn("SELECT * FROM Students");
        Assert.Empty(students.Data);
    }

    [Fact]
    public void Delete_Cascade_PKIndexStillWorks()
    {
        Execute("CREATE TABLE Orgs (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("CREATE TABLE Staff (Id INT PRIMARY KEY, OrgId INT REFERENCES Orgs(Id) ON DELETE CASCADE, Name VARCHAR)");

        Execute("INSERT INTO Orgs VALUES (1, 'Acme')");
        Execute("INSERT INTO Staff VALUES (10, 1, 'Alice')");

        // Cascade delete
        Execute("DELETE FROM Orgs WHERE Id = 1");

        // Re-insert same PK should work (no ghost index entries on parent or child)
        Execute("INSERT INTO Orgs VALUES (1, 'Acme')");
        Execute("INSERT INTO Staff VALUES (10, 1, 'Bob')");
        var staff = ExecuteAndReturn("SELECT * FROM Staff");
        Assert.Single(staff.Data);
        Assert.Equal("Bob", staff.Data.First()["Name"]);
    }
}


// --- Multiplexed XUnit Executions ---

[Collection("SequentialStorageTests")]
public class InMemoryForeignKeyDeleteTests : ForeignKeyDeleteTestsBase
{
    public InMemoryForeignKeyDeleteTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "FKDeleteDB_Mem") { }
}

[Collection("SequentialStorageTests")]
public class DiskForeignKeyDeleteTests : ForeignKeyDeleteTestsBase
{
    public DiskForeignKeyDeleteTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_fkdelete" }, "FKDeleteDB_Disk") { }
}
