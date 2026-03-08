using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;


public abstract class VacuumTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void Vacuum_RemovesTombstones_DataIntact()
    {
        Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("INSERT INTO Items VALUES (1, 'Alpha')");
        Execute("INSERT INTO Items VALUES (2, 'Beta')");
        Execute("INSERT INTO Items VALUES (3, 'Gamma')");

        // Delete the middle row
        Execute("DELETE FROM Items WHERE Id = 2");

        // Verify 2 rows remain
        var before = ExecuteAndReturn("SELECT * FROM Items");
        Assert.Equal(2, before.Data.Count);

        // VACUUM
        Execute("VACUUM Items");

        // Same 2 rows should remain
        var after = ExecuteAndReturn("SELECT * FROM Items");
        Assert.Equal(2, after.Data.Count);
        Assert.Contains(after.Data, r => r["Name"].ToString() == "Alpha");
        Assert.Contains(after.Data, r => r["Name"].ToString() == "Gamma");
    }

    [Fact]
    public void Vacuum_IndexesStillWork_PKDuplicateCheck()
    {
        Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("INSERT INTO Users VALUES (1, 'Alice')");
        Execute("INSERT INTO Users VALUES (2, 'Bob')");

        // Delete Alice
        Execute("DELETE FROM Users WHERE Id = 1");

        // VACUUM
        Execute("VACUUM Users");

        // Bob should still exist
        var result = ExecuteAndReturn("SELECT * FROM Users");
        Assert.Single(result.Data);
        Assert.Equal("Bob", result.Data.First()["Name"]);

        // Re-inserting Id=1 should work (index was rebuilt)
        Execute("INSERT INTO Users VALUES (1, 'Charlie')");

        var afterInsert = ExecuteAndReturn("SELECT * FROM Users");
        Assert.Equal(2, afterInsert.Data.Count);

        // But Id=2 (Bob) should be rejected as duplicate
        var dup = ExecuteAndReturn("INSERT INTO Users VALUES (2, 'Duplicate')");
        Assert.Contains(dup.Messages, m => m.Contains("Primary key violation"));
    }

    [Fact]
    public void Vacuum_EmptyTable_NoError()
    {
        Execute("CREATE TABLE Empty (Id INT PRIMARY KEY, Name VARCHAR)");

        // VACUUM on empty table should succeed silently
        var result = ExecuteAndReturn("VACUUM Empty");
        Assert.False(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("0 rows compacted"));
    }
}


// --- Multiplexed XUnit Executions ---

public class InMemoryVacuumTests : VacuumTestsBase
{
    public InMemoryVacuumTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "VacuumDB_Mem") { }
}

public class DiskVacuumTests : VacuumTestsBase
{
    public DiskVacuumTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_vacuum" }, "VacuumDB_Disk") { }
}
