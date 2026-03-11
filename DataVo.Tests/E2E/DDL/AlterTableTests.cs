using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DDL;

public abstract class AlterTableTestsBase : SqlExecutionTestsBase
{
    protected AlterTableTestsBase(DataVoConfig config, string testDbName) : base(config, testDbName)
    {
        Seed();
    }

    private void Seed()
    {
        Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(50))");
        Execute("INSERT INTO Users VALUES (1, 'Alice')");
        Execute("INSERT INTO Users VALUES (2, 'Bob')");
    }

    [Fact]
    public void AlterTable_AddColumn_BackfillsExistingRowsWithNull()
    {
        var alter = ExecuteAndReturn("ALTER TABLE Users ADD COLUMN Age INT");

        Assert.False(alter.IsError);

        var result = ExecuteAndReturn("SELECT Age FROM Users ORDER BY Id");
        Assert.False(result.IsError);
        Assert.Equal(2, result.Data.Count);
        Assert.All(result.Data, row => Assert.Null(row["Age"]));
    }

    [Fact]
    public void AlterTable_AddColumn_DefaultBackfillsExistingRowsAndNewInserts()
    {
        Execute("ALTER TABLE Users ADD COLUMN Status VARCHAR(20) DEFAULT 'Active'");
        Execute("INSERT INTO Users (Id, Name) VALUES (3, 'Cara')");

        var result = ExecuteAndReturn("SELECT Status FROM Users ORDER BY Id");
        Assert.False(result.IsError);
        Assert.Equal(["Active", "Active", "Active"], result.Data.Select(row => row["Status"]?.ToString()).ToList());
    }

    [Fact]
    public void AlterTable_AddColumn_PreservesExistingIndexes()
    {
        Execute("ALTER TABLE Users ADD COLUMN Status VARCHAR(20) DEFAULT 'Active'");

        var result = ExecuteAndReturn("SELECT Name FROM Users WHERE Id = 2");
        Assert.False(result.IsError);
        Assert.Single(result.Data);
        Assert.Equal("Bob", result.Data[0]["Name"]);
    }

    [Fact]
    public void AlterTable_AddColumn_RejectsUnsupportedConstraints()
    {
        var result = ExecuteAndReturn("ALTER TABLE Users ADD COLUMN Code INT UNIQUE");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("currently supports only nullable/default columns", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void AlterTable_DropColumn_RemovesColumnAndPreservesPrimaryKeyLookups()
    {
        Execute("ALTER TABLE Users DROP COLUMN Name");

        var result = ExecuteAndReturn("SELECT Id FROM Users WHERE Id = 2");
        Assert.False(result.IsError);
        Assert.Single(result.Data);
        Assert.Equal(2, result.Data[0]["Id"]);

        var missingColumn = ExecuteAndReturn("SELECT Name FROM Users");
        Assert.True(missingColumn.IsError);
        Assert.Contains(missingColumn.Messages, m => m.Contains("invalid column name: Name", StringComparison.OrdinalIgnoreCase)
            || m.Contains("cannot resolve column 'Name'", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void AlterTable_DropColumn_RejectsPrimaryKeyColumn()
    {
        var result = ExecuteAndReturn("ALTER TABLE Users DROP COLUMN Id");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("PRIMARY KEY", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void AlterTable_DropColumn_CannotRemoveLastRemainingColumn()
    {
        Execute("ALTER TABLE Users DROP COLUMN Name");

        var result = ExecuteAndReturn("ALTER TABLE Users DROP COLUMN Id");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("last remaining column", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void AlterTable_ModifyColumn_ConvertsExistingValuesAndPreservesDefault()
    {
        Execute("ALTER TABLE Users ADD COLUMN Score INT DEFAULT 7");
        var alter = ExecuteAndReturn("ALTER TABLE Users MODIFY COLUMN Score FLOAT");

        Assert.False(alter.IsError);

        Execute("INSERT INTO Users (Id, Name) VALUES (3, 'Cara')");

        var result = ExecuteAndReturn("SELECT Score FROM Users ORDER BY Id");
        Assert.False(result.IsError);
        Assert.Equal(3, result.Data.Count);
        Assert.All(result.Data, row => Assert.Equal(7f, Convert.ToSingle(row["Score"])));
    }

    [Fact]
    public void AlterTable_ModifyColumn_TruncatesVarcharAndUpdatesDefault()
    {
        var alter = ExecuteAndReturn("ALTER TABLE Users MODIFY COLUMN Name VARCHAR(3) DEFAULT 'Zed'");

        Assert.False(alter.IsError);

        Execute("INSERT INTO Users (Id) VALUES (3)");

        var result = ExecuteAndReturn("SELECT Name FROM Users ORDER BY Id");
        Assert.False(result.IsError);
        Assert.Equal(["Ali", "Bob", "Zed"], result.Data.Select(row => row["Name"]?.ToString()).ToList());
    }

    [Fact]
    public void AlterTable_ModifyColumn_RejectsPrimaryKeyColumn()
    {
        var result = ExecuteAndReturn("ALTER TABLE Users MODIFY COLUMN Id FLOAT");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("PRIMARY KEY", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void AlterTable_ModifyColumn_RejectsIncompatibleExistingValues()
    {
        var result = ExecuteAndReturn("ALTER TABLE Users MODIFY COLUMN Name INT");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("cannot convert existing value", StringComparison.OrdinalIgnoreCase));

        var verify = ExecuteAndReturn("SELECT Name FROM Users ORDER BY Id");
        Assert.False(verify.IsError);
        Assert.Equal(["Alice", "Bob"], verify.Data.Select(row => row["Name"]?.ToString()).ToList());
    }
}

public class AlterTableTestsMemory : AlterTableTestsBase
{
    public AlterTableTestsMemory() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "AlterTableDB_Mem") { }
}

public class AlterTableTestsDisk : AlterTableTestsBase
{
    public AlterTableTestsDisk() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "AlterTableDB_Disk" }, "AlterTableDB_Disk") { }
}