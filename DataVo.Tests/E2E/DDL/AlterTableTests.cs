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

    [Fact]
    public void AlterTable_AddColumn_DateType_BackfillsNullAndAcceptsNewValues()
    {
        var alter = ExecuteAndReturn("ALTER TABLE Users ADD COLUMN Born DATE");
        Assert.False(alter.IsError);

        Execute("INSERT INTO Users (Id, Name, Born) VALUES (3, 'Cara', '2026-06-22')");

        var result = ExecuteAndReturn("SELECT Born FROM Users ORDER BY Id");
        Assert.False(result.IsError);
        Assert.Equal(3, result.Data.Count);
        Assert.Null(result.Data[0]["Born"]);
        Assert.Null(result.Data[1]["Born"]);
        Assert.Equal(new DateOnly(2026, 6, 22), result.Data[2]["Born"]);
    }

    [Fact]
    public void AlterTable_AddColumn_VectorType_BackfillsNullAndPreservesPrimaryKeyLookups()
    {
        var alter = ExecuteAndReturn("ALTER TABLE Users ADD COLUMN Embedding VECTOR(3)");
        Assert.False(alter.IsError);

        Execute("INSERT INTO Users (Id, Name, Embedding) VALUES (3, 'Cara', '[1,2,3]')");

        var result = ExecuteAndReturn("SELECT Embedding FROM Users ORDER BY Id");
        Assert.False(result.IsError);
        Assert.Equal(3, result.Data.Count);
        Assert.Null(result.Data[0]["Embedding"]);
        Assert.IsType<float[]>(result.Data[2]["Embedding"]!);
        Assert.Equal([1f, 2f, 3f], (float[])result.Data[2]["Embedding"]!);

        // The pre-existing primary-key index must survive the rewrite + reindex.
        var pkLookup = ExecuteAndReturn("SELECT Name FROM Users WHERE Id = 2");
        Assert.False(pkLookup.IsError);
        Assert.Single(pkLookup.Data);
        Assert.Equal("Bob", pkLookup.Data[0]["Name"]);
    }

    [Fact]
    public void AlterTable_ModifyColumn_VarcharToDate_ConvertsExistingValues()
    {
        Execute("ALTER TABLE Users ADD COLUMN Born VARCHAR(20) DEFAULT '2026-06-22'");
        var alter = ExecuteAndReturn("ALTER TABLE Users MODIFY COLUMN Born DATE");
        Assert.False(alter.IsError);

        var result = ExecuteAndReturn("SELECT Born FROM Users ORDER BY Id");
        Assert.False(result.IsError);
        Assert.All(result.Data, row => Assert.Equal(new DateOnly(2026, 6, 22), row["Born"]));
    }

    [Fact]
    public void AlterTable_ModifyColumn_VarcharToVector_ConvertsExistingValues()
    {
        Execute("ALTER TABLE Users ADD COLUMN Embedding VARCHAR(20) DEFAULT '[1,2,3]'");
        var alter = ExecuteAndReturn("ALTER TABLE Users MODIFY COLUMN Embedding VECTOR(3)");
        Assert.False(alter.IsError);

        var result = ExecuteAndReturn("SELECT Embedding FROM Users ORDER BY Id");
        Assert.False(result.IsError);
        Assert.All(result.Data, row =>
        {
            Assert.IsType<float[]>(row["Embedding"]!);
            Assert.Equal([1f, 2f, 3f], (float[])row["Embedding"]!);
        });
    }

    [Fact]
    public void AlterTable_DropColumn_DateType_RemovesColumnAndPreservesPrimaryKeyLookups()
    {
        Execute("ALTER TABLE Users ADD COLUMN Born DATE");

        var drop = ExecuteAndReturn("ALTER TABLE Users DROP COLUMN Born");
        Assert.False(drop.IsError);

        var missing = ExecuteAndReturn("SELECT Born FROM Users");
        Assert.True(missing.IsError);

        var pkLookup = ExecuteAndReturn("SELECT Name FROM Users WHERE Id = 1");
        Assert.False(pkLookup.IsError);
        Assert.Single(pkLookup.Data);
        Assert.Equal("Alice", pkLookup.Data[0]["Name"]);
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