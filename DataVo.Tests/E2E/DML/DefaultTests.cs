using DataVo.Core.StorageEngine.Config;
using Xunit;

namespace DataVo.Tests.E2E.DML;

public abstract class DefaultTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void CreateTable_WithDefaultValues_ParsesSuccessfully()
    {
        // Act
        Execute(@"
            CREATE TABLE ConfigOptions (
                Id INT PRIMARY KEY,
                Status VARCHAR(50) DEFAULT 'Pending',
                RetryCount INT DEFAULT 5,
                IsActive BIT DEFAULT true
            )");

        // Assert - Insert something empty-ish and check what pops out
        Execute("INSERT INTO ConfigOptions (Id) VALUES (1)");

        var result = ExecuteAndReturn("SELECT * FROM ConfigOptions");
        Assert.Single(result.Data);

        var row = result.Data.First();
        Assert.Equal(1, row["Id"]);
        Assert.Equal("Pending", row["Status"]);
        Assert.Equal(5, row["RetryCount"]);
        Assert.True((bool)row["IsActive"]);
    }

    [Fact]
    public void InsertInto_OmittingDefaultColumn_AppliesDefaultValue()
    {
        // Arrange
        Execute(@"
            CREATE TABLE DefaultsTest1 (
                Id INT PRIMARY KEY,
                Name VARCHAR(100),
                State VARCHAR(20) DEFAULT 'New'
            )");

        // Act - omit 'State' entirely
        Execute("INSERT INTO DefaultsTest1 (Id, Name) VALUES (1, 'Task A')");

        // Assert
        var result = ExecuteAndReturn("SELECT * FROM DefaultsTest1");
        Assert.Single(result.Data);

        var row = result.Data.First();
        Assert.Equal(1, row["Id"]);
        Assert.Equal("Task A", row["Name"]);
        Assert.Equal("New", row["State"]); // Default should be injected
    }

    [Fact]
    public void InsertInto_ProvidingValue_OverridesDefaultValue()
    {
        // Arrange
        Execute(@"
            CREATE TABLE DefaultsTest2 (
                Id INT PRIMARY KEY,
                Score INT DEFAULT 0
            )");

        // Act - provide explicit score
        Execute("INSERT INTO DefaultsTest2 (Id, Score) VALUES (1, 100)");

        // Assert
        var result = ExecuteAndReturn("SELECT * FROM DefaultsTest2");
        Assert.Single(result.Data);

        var row = result.Data.First();
        Assert.Equal(1, row["Id"]);
        Assert.Equal(100, row["Score"]); // Default overridden
    }

    [Fact]
    public void InsertInto_ProvidingNull_OverridesDefaultWithNull()
    {
        // Arrange
        Execute(@"
            CREATE TABLE DefaultsTest3 (
                Id INT PRIMARY KEY,
                Metadata VARCHAR(255) DEFAULT 'default_meta'
            )");

        // Act - explicitly write NULL
        Execute("INSERT INTO DefaultsTest3 (Id, Metadata) VALUES (1, NULL)");

        // Assert
        var result = ExecuteAndReturn("SELECT * FROM DefaultsTest3");
        Assert.Single(result.Data);

        var row = result.Data.First();
        Assert.Equal(1, row["Id"]);
        Assert.Null(row["Metadata"]); // User explicit NULL overrides default
    }

    [Fact]
    public void InsertInto_NoColumnsSpecified_DoesNotApplyDefaultsIfValueProvided()
    {
        // Arrange
        Execute(@"
            CREATE TABLE DefaultsTest4 (
                Id INT PRIMARY KEY,
                Category VARCHAR(50) DEFAULT 'General'
            )");

        // Act - insert without explicit columns, provide both values
        Execute("INSERT INTO DefaultsTest4 VALUES (1, 'Specific')");

        // Assert
        var result = ExecuteAndReturn("SELECT * FROM DefaultsTest4");
        Assert.Single(result.Data);

        var row = result.Data.First();
        Assert.Equal(1, row["Id"]);
        Assert.Equal("Specific", row["Category"]);
    }
}

public class DefaultTestsMemory() : DefaultTestsBase(new DataVoConfig { StorageMode = StorageMode.InMemory }, "DefaultDb_Mem");

public class DefaultTestsDisk() : DefaultTestsBase(new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DiskStoragePath = Path.Combine(Path.GetTempPath(), "DataVo_Tests_Default")
}, "DefaultDb_Disk");
