using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;


public abstract class DropTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void DropTests_DropExistingTable_DropsSuccessfully()
    {
        Execute("CREATE TABLE Users (Id INT, Name VARCHAR, Age INT)");
        var result = ExecuteAndReturn("DROP TABLE Users");

        Assert.False(result.IsError);
        Assert.Empty(result.Data);
    }

    [Fact]
    public void DropTests_DropNonExistingTable_ThrowsError()
    {
        var result = ExecuteAndReturn("DROP TABLE NonExistingTable");

        Assert.True(result.IsError);
        Assert.Empty(result.Data);
    }
}


// --- Multiplexed XUnit Executions ---

[Collection("SequentialStorageTests")]
public class InMemoryDropTests : DropTestsBase
{
    public InMemoryDropTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "DropDB_Mem") { }
}

[Collection("SequentialStorageTests")]
public class DiskDropTests : DropTestsBase
{
    public DiskDropTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_drop" }, "DropDB_Disk") { }
}
