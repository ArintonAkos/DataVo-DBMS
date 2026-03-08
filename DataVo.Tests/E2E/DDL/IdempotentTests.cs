using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DDL;

public abstract class IdempotentTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void CreateTable_IfNotExists_BypassesExistingProperly()
    {
        string uniqueTable = $"Users_{Guid.NewGuid():N}";

        // 1. First creation should succeed
        var firstResult = ExecuteAndReturn($"CREATE TABLE {uniqueTable} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Assert.NotNull(firstResult);
        Assert.Contains("successfully created", firstResult.Messages.FirstOrDefault() ?? "");

        // 2. Second creation with IF NOT EXISTS should bypass without throwing
        var bypassResult = ExecuteAndReturn($"CREATE TABLE IF NOT EXISTS {uniqueTable} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Assert.NotNull(bypassResult);
        Assert.Contains("already exists. Skipping creation", bypassResult.Messages.FirstOrDefault() ?? "");
    }

    [Fact]
    public void DropTable_IfExists_BypassesMissingProperly()
    {
        string missingTable = $"Missing_{Guid.NewGuid():N}";

        // 1. Dropping a non-existent table with IF EXISTS should bypass without throwing
        var bypassResult = ExecuteAndReturn($"DROP TABLE IF EXISTS {missingTable};");
        Assert.NotNull(bypassResult);
        Assert.Contains("does not exist. Skipping drop", bypassResult.Messages.FirstOrDefault() ?? "");
    }

    [Fact]
    public void CreateDatabase_IfNotExists_BypassesExistingProperly()
    {
        string uniqueDb = $"SecondDb_{Guid.NewGuid():N}";
        
        // 1. Create a secondary database
        var firstResult = ExecuteAndReturn($"CREATE DATABASE {uniqueDb};");
        Assert.NotNull(firstResult);
        Assert.Contains("successfully created", firstResult.Messages.FirstOrDefault() ?? "");

        // 2. Second creation should bypass 
        var bypassResult = ExecuteAndReturn($"CREATE DATABASE IF NOT EXISTS {uniqueDb};");
        Assert.NotNull(bypassResult);
        Assert.Contains("already exists. Skipping creation", bypassResult.Messages.FirstOrDefault() ?? "");
        
        // Cleanup explicitly
        ExecuteAndReturn($"DROP DATABASE IF EXISTS {uniqueDb};");
    }

    [Fact]
    public void DropDatabase_IfExists_BypassesMissingProperly()
    {
        string missingDb = $"MissingDb_{Guid.NewGuid():N}";

        // 1. Dropping a non-existent database
        var bypassResult = ExecuteAndReturn($"DROP DATABASE IF EXISTS {missingDb};");
        Assert.NotNull(bypassResult);
        Assert.Contains("does not exist. Skipping drop", bypassResult.Messages.FirstOrDefault() ?? "");
    }
}

public class IdempotentTestsMemory : IdempotentTestsBase
{
    public IdempotentTestsMemory() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "IdempotentMemDb") { }
}

public class IdempotentTestsDisk : IdempotentTestsBase
{
    public IdempotentTestsDisk() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "IdempotentDiskDb" }, "IdempotentDiskDb") { }
}
