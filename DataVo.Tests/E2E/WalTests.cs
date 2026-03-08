using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;

namespace DataVo.Tests.E2E;

[Collection("SequentialStorageTests")]
public class DiskWalTests : SqlExecutionTestsBase
{
    public DiskWalTests()
        : base(new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = "./test_datavo_wal",
            WalFilePath = "datavo.wal",
            WalCheckpointThreshold = 1000,
        }, "WalDb_Disk")
    {
    }

    [Fact]
    public void Commit_WritesCheckpointedEntryToWal()
    {
        string table = $"WalCommit_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        Execute("BEGIN;");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute("COMMIT;");

        string walPath = Config.ResolveWalFilePath();
        Assert.True(File.Exists(walPath));

        List<WalEntry> entries = new WalReader(Config).ReadAll();
        Assert.Single(entries);
        Assert.Equal(TestDb, entries[0].DatabaseName);
        Assert.True(entries[0].IsCheckpointed);
        Assert.Contains(entries[0].Operations, operation => operation.OperationType == WalOperationType.Insert && operation.TableName == table);
    }

    [Fact]
    public void Recovery_ReplaysUncheckpointedWalEntries()
    {
        string table = $"WalRecovery_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        var entry = new WalEntry
        {
            TransactionId = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow.Ticks,
            DatabaseName = TestDb,
            IsCheckpointed = false,
            Operations =
            [
                new WalOperation
                {
                    OperationType = WalOperationType.Insert,
                    TableName = table,
                    RowData = new Dictionary<string, object?>
                    {
                        ["Id"] = 1,
                        ["Name"] = "Alice",
                    },
                },
            ],
        };

        new WalWriter(Config).Append(entry);

        var beforeRecovery = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.Empty(beforeRecovery.Data);

        StorageContext.Initialize(Config);
        Execute($"USE {TestDb};");

        var afterRecovery = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.Single(afterRecovery.Data);
        Assert.Equal("Alice", afterRecovery.Data[0]["Name"]?.ToString());
        Assert.False(File.Exists(Config.ResolveWalFilePath()));
    }

    [Fact]
    public void CheckpointThreshold_TruncatesCheckpointedWalEntries()
    {
        var thresholdConfig = new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = Config.DiskStoragePath,
            WalFilePath = Config.WalFilePath,
            WalCheckpointThreshold = 2,
        };

        StorageContext.Initialize(thresholdConfig);
        Execute($"USE {TestDb};");

        string table = $"WalThreshold_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        Execute("BEGIN;");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute("COMMIT;");

        Execute("BEGIN;");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (2, 'Bob');");
        Execute("COMMIT;");

        string walPath = thresholdConfig.ResolveWalFilePath();
        Assert.True(!File.Exists(walPath) || new FileInfo(walPath).Length == 0);
    }
}

[Collection("SequentialStorageTests")]
public class InMemoryWalTests : SqlExecutionTestsBase
{
    public InMemoryWalTests()
        : base(new DataVoConfig
        {
            StorageMode = StorageMode.InMemory,
            WalFilePath = Path.Combine(Path.GetTempPath(), $"datavo-wal-{Guid.NewGuid():N}.wal"),
        }, "WalDb_Memory")
    {
    }

    [Fact]
    public void InMemoryMode_DoesNotCreateWalFile()
    {
        string walPath = Config.ResolveWalFilePath();
        if (File.Exists(walPath))
        {
            File.Delete(walPath);
        }

        string table = $"WalMemory_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        Execute("BEGIN;");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute("COMMIT;");

        Assert.False(File.Exists(walPath));
    }
}
