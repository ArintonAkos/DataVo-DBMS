using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;
using DataVo.Tests.BrowserParity;

namespace DataVo.Tests.E2E;

/// <summary>
/// Audit evidence (not a regression guard): pins down the OBSERVED durability behaviour of the
/// autocommit write path in Disk + WAL mode, so the WAL-integration audit rests on facts rather
/// than code-reading inference. These tests document what the engine does today; if the durability
/// contract is later changed, expect to update them.
/// </summary>
public class DiskAutocommitDurabilityAuditTests : SqlExecutionTestsBase
{
    public DiskAutocommitDurabilityAuditTests()
        : base(new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = "./test_datavo_autocommit_audit",
            WalFilePath = "datavo.wal",
            WalCheckpointThreshold = 1000,
        }, "AutocommitAuditDb")
    {
    }

    [Fact]
    [BrowserTranslateIgnore("Disk WAL file inspection is disk-specific and outside browser SQL parity")]
    public void AutocommitInsert_DoesNotWriteToWal_ButRowIsImmediatelyReadable()
    {
        string table = $"AcInsert_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        // Clean slate: drop any WAL the harness/recovery may have touched.
        string walPath = Config.ResolveWalFilePath();
        if (File.Exists(walPath))
        {
            File.Delete(walPath);
        }

        // Autocommit: a bare INSERT with NO BEGIN/COMMIT around it.
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");

        // The row is durably written to the .dat file and is immediately readable...
        var selected = ExecuteAndReturn($"SELECT * FROM {table} WHERE Id = 1;");
        Assert.Single(selected.Data);
        Assert.Equal("Alice", selected.Data[0]["Name"]?.ToString());

        // ...but the autocommit path NEVER appended to the WAL. The WAL only captures explicit-
        // transaction commits, so it is absent or empty after a pure autocommit write.
        List<WalEntry> walEntries = File.Exists(walPath) ? new WalReader(Config).ReadAll() : [];
        Assert.Empty(walEntries);
    }

    [Fact]
    [BrowserTranslateIgnore("Disk persistence across engine restart depends on disk internals not modelled in browser parity")]
    public void AutocommitInsert_SurvivesEngineRestart_WithoutWalReplay()
    {
        string table = $"AcSurvive_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        string walPath = Config.ResolveWalFilePath();
        if (File.Exists(walPath))
        {
            File.Delete(walPath);
        }

        Execute($"INSERT INTO {table} (Id, Name) VALUES (7, 'Bob');");

        // No WAL entry exists for the autocommit write.
        List<WalEntry> beforeRestart = File.Exists(walPath) ? new WalReader(Config).ReadAll() : [];
        Assert.Empty(beforeRestart);

        // Restart the engine against the same disk path. Recovery has no WAL to replay, yet the row
        // survives because the autocommit write went straight to the durable .dat file.
        ReinitializeEngine(Config);
        Execute($"USE {TestDb};");

        var afterRestart = ExecuteAndReturn($"SELECT * FROM {table} WHERE Id = 7;");
        Assert.Single(afterRestart.Data);
        Assert.Equal("Bob", afterRestart.Data[0]["Name"]?.ToString());
    }

    [Fact]
    [BrowserTranslateIgnore("Disk WAL file inspection is disk-specific and outside browser SQL parity")]
    public void ExplicitTransactionCommit_DoesWriteToWal()
    {
        // Contrast case: the SAME insert wrapped in BEGIN/COMMIT DOES produce a checkpointed WAL entry.
        string table = $"TxInsert_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        Execute("BEGIN;");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (2, 'Carol');");
        Execute("COMMIT;");

        string walPath = Config.ResolveWalFilePath();
        Assert.True(File.Exists(walPath));
        List<WalEntry> entries = new WalReader(Config).ReadAll();
        Assert.Contains(entries, e => e.Operations.Any(op =>
            op.OperationType == WalOperationType.Insert && op.TableName == table));
    }
}

public class DiskGroupCommitAutocommitTests : SqlExecutionTestsBase
{
    public DiskGroupCommitAutocommitTests()
        : base(new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = "./test_datavo_group_commit_autocommit",
            WalFilePath = "datavo.walbin",
            WalCheckpointThreshold = 1000,
            IoSchedulerMode = IoSchedulerMode.GroupCommit,
        }, "GroupCommitAutocommitDb")
    {
    }

    [Fact]
    [BrowserTranslateIgnore("Binary WAL frame inspection is disk-specific and outside browser SQL parity")]
    public void AutocommitInsert_WritesBinaryWalFrame_WhenGroupCommitSchedulerIsEnabled()
    {
        string table = $"GcInsert_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        string walPath = Config.ResolveWalFilePath();
        if (File.Exists(walPath))
        {
            File.Delete(walPath);
        }

        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");

        Assert.True(File.Exists(walPath));
        byte[] bytes = File.ReadAllBytes(walPath);
        Assert.True(bytes.Length >= WalAppender.FrameHeaderSize);
        Assert.True(WalAppender.TryReadFrameHeader(bytes, out WalFrameHeader header));
        Assert.Equal(WalFrameOperationType.TxnCommit, header.OpType);
        Assert.True(WalAppender.ValidateFrame(bytes, header));
    }
}

/// <summary>
/// Functional coverage for the durable (fsync) disk write mode introduced via
/// <see cref="DataVoConfig.SyncDiskWrites"/>. We can't unit-test power-loss durability, but we can
/// prove the fsync'd write path stays correct end-to-end: insert, update, read, and survive a restart.
/// </summary>
public class DurableDiskWriteTests : SqlExecutionTestsBase
{
    public DurableDiskWriteTests()
        : base(new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = "./test_datavo_fsync",
            WalFilePath = "datavo.wal",
            WalCheckpointThreshold = 1000,
            SyncDiskWrites = true,
        }, "DurableDiskDb")
    {
    }

    [Fact]
    [BrowserTranslateIgnore("fsync'd disk write path is disk-specific and outside browser SQL parity")]
    public void SyncDiskWrites_InsertUpdateRead_RoundTripsAndSurvivesRestart()
    {
        Assert.True(Config.SyncDiskWrites);

        string table = $"Durable_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50), Value INT);");

        Execute($"INSERT INTO {table} (Id, Name, Value) VALUES (1, 'Alice', 10);");
        Execute($"UPDATE {table} SET Value = 99 WHERE Id = 1;");

        var afterUpdate = ExecuteAndReturn($"SELECT Value FROM {table} WHERE Id = 1;");
        Assert.Single(afterUpdate.Data);
        Assert.Equal(99, Convert.ToInt32(afterUpdate.Data[0]["Value"]));

        ReinitializeEngine(Config);
        Execute($"USE {TestDb};");

        var afterRestart = ExecuteAndReturn($"SELECT Name, Value FROM {table} WHERE Id = 1;");
        Assert.Single(afterRestart.Data);
        Assert.Equal("Alice", afterRestart.Data[0]["Name"]?.ToString());
        Assert.Equal(99, Convert.ToInt32(afterRestart.Data[0]["Value"]));
    }
}
