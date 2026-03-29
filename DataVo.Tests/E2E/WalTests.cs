using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;
using DataVo.Core.Runtime;
using DataVo.Tests.BrowserParity;
using Newtonsoft.Json.Linq;

namespace DataVo.Tests.E2E;

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
    [BrowserTranslateIgnore("WAL file and entry checkpoint persistence validation is disk-specific and outside browser SQL scenario parity")]
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
        Assert.True(entries[0].MvccTransactionId > 0);
        Assert.Contains(entries[0].Operations, operation => operation.OperationType == WalOperationType.Insert && operation.TableName == table);
    }

    [Fact]
    [BrowserTranslateIgnore("WAL replay + engine reinitialization flow depends on disk WAL internals not representable in browser SQL fixture model")]
    public void Recovery_ReplaysUncheckpointedWalEntries()
    {
        string table = $"WalRecovery_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        var entry = new WalEntry
        {
            TransactionId = Guid.NewGuid(),
            MvccTransactionId = 9876,
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

        ReinitializeEngine(Config);
        Execute($"USE {TestDb};");

        var afterRecovery = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.Single(afterRecovery.Data);
        Assert.Equal("Alice", afterRecovery.Data[0]["Name"]?.ToString());
        Assert.False(File.Exists(Config.ResolveWalFilePath()));

        // Ensure allocator was advanced past recovered MVCC transaction IDs.
        long nextTxId = Engine.TransactionIdAllocator.AllocateTransactionId();
        Assert.True(nextTxId > 9876);
    }

    [Fact]
    [BrowserTranslateIgnore("WAL truncation threshold behavior depends on local file state not represented in browser fixture model")]
    public void CheckpointThreshold_TruncatesCheckpointedWalEntries()
    {
        var thresholdConfig = new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = Config.DiskStoragePath,
            WalFilePath = Config.WalFilePath,
            WalCheckpointThreshold = 2,
        };

        ReinitializeEngine(thresholdConfig);
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

    [Fact]
    [BrowserTranslateIgnore("WAL corruption checksum validation is disk-file specific and outside browser fixture semantics")]
    public void Reader_ThrowsOnChecksumMismatch()
    {
        var writer = new WalWriter(Config);
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
                    TableName = "T",
                    RowData = new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Alice" }
                }
            ]
        };

        writer.Append(entry);

        string walPath = Config.ResolveWalFilePath();
        string line = File.ReadAllLines(walPath).Single();
        string tampered = line.Replace("Alice", "Alicf", StringComparison.Ordinal);
        File.WriteAllText(walPath, tampered + Environment.NewLine);

        Assert.Throws<InvalidDataException>(() => new WalReader(Config).ReadAll());
    }

    [Fact]
    [BrowserTranslateIgnore("WAL vector envelope serialization requires raw file payload inspection")]
    public void Wal_VectorRowData_UsesCompactEnvelopeFormat()
    {
        var writer = new WalWriter(Config);
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
                    TableName = "VecTable",
                    RowData = new Dictionary<string, object?>
                    {
                        ["Id"] = 1,
                        ["Emb"] = new float[] { 1f, 0.25f, -2f, 3.5f }
                    }
                }
            ]
        };

        writer.Append(entry);

        string walPath = Config.ResolveWalFilePath();
        string line = File.ReadAllLines(walPath).Single();
        JObject envelope = JObject.Parse(line);
        string payload = envelope["Payload"]!.Value<string>()!;
        JObject payloadObj = JObject.Parse(payload);

        JObject rowData = (JObject)payloadObj["Operations"]![0]!["RowData"]!;
        JObject emb = (JObject)rowData["Emb"]!;

        Assert.Equal("vector-f32b64-v1", emb["__dvType"]!.Value<string>());
        Assert.Equal(4, emb["dims"]!.Value<int>());
        Assert.False(string.IsNullOrWhiteSpace(emb["data"]!.Value<string>()));
    }

    [Fact]
    [BrowserTranslateIgnore("WAL vector envelope replay validation requires disk WAL readback")]
    public void Wal_VectorRowData_ReplayNormalizesToFloatArray()
    {
        var writer = new WalWriter(Config);
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
                    TableName = "VecTable",
                    RowData = new Dictionary<string, object?>
                    {
                        ["Id"] = 1,
                        ["Emb"] = new float[] { 0.1f, 0.2f, 0.3f }
                    }
                }
            ]
        };

        writer.Append(entry);

        WalEntry readEntry = new WalReader(Config).ReadAll().Single();
        TransactionContext replay = readEntry.ToTransactionContext();

        Assert.True(replay.InsertedRows.TryGetValue("VecTable", out var rows));
        Assert.Single(rows!);
        Assert.True(rows[0].TryGetValue("Emb", out var embValue));

        float[] vector = Assert.IsType<float[]>(embValue);
        Assert.Equal([0.1f, 0.2f, 0.3f], vector);
    }

    [Fact]
    [BrowserTranslateIgnore("Transaction ID state persistence requires disk-backed runtime lifecycle")]
    public void TransactionIdAllocator_PersistsHighWaterMarkAcrossRestart()
    {
        var txConfig = new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = Config.DiskStoragePath,
            WalFilePath = Config.WalFilePath,
            TransactionIdStateFilePath = "datavo.txid"
        };

        using (var engine1 = DataVoEngine.Initialize(txConfig))
        {
            Assert.Equal(1, engine1.TransactionIdAllocator.AllocateTransactionId());
            Assert.Equal(2, engine1.TransactionIdAllocator.AllocateTransactionId());
            Assert.Equal(3, engine1.TransactionIdAllocator.AllocateTransactionId());
        }

        using var engine2 = DataVoEngine.Initialize(txConfig);
        long next = engine2.TransactionIdAllocator.AllocateTransactionId();
        Assert.Equal(4, next);
    }
}

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
    [BrowserTranslateIgnore("In-memory WAL file absence assertion is filesystem-specific and not a browser SQL scenario")]
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
