using System.Diagnostics;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Transactions;
using DataVo.Tests.E2E;

namespace DataVo.Tests.Transactions;

/// <summary>
/// Verifies the background checkpoint worker autonomously fsyncs the data files, advances the checkpoint
/// LSN, and prunes the WAL on its interval — no forced checkpoint call required.
/// </summary>
public class BinaryWalBackgroundCheckpointerTests : SqlExecutionTestsBase
{
    public BinaryWalBackgroundCheckpointerTests()
        : base(new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = "./test_datavo_binwal_bgcheckpoint",
            WalFilePath = "datavo.walbin",
            WalCheckpointThreshold = 1000,
            WalCheckpointIntervalMs = 50,
            IoSchedulerMode = IoSchedulerMode.GroupCommit,
        }, "BinWalBgCheckpointDb")
    {
    }

    [Fact]
    public void BackgroundCheckpointer_AdvancesCheckpointLsn_AndPrunesWal()
    {
        string table = $"Bg_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (2, 'Bob');");

        string walPath = Config.ResolveWalFilePath();

        // Poll (no fixed sleep-then-assert) for the background worker to checkpoint through both frames.
        var deadline = Stopwatch.StartNew();
        bool pruned = false;
        while (deadline.Elapsed < TimeSpan.FromSeconds(10))
        {
            if (Engine.WalCheckpointer!.CheckpointLsn >= 2
                && new WalFileStore(walPath).ReadBinaryFrames().Count == 0)
            {
                pruned = true;
                break;
            }

            Thread.Sleep(25);
        }

        Assert.True(pruned, "Background checkpointer did not advance the checkpoint LSN and prune the WAL within the timeout.");
        Assert.Equal(2, ExecuteAndReturn($"SELECT Id FROM {table};").Data.Count);
    }
}

/// <summary>
/// Phase 4 coverage for binary WAL recovery and checkpointing in <see cref="IoSchedulerMode.GroupCommit"/>.
/// These tests craft a durable binary WAL out-of-band (frames whose effects never reached the .dat
/// files) and then restart the engine, proving startup recovery scans, validates, and re-applies the
/// frames — closing the durability loop the group-commit pipeline opened.
/// </summary>
public class BinaryWalRecoveryTests : SqlExecutionTestsBase
{
    public BinaryWalRecoveryTests()
        : base(new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = "./test_datavo_binwal_recovery",
            WalFilePath = "datavo.walbin",
            WalCheckpointThreshold = 1000,
            IoSchedulerMode = IoSchedulerMode.GroupCommit,
        }, "BinWalRecoveryDb")
    {
    }

    [Fact]
    public void BinaryWal_FramesReplayedOnStartup_RowsBecomeReadable()
    {
        string table = $"Replay_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        string walPath = Config.ResolveWalFilePath();
        string db = TestDb;

        // Bring the engine down (no inserts ran through it, so .dat for the table holds only its header
        // and the checkpoint watermark is still 0). Then forge a durable binary WAL describing two
        // inserts that NEVER touched the .dat file — exactly the crash window the WAL exists to cover.
        Engine.Dispose();
        CraftInsertWal(walPath, db, table, [(1, "Alice"), (2, "Bob")], appendTornTail: false);

        Engine = DataVoEngine.Initialize(Config);
        Execute($"USE {db};");

        var rows = ExecuteAndReturn($"SELECT Id, Name FROM {table} ORDER BY Id;");
        Assert.Equal(2, rows.Data.Count);
        Assert.Equal("Alice", rows.Data[0]["Name"]?.ToString());
        Assert.Equal("Bob", rows.Data[1]["Name"]?.ToString());
    }

    [Fact]
    public void BinaryWal_StopsAtTornTail_RecoversFramesBeforeIt()
    {
        string table = $"Torn_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        string walPath = Config.ResolveWalFilePath();
        string db = TestDb;

        Engine.Dispose();
        // Two intact frames followed by a partial write that never finished — a classic crash tail.
        CraftInsertWal(walPath, db, table, [(1, "Alice"), (2, "Bob")], appendTornTail: true);

        Engine = DataVoEngine.Initialize(Config);
        Execute($"USE {db};");

        var rows = ExecuteAndReturn($"SELECT Id, Name FROM {table} ORDER BY Id;");
        Assert.Equal(2, rows.Data.Count);
        Assert.Equal("Alice", rows.Data[0]["Name"]?.ToString());
        Assert.Equal("Bob", rows.Data[1]["Name"]?.ToString());
    }

    [Fact]
    public void BinaryWal_StopsAtChecksumMismatch_RecoversFramesBeforeIt()
    {
        string table = $"Crc_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        string walPath = Config.ResolveWalFilePath();
        string db = TestDb;

        Engine.Dispose();
        CraftInsertWal(walPath, db, table, [(1, "Alice"), (2, "Bob")], appendTornTail: false);
        // Flip a byte inside the second frame's payload so its CRC32C no longer matches. Recovery must
        // treat the corrupted frame as the torn tail and stop, keeping only the intact first frame.
        CorruptLastByte(walPath);

        Engine = DataVoEngine.Initialize(Config);
        Execute($"USE {db};");

        var rows = ExecuteAndReturn($"SELECT Id, Name FROM {table} ORDER BY Id;");
        Assert.Single(rows.Data);
        Assert.Equal("Alice", rows.Data[0]["Name"]?.ToString());
    }

    [Fact]
    public void BinaryWal_RecoveredFrames_NotReplayedAgainOnSecondRestart()
    {
        string table = $"Once_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        string walPath = Config.ResolveWalFilePath();
        string db = TestDb;

        Engine.Dispose();
        CraftInsertWal(walPath, db, table, [(1, "Alice"), (2, "Bob")], appendTornTail: false);

        // First restart: recovery replays the two frames, then checkpoints through them and prunes the WAL.
        Engine = DataVoEngine.Initialize(Config);
        Execute($"USE {db};");
        Assert.Equal(2, ExecuteAndReturn($"SELECT Id FROM {table};").Data.Count);

        // Second restart: the frames are now below the checkpoint LSN (and the WAL is pruned), so they
        // must not be replayed a second time — the row count stays at two, not four.
        ReinitializeEngine(Config);
        Execute($"USE {db};");
        Assert.Equal(2, ExecuteAndReturn($"SELECT Id FROM {table};").Data.Count);
    }

    [Fact]
    public void GroupCommitInsert_SurvivesRestart_ExactlyOnce()
    {
        string table = $"Survive_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (2, 'Bob');");

        ReinitializeEngine(Config);
        Execute($"USE {TestDb};");

        var rows = ExecuteAndReturn($"SELECT Id, Name FROM {table} ORDER BY Id;");
        Assert.Equal(2, rows.Data.Count);
        Assert.Equal("Alice", rows.Data[0]["Name"]?.ToString());
        Assert.Equal("Bob", rows.Data[1]["Name"]?.ToString());
    }

    [Fact]
    public void Checkpoint_AdvancesCheckpointLsn_PrunesWal_AndKeepsRowsReadable()
    {
        string table = $"Ckpt_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (2, 'Bob');");

        string walPath = Config.ResolveWalFilePath();
        Assert.NotEmpty(new WalFileStore(walPath).ReadBinaryFrames());

        long checkpointed = Engine.WalCheckpointer!.Checkpoint();

        Assert.True(checkpointed >= 2, $"Expected checkpoint LSN to advance to at least 2 but was {checkpointed}.");
        Assert.Empty(new WalFileStore(walPath).ReadBinaryFrames());

        // Checkpointing prunes the WAL but must never drop committed rows.
        var rows = ExecuteAndReturn($"SELECT Id FROM {table};");
        Assert.Equal(2, rows.Data.Count);
    }

    [Fact]
    public void BinaryWal_UpdateFrameReplayedOnStartup_NewValueReadable()
    {
        const string table = "Records";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(40), Value INT, Score FLOAT);");
        Execute($"INSERT INTO {table} (Id, Name, Value, Score) VALUES (1, 'a', 10, 1.5);");

        string db = TestDb;
        string walPath = Config.ResolveWalFilePath();

        // Serialize the post-update row (Value 10 -> 99) in the engine's on-disk wire format, exactly as
        // the live zero-alloc path will hand it to the WAL frame.
        IReadOnlyList<Column> columns = Engine.Catalog.GetTableColumns(table, db);
        byte[] newRowBytes = RowSerializer.SerializeCells(
            columns,
            [CellValue.From(1), CellValue.From("a"), CellValue.From(99), CellValue.From(1.5d)]);

        Engine.Dispose();

        // Drop the checkpoint watermark so the forged Update frame counts as an uncheckpointed tail.
        string checkpointPath = Config.ResolveCheckpointStateFilePath();
        if (File.Exists(checkpointPath))
        {
            File.Delete(checkpointPath);
        }

        // The sole row in a freshly created .dat lives at byte offset = the 8-byte file header size.
        const long firstRowId = 8;
        CraftUpdateFrameWal(walPath, db, table, firstRowId, newRowBytes);

        Engine = DataVoEngine.Initialize(Config);
        Execute($"USE {db};");

        var rows = ExecuteAndReturn($"SELECT Value FROM {table} WHERE Id = 1;");
        Assert.Single(rows.Data);
        Assert.Equal(99, Convert.ToInt32(rows.Data[0]["Value"]));
    }

    /// <summary>
    /// Forges a durable binary WAL containing a single <see cref="WalFrameOperationType.Update"/> frame,
    /// using the exact appender + payload codec the live update path uses.
    /// </summary>
    private static void CraftUpdateFrameWal(string walPath, string databaseName, string tableName, long oldRowId, byte[] newRowBytes)
    {
        if (File.Exists(walPath))
        {
            File.Delete(walPath);
        }

        var appender = new WalAppender(1 << 20);
        var store = new WalFileStore(walPath);

        int size = WalUpdateFramePayload.MeasureSize(databaseName, tableName, newRowBytes.Length);
        WalFrameReservation reservation =
            appender.Reserve(WalFrameOperationType.Update, tableId: 0, rowId: oldRowId, size);
        WalUpdateFramePayload.Write(reservation.PayloadSpan, databaseName, tableName, oldRowId, newRowBytes);
        using WalFrame frame = reservation.Commit();
        store.AppendFrame(frame);
    }

    /// <summary>
    /// Corrupts the final byte of the WAL file, invalidating the last frame's CRC32C.
    /// </summary>
    private static void CorruptLastByte(string walPath)
    {
        byte[] bytes = File.ReadAllBytes(walPath);
        bytes[^1] ^= 0xFF;
        File.WriteAllBytes(walPath, bytes);
    }

    /// <summary>
    /// Forges a durable binary WAL file containing one TxnCommit frame per insert, using the exact same
    /// appender + framing the engine uses for group commit. Optionally appends a torn (partial) tail.
    /// </summary>
    private static void CraftInsertWal(
        string walPath,
        string databaseName,
        string tableName,
        (long Id, string Name)[] rows,
        bool appendTornTail)
    {
        string? directory = Path.GetDirectoryName(walPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        if (File.Exists(walPath))
        {
            File.Delete(walPath);
        }

        var appender = new WalAppender(1 << 20);
        var store = new WalFileStore(walPath);

        foreach ((long id, string name) in rows)
        {
            var entry = new WalEntry
            {
                TransactionId = Guid.NewGuid(),
                MvccTransactionId = id,
                Timestamp = DateTime.UtcNow.Ticks,
                DatabaseName = databaseName,
                Operations =
                [
                    new WalOperation
                    {
                        OperationType = WalOperationType.Insert,
                        TableName = tableName,
                        RowData = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                        {
                            ["Id"] = id,
                            ["Name"] = name,
                        },
                    },
                ],
                IsCheckpointed = false,
            };

            byte[] payload = WalFileStore.SerializeWalEntryPayload(entry);
            WalFrameReservation reservation =
                appender.Reserve(WalFrameOperationType.TxnCommit, tableId: 0, rowId: 0, payload.Length);
            payload.CopyTo(reservation.PayloadSpan);
            using WalFrame frame = reservation.Commit();
            store.AppendFrame(frame);
        }

        if (appendTornTail)
        {
            // Fewer than a full frame header: a write that was interrupted mid-append. Recovery must
            // stop here rather than mis-parse the partial bytes. Share with the pooled WAL handle that
            // AppendFrame leaves open so the append does not collide on its file lock.
            using var fs = new FileStream(
                walPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete);
            fs.Write(new byte[] { 0xAB, 0xCD, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 });
        }
    }
}
