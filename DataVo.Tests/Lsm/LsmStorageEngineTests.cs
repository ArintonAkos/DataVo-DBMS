using System.Reflection;
using System.Text;
using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public sealed class LsmStorageEngineTests
{
    [Fact]
    public void InsertRow_ReadRow_RoundTripsLogicalRowIdStartingAtOne()
    {
        using var fixture = new LsmStorageEngineFixture();

        long rowId = fixture.Engine.InsertRow("db", "users", Val("alice"));

        Assert.Equal(1, rowId);
        Assert.Equal(Val("alice"), fixture.Engine.ReadRow("db", "users", rowId));
    }

    [Fact]
    public void InsertRows_ReturnsSequentialIdsAndReadAllRowsInIdOrder()
    {
        using var fixture = new LsmStorageEngineFixture();

        List<long> rowIds = fixture.Engine.InsertRows(
            "db",
            "users",
            [Val("charlie"), Val("alice"), Val("bob")]);

        Assert.Equal([1L, 2L, 3L], rowIds);
        AssertRows(
            [(1L, Val("charlie")), (2L, Val("alice")), (3L, Val("bob"))],
            fixture.Engine.ReadAllRows("db", "users").ToList());
    }

    [Fact]
    public void DeleteRow_TombstonesLogicalRowIdAndReadAllRowsSkipsIt()
    {
        using var fixture = new LsmStorageEngineFixture();
        List<long> rowIds = fixture.Engine.InsertRows(
            "db",
            "users",
            [Val("one"), Val("two"), Val("three")]);

        fixture.Engine.DeleteRow("db", "users", rowIds[1]);

        Assert.Throws<RowDeletedException>(() => fixture.Engine.ReadRow("db", "users", rowIds[1]));
        AssertRows(
            [(1L, Val("one")), (3L, Val("three"))],
            fixture.Engine.ReadAllRows("db", "users").ToList());
    }

    [Fact]
    public void ReadRow_MissingRowThrowsRowNotFoundException()
    {
        using var fixture = new LsmStorageEngineFixture();

        Assert.Throws<RowNotFoundException>(() => fixture.Engine.ReadRow("db", "users", 42));
    }

    [Fact]
    public void CompactTable_DoesNotRemapLogicalRowIds()
    {
        using var fixture = new LsmStorageEngineFixture();
        List<long> rowIds = fixture.Engine.InsertRows(
            "db",
            "users",
            [Val("one"), Val("two"), Val("three")]);
        fixture.Engine.DeleteRow("db", "users", rowIds[1]);

        List<(long NewRowId, byte[] RawRow)> compacted = fixture.Engine.CompactTable("db", "users");

        AssertRows(
            [(1L, Val("one")), (3L, Val("three"))],
            compacted);
        Assert.Equal(4, fixture.Engine.InsertRow("db", "users", Val("four")));
        Assert.Equal(Val("three"), fixture.Engine.ReadRow("db", "users", 3));
    }

    [Fact]
    public void ReadRow_WhenFileNumberOrderDiffersFromSequenceOrder_ReturnsNewestSequence()
    {
        using var fixture = new LsmStorageEngineFixture();
        fixture.Engine.Dispose();

        string tableDirectory = Path.Combine(fixture.RootDirectory, "db", "users");
        Directory.CreateDirectory(tableDirectory);
        var manifest = new LsmManifest(Path.Combine(tableDirectory, "MANIFEST"));
        AddSstable(tableDirectory, manifest, fileNumber: 1, Internal(rowId: 1, seqno: 20, LsmValueType.Put), Val("newer"));
        AddSstable(tableDirectory, manifest, fileNumber: 2, Internal(rowId: 1, seqno: 10, LsmValueType.Put), Val("older"));

        using var reopened = new LsmStorageEngine(fixture.RootDirectory);

        Assert.Equal(Val("newer"), reopened.ReadRow("db", "users", 1));
    }

    [Fact]
    public void Reopen_AfterHighestRowIdTombstoneAndCompaction_PreservesDeletionAndNextRowId()
    {
        using var fixture = new LsmStorageEngineFixture();
        List<long> rowIds = fixture.Engine.InsertRows(
            "db",
            "users",
            [Val("one"), Val("two"), Val("three")]);
        fixture.Engine.DeleteRow("db", "users", rowIds[2]);
        fixture.Engine.CompactTable("db", "users");
        fixture.Engine.Dispose();

        using var reopened = new LsmStorageEngine(fixture.RootDirectory);

        Assert.Throws<RowDeletedException>(() => reopened.ReadRow("db", "users", rowIds[2]));
        AssertRows(
            [(1L, Val("one")), (2L, Val("two"))],
            reopened.ReadAllRows("db", "users").ToList());
        Assert.Equal(4, reopened.InsertRow("db", "users", Val("four")));
    }

    [Fact]
    public void Reopen_AfterDirtyShutdown_ReplaysActiveWal()
    {
        string root = Path.Combine(Path.GetTempPath(), "datavo-lsm-storage-engine-tests", Guid.NewGuid().ToString("N"));
        LsmStorageEngine? reopened = null;
        try
        {
            var first = new LsmStorageEngine(root);
            long rowId = first.InsertRow("db", "users", Val("wal-only"));
            first = null;

            reopened = new LsmStorageEngine(root);

            Assert.Equal(Val("wal-only"), reopened.ReadRow("db", "users", rowId));
        }
        finally
        {
            reopened?.Dispose();
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task InsertRows_StrictDurability_AmortizesWalFlushWhileSnapshotsOpenConcurrently()
    {
        const int rows = 1024;
        using var fixture = new LsmStorageEngineFixture();
        fixture.Engine.InsertRow("db", "users", Val("seed"));
        LsmFileRegistry registry = GetFileRegistry(fixture.Engine);
        int flushesBeforeBatch = GetWalDurableFlushCount(fixture.Engine);
        using var snapshotsCanRun = new CancellationTokenSource();

        Task snapshotLoop = Task.Run(() =>
        {
            while (!snapshotsCanRun.IsCancellationRequested)
            {
                using LsmReadSnapshot snapshot = registry.OpenSnapshot();
                foreach (LsmTableFileMetadata file in snapshot.Files)
                {
                    _ = snapshot.ReadAllBytes(file.FileNumber);
                }
            }
        });

        Task<List<long>> insertTask = Task.Run(() =>
            fixture.Engine.InsertRows(
                "db",
                "users",
                Enumerable.Range(0, rows).Select(i => Val($"bulk-{i}")).ToList()));

        List<long> rowIds;
        try
        {
            rowIds = await insertTask.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            snapshotsCanRun.Cancel();
            await snapshotLoop.WaitAsync(TimeSpan.FromSeconds(5));
        }

        Assert.Equal(rows, rowIds.Count);
        Assert.Equal(1, GetWalDurableFlushCount(fixture.Engine) - flushesBeforeBatch);
    }

    [Fact]
    public void HasAnyRows_EmptyTableReturnsFalse()
    {
        using var fixture = new LsmStorageEngineFixture();

        Assert.False(fixture.Engine.HasAnyRows("db", "users"));
    }

    [Fact]
    public void HasAnyRows_AfterInsertReturnsTrue()
    {
        using var fixture = new LsmStorageEngineFixture();
        fixture.Engine.InsertRow("db", "users", Val("alice"));

        Assert.True(fixture.Engine.HasAnyRows("db", "users"));
    }

    [Fact]
    public void HasAnyRows_AfterDeletingAllRowsReturnsFalse()
    {
        using var fixture = new LsmStorageEngineFixture();
        long rowId = fixture.Engine.InsertRow("db", "users", Val("alice"));
        fixture.Engine.DeleteRow("db", "users", rowId);

        Assert.False(fixture.Engine.HasAnyRows("db", "users"));
    }

    [Fact]
    public void HasAnyRows_PersistsAcrossReopen()
    {
        using var fixture = new LsmStorageEngineFixture();
        fixture.Engine.InsertRow("db", "users", Val("alice"));
        fixture.Engine.Dispose();

        using var reopened = new LsmStorageEngine(fixture.RootDirectory);

        Assert.True(reopened.HasAnyRows("db", "users"));
    }

    [Fact]
    public void HasAnyRows_FlushesBufferedMutationsLikeScanProbes()
    {
        // Read probes historically flushed the active MemTable before answering (via ReadAllRows);
        // bulk-ingest disk layout depends on that cadence, so the cheap probe must keep it.
        using var fixture = new LsmStorageEngineFixture();
        fixture.Engine.InsertRow("db", "users", Val("alice"));

        Assert.True(fixture.Engine.HasAnyRows("db", "users"));

        string tableDirectory = Path.Combine(fixture.RootDirectory, "db", "users");
        Assert.NotEmpty(Directory.GetFiles(tableDirectory, "*.sst"));
    }

    [Fact]
    public void HasAnyRows_DoesNotRescanSstablesOnLargeTables()
    {
        using var fixture = new LsmStorageEngineFixture();
        var rows = new List<byte[]>(4096);
        for (int i = 0; i < 4096; i++)
        {
            rows.Add(Val($"row-{i}"));
        }

        fixture.Engine.InsertRows("db", "users", rows);
        Assert.True(fixture.Engine.HasAnyRows("db", "users")); // warm-up: flush + JIT

        long before = GC.GetAllocatedBytesForCurrentThread();
        bool hasRows = fixture.Engine.HasAnyRows("db", "users");
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(hasRows);
        Assert.True(allocated < 1024, $"HasAnyRows allocated {allocated} bytes; expected a rescan-free probe.");
    }

    private static byte[] Val(string value) => Encoding.UTF8.GetBytes(value);

    private static byte[] Internal(long rowId, ulong seqno, LsmValueType valueType)
    {
        byte[] userKey = new byte[sizeof(long)];
        InternalKey.EncodeInt64UserKey(userKey, rowId);
        byte[] internalKey = new byte[InternalKey.MeasureSize(userKey.Length)];
        InternalKey.Write(internalKey, userKey, seqno, valueType);
        return internalKey;
    }

    private static void AddSstable(
        string tableDirectory,
        LsmManifest manifest,
        long fileNumber,
        byte[] internalKey,
        byte[] value)
    {
        var writer = new SsTableWriter(expectedEntries: 1);
        writer.Add(internalKey, value);
        byte[] bytes = writer.Finish();
        string fileName = $"{fileNumber:D6}.sst";
        File.WriteAllBytes(Path.Combine(tableDirectory, fileName), bytes);

        var edit = new LsmVersionEdit();
        edit.AddFile(new LsmTableFileMetadata(
            fileNumber,
            level: 0,
            internalKey,
            internalKey,
            bytes.LongLength,
            fileName));
        manifest.ApplyEdit(edit);
    }

    private static void AssertRows(
        IReadOnlyList<(long RowId, byte[] RawRow)> expected,
        IReadOnlyList<(long RowId, byte[] RawRow)> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].RowId, actual[i].RowId);
            Assert.Equal(expected[i].RawRow, actual[i].RawRow);
        }
    }

    private static LsmFileRegistry GetFileRegistry(LsmStorageEngine engine)
    {
        object tableState = GetSingleTableState(engine);
        return (LsmFileRegistry)tableState
            .GetType()
            .GetProperty("FileRegistry", BindingFlags.Instance | BindingFlags.Public)!
            .GetValue(tableState)!;
    }

    private static int GetWalDurableFlushCount(LsmStorageEngine engine)
    {
        object tableState = GetSingleTableState(engine);
        object table = tableState
            .GetType()
            .GetProperty("Table", BindingFlags.Instance | BindingFlags.Public)!
            .GetValue(tableState)!;
        object walWriter = table
            .GetType()
            .GetField("_walWriter", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(table)!;

        return (int)walWriter
            .GetType()
            .GetProperty("DurableFlushCount", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(walWriter)!;
    }

    private static object GetSingleTableState(LsmStorageEngine engine)
    {
        object tables = typeof(LsmStorageEngine)
            .GetField("_tables", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(engine)!;
        object values = tables
            .GetType()
            .GetProperty("Values")!
            .GetValue(tables)!;
        return ((System.Collections.IEnumerable)values).Cast<object>().Single();
    }

    private sealed class LsmStorageEngineFixture : IDisposable
    {
        public LsmStorageEngineFixture()
        {
            RootDirectory = Path.Combine(Path.GetTempPath(), "datavo-lsm-storage-engine-tests", Guid.NewGuid().ToString("N"));
            Engine = new LsmStorageEngine(RootDirectory);
        }

        public string RootDirectory { get; }

        public LsmStorageEngine Engine { get; }

        public void Dispose()
        {
            Engine.Dispose();
            if (Directory.Exists(RootDirectory))
            {
                Directory.Delete(RootDirectory, recursive: true);
            }
        }
    }
}
