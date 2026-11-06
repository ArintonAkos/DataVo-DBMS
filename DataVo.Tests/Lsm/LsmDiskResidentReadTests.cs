using DataVo.Core;
using System.Text;
using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

/// <summary>
/// The LSM engine serves flushed rows from memory-mapped (or positionally read) v2 SSTables instead of
/// keeping every row in the managed heap. These tests pin the observable behavior across flush, overlay
/// eviction, compaction, reopen, v1 upgrade, and concurrent readers, in both read modes.
/// </summary>
public sealed class LsmDiskResidentReadTests : IDisposable
{
    private const string Db = "db";
    private const string Table = "items";

    // fsync per commit only measures the disk here; the read-path behavior under test is the same.
    private const LsmWalDurabilityMode Relaxed = LsmWalDurabilityMode.RelaxedOsBuffer;
    private readonly string _root = TestStorageDirectories.Create("lsm_disk_resident");

    public static TheoryData<LsmReadMode> Modes => new() { LsmReadMode.MemoryMapped, LsmReadMode.PositionalRead };

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void FlushedRows_LeaveTheHeapOverlay_AndStayReadable(LsmReadMode mode)
    {
        using var engine = new LsmStorageEngine(_root, Relaxed, mode);
        List<RowKey> rowIds = engine.InsertRows(Db, Table, [.. Enumerable.Range(1, 5_000).Select(Row)]);
        Assert.Equal(5_000, engine.OverlayRowCount(Db, Table));

        engine.FlushTableForTesting(Db, Table);

        Assert.Equal(0, engine.OverlayRowCount(Db, Table));
        foreach (RowKey rowId in rowIds.Where(id => (long)id % 97 == 0))
        {
            Assert.Equal(Row((int)(long)rowId), engine.ReadRow(Db, Table, rowId));
        }

        Assert.Equal(5_000, engine.EstimateRowCount(Db, Table));
        Assert.Equal(5_000, engine.ReadAllRows(Db, Table).Count());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UpdatesAndDeletes_AcrossFlushCompactionAndReopen_KeepTheNewestVersion(LsmReadMode mode)
    {
        using (var engine = new LsmStorageEngine(_root, Relaxed, mode))
        {
            engine.InsertRows(Db, Table, [.. Enumerable.Range(1, 3_000).Select(Row)]);
            engine.FlushTableForTesting(Db, Table);

            // Rewrite and delete rows that now live only in the SSTable.
            for (long rowId = 1; rowId <= 3_000; rowId += 10)
            {
                engine.DeleteRow(Db, Table, rowId);
            }

            engine.FlushTableForTesting(Db, Table);
            engine.CompactTable(Db, Table);
            engine.DeleteRow(Db, Table, 2); // stays in the overlay until shutdown

            Assert.Throws<RowDeletedException>(() => engine.ReadRow(Db, Table, 11));
            Assert.Throws<RowDeletedException>(() => engine.ReadRow(Db, Table, 2));
            Assert.Equal(Row(3), engine.ReadRow(Db, Table, 3));
            Assert.Equal(2_699, engine.EstimateRowCount(Db, Table));
        }

        using var reopened = new LsmStorageEngine(_root, Relaxed, mode);
        Assert.Equal(0, reopened.OverlayRowCount(Db, Table)); // open reads no rows
        Assert.Throws<RowDeletedException>(() => reopened.ReadRow(Db, Table, 11));
        Assert.Throws<RowDeletedException>(() => reopened.ReadRow(Db, Table, 2));
        Assert.Equal(Row(2_999), reopened.ReadRow(Db, Table, 2_999));
        Assert.Equal(2_699, reopened.EstimateRowCount(Db, Table));
        Assert.True(reopened.HasAnyRows(Db, Table));

        List<(RowKey RowId, byte[] RawRow)> all = [.. reopened.ReadAllRows(Db, Table)];
        Assert.Equal(2_699, all.Count);
        Assert.True(all.Zip(all.Skip(1)).All(pair => pair.First.RowId < pair.Second.RowId));
        Assert.Equal(3_001, reopened.InsertRow(Db, Table, Row(3_001)));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Reopen_AfterEveryRowWasDeleted_KeepsRowIdsMonotonicAndCountZero(LsmReadMode mode)
    {
        using (var engine = new LsmStorageEngine(_root, Relaxed, mode))
        {
            List<RowKey> rowIds = engine.InsertRows(Db, Table, [Row(1), Row(2), Row(3)]);
            foreach (long rowId in rowIds)
            {
                engine.DeleteRow(Db, Table, rowId);
            }

            engine.CompactTable(Db, Table);
        }

        using var reopened = new LsmStorageEngine(_root, Relaxed, mode);
        Assert.False(reopened.HasAnyRows(Db, Table));
        Assert.Equal(0, reopened.EstimateRowCount(Db, Table));
        Assert.Equal(4, reopened.InsertRow(Db, Table, Row(4)));
    }

    [Fact]
    public void LegacyV1Sstables_AreUpgradedOnOpen_WithIdenticalContents()
    {
        string tableDirectory = Path.Combine(_root, Db, Table);
        Directory.CreateDirectory(tableDirectory);
        var manifest = new LsmManifest(Path.Combine(tableDirectory, "MANIFEST"));
        using (var legacy = new LsmTable(tableDirectory, manifest)) // v1 writer, as before this format
        {
            // Row keys of that era: 8-byte order-preserving integers.
            Span<byte> key = stackalloc byte[8];
            for (int rowId = 1; rowId <= 500; rowId++)
            {
                InternalKey.EncodeInt64UserKey(key, rowId);
                legacy.Put(key, (ulong)rowId, Row(rowId));
            }

            InternalKey.EncodeInt64UserKey(key, 7);
            legacy.Delete(key, 600);
            legacy.Put(LsmRowKeyCodec.TableMetadataKey, 600, BitConverter.GetBytes(501L)); // pre-count 8-byte form
            legacy.FlushActiveMemTable();
        }

        Assert.All(Directory.GetFiles(tableDirectory, "*.sst"), file => Assert.False(SsTableV2Format.IsV2File(file)));

        using var engine = new LsmStorageEngine(_root, Relaxed);

        Assert.All(Directory.GetFiles(tableDirectory, "*.sst"), file => Assert.True(SsTableV2Format.IsV2File(file)));
        Assert.Equal(Row(500), engine.ReadRow(Db, Table, 500));
        Assert.Throws<RowDeletedException>(() => engine.ReadRow(Db, Table, 7));
        Assert.Equal(499, engine.EstimateRowCount(Db, Table)); // counted once, the legacy metadata had no count
        Assert.Equal(501, engine.InsertRow(Db, Table, Row(501)));

        // The upgrade stored the count in the file's properties: later opens do not recount.
        string upgraded = Assert.Single(Directory.GetFiles(tableDirectory, "*.sst"));
        using SsTableV2Reader reader = SsTableV2Reader.Open(upgraded, LsmReadMode.PositionalRead);
        Assert.Equal(499, reader.Properties.LiveRowCount);
        Assert.Equal(501, reader.Properties.NextRowId);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ConcurrentReaders_DuringFlushEvictionAndCompaction_AlwaysSeeACompleteVersion(LsmReadMode mode)
    {
        using var engine = new LsmStorageEngine(_root, Relaxed, mode) { MemTableFlushThresholdOverrideBytes = 64 * 1024 };
        engine.InsertRows(Db, Table, [.. Enumerable.Range(1, 2_000).Select(i => Versioned(i, 0))]);

        using var stop = new CancellationTokenSource();
        Exception? readerFailure = null;
        Thread[] readers = [.. Enumerable.Range(0, 4).Select(seed => new Thread(() =>
        {
            var random = new Random(seed);
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    long rowId = random.Next(1, 2_001);
                    string text = Encoding.UTF8.GetString(engine.ReadRow(Db, Table, rowId));
                    Assert.StartsWith($"row-{rowId}-v", text);
                }
            }
            catch (Exception ex)
            {
                readerFailure = ex;
            }
        }))];

        foreach (Thread reader in readers)
        {
            reader.Start();
        }

        // Replace every row (delete + reinsert under a new row id); small MemTables force flushes,
        // evictions, and compactions while the readers run.
        for (int rowId = 1; rowId <= 2_000; rowId++)
        {
            engine.DeleteRow(Db, Table, rowId);
            Assert.True(engine.InsertRow(Db, Table, Versioned(rowId, 1)) > 2_000);
        }

        engine.CompactTable(Db, Table);

        stop.Cancel();
        foreach (Thread reader in readers)
        {
            reader.Join();
        }

        // Readers may observe deleted rows (RowDeletedException) while rows move; anything else is a bug.
        Assert.True(readerFailure is null or RowDeletedException, readerFailure?.ToString());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ConcurrentReaders_OfStableRows_NeverFailWhileOtherRowsFlush(LsmReadMode mode)
    {
        using var engine = new LsmStorageEngine(_root, Relaxed, mode) { MemTableFlushThresholdOverrideBytes = 32 * 1024 };
        List<RowKey> stable = engine.InsertRows(Db, Table, [.. Enumerable.Range(1, 1_000).Select(Row)]);

        using var stop = new CancellationTokenSource();
        Exception? readerFailure = null;
        long reads = 0;
        Thread[] readers = [.. Enumerable.Range(0, 4).Select(seed => new Thread(() =>
        {
            var random = new Random(seed);
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    RowKey rowId = stable[random.Next(stable.Count)];
                    Assert.Equal(Row((int)(long)rowId), engine.ReadRow(Db, Table, rowId));
                    Interlocked.Increment(ref reads);
                }
            }
            catch (Exception ex)
            {
                readerFailure = ex;
            }
        }))];

        foreach (Thread reader in readers)
        {
            reader.Start();
        }

        for (int batch = 0; batch < 40; batch++)
        {
            engine.InsertRows(Db, Table, [.. Enumerable.Range(0, 500).Select(i => Row(100_000 + i))]);
            if (batch % 10 == 9)
            {
                engine.CompactTable(Db, Table);
            }
        }

        stop.Cancel();
        foreach (Thread reader in readers)
        {
            reader.Join();
        }

        Assert.Null(readerFailure);
        Assert.True(Interlocked.Read(ref reads) > 0);
        Assert.Equal(21_000, engine.EstimateRowCount(Db, Table));
    }

    private static byte[] Row(int rowId) => Encoding.UTF8.GetBytes($"row-{rowId}-payload");

    private static byte[] Versioned(int rowId, int version) => Encoding.UTF8.GetBytes($"row-{rowId}-v{version}");
}
