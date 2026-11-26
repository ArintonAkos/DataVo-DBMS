using System.Text;
using DataVo.Core;
using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Rows stored under 16-byte keys the caller chooses (GUIDs, negative and very large integers): the
/// keys a clustered primary key produces. They live in the sparse part of the overlay, in v6 SSTables,
/// and in version 2 commit-log frames.
/// </summary>
public sealed class LsmWideRowKeyTests : IDisposable
{
    private const string Db = "db";
    private const string Table = "t";
    private const LsmWalDurabilityMode Relaxed = LsmWalDurabilityMode.RelaxedOsBuffer;

    private static readonly RowKey[] Keys =
    [
        long.MinValue,
        -42,
        3,
        LsmLatestRowStore.DenseLimit + 5,
        long.MaxValue,
        RowKey.FromGuid(Guid.Parse("018f2c1e-7b3a-7cde-8f00-123456789abc")),
        RowKey.FromGuid(Guid.Parse("f47ac10b-58cc-4372-a567-0e02b2c3d479")),
        RowKey.FromGuid(Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")),
    ];

    private readonly string _root = TestStorageDirectories.Create("lsm_wide_keys");

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

    [Fact]
    public void ChosenKeys_AreReadFromTheOverlay_TheSstables_AndAfterReopen()
    {
        using (var engine = new LsmStorageEngine(_root, Relaxed))
        {
            InsertAll(engine);
            AssertAll(engine, "overlay");
            Assert.Equal(Keys.Order(), engine.ReadAllRows(Db, Table).Select(row => row.RowId));

            engine.FlushTableForTesting(Db, Table);
            Assert.Equal(0, engine.OverlayRowCount(Db, Table));
            AssertAll(engine, "sstable");
            Assert.Equal(Keys.Order(), engine.ReadAllRows(Db, Table).Select(row => row.RowId));
        }

        string file = Assert.Single(Directory.GetFiles(Path.Combine(_root, Db, Table), "*.sst"));
        using (SsTableV2Reader reader = SsTableV2Reader.Open(file, LsmReadMode.PositionalRead))
        {
            Assert.Equal(SsTableV2Format.WideVersion, reader.Version);
        }

        using var reopened = new LsmStorageEngine(_root, Relaxed);
        AssertAll(reopened, "reopen");
        Assert.Equal(Keys.Length, reopened.EstimateRowCount(Db, Table));
    }

    [Fact]
    public void ChosenKeys_SurviveACrash_ThroughCommitLogReplay()
    {
        var engine = new LsmStorageEngine(_root, Relaxed);
        InsertAll(engine);
        engine.DeleteRow(Db, Table, Keys[1]);
        engine.SimulateCrashForTesting();

        using var recovered = new LsmStorageEngine(_root, Relaxed);
        foreach (RowKey key in Keys)
        {
            if (key == Keys[1])
            {
                Assert.Throws<RowDeletedException>(() => recovered.ReadRow(Db, Table, key)); // its tombstone replayed
            }
            else
            {
                Assert.Equal(Value(key), recovered.ReadRow(Db, Table, key));
            }
        }

        Assert.Equal(Keys.Length - 1, recovered.EstimateRowCount(Db, Table));
    }

    [Fact]
    public void DuplicateKey_IsRejected_WhetherCommittedOrStagedInTheSameBatch()
    {
        using var engine = new LsmStorageEngine(_root, Relaxed);
        RowKey guid = Keys[5];
        Assert.True(engine.TryInsertRowWithKey(Db, Table, guid, Value(guid)));
        Assert.False(engine.TryInsertRowWithKey(Db, Table, guid, "other"u8.ToArray()));

        using (IWriteBatchScope scope = engine.BeginWriteBatch())
        {
            Assert.True(engine.TryInsertRowWithKey(Db, Table, -1, "a"u8.ToArray()));
            Assert.False(engine.TryInsertRowWithKey(Db, Table, -1, "b"u8.ToArray()));
            scope.Complete();
        }

        Assert.Equal("a"u8.ToArray(), engine.ReadRow(Db, Table, -1));
        Assert.Equal(Value(guid), engine.ReadRow(Db, Table, guid));

        // A deleted key can be used again.
        engine.DeleteRow(Db, Table, guid);
        Assert.True(engine.TryInsertRowWithKey(Db, Table, guid, "again"u8.ToArray()));
        Assert.Equal("again"u8.ToArray(), engine.ReadRow(Db, Table, guid));
    }

    [Fact]
    public void EngineAssignedIds_SkipPastChosenIntegerKeys_AfterReopenToo()
    {
        using (var engine = new LsmStorageEngine(_root, Relaxed))
        {
            Assert.True(engine.TryInsertRowWithKey(Db, Table, 100, "chosen"u8.ToArray()));
            Assert.Equal((RowKey)101, engine.InsertRow(Db, Table, "assigned"u8.ToArray()));
            Assert.True(engine.TryInsertRowWithKey(Db, Table, -5, "negative"u8.ToArray()));
            Assert.Equal((RowKey)102, engine.InsertRow(Db, Table, "assigned"u8.ToArray()));
        }

        using var reopened = new LsmStorageEngine(_root, Relaxed);
        Assert.Equal((RowKey)103, reopened.InsertRow(Db, Table, "assigned"u8.ToArray()));
    }

    [Fact]
    public void Compaction_MergesManyFilesOfChosenKeys_AndDropsDeletedOnes()
    {
        var random = new Random(5);
        var expected = new SortedDictionary<RowKey, byte[]>();
        using (var engine = new LsmStorageEngine(_root, Relaxed) { MemTableFlushThresholdOverrideBytes = 8 * 1024 })
        {
            for (int round = 0; round < 20; round++)
            {
                for (int i = 0; i < 200; i++)
                {
                    RowKey key = RandomKey(random);
                    if (engine.TryInsertRowWithKey(Db, Table, key, Value(key)))
                    {
                        expected[key] = Value(key);
                    }
                }

                foreach (RowKey key in expected.Keys.Where(_ => random.Next(10) == 0).ToList())
                {
                    engine.DeleteRow(Db, Table, key);
                    expected.Remove(key);
                }

                engine.FlushTableForTesting(Db, Table);
            }

            List<(RowKey NewRowId, byte[] RawRow)> compacted = engine.CompactTable(Db, Table);
            Assert.Equal(expected.Keys, compacted.Select(row => row.NewRowId));
        }

        using var reopened = new LsmStorageEngine(_root, Relaxed);
        Assert.Equal(expected.Keys, reopened.ReadAllRows(Db, Table).Select(row => row.RowId));
        foreach ((RowKey key, byte[] value) in expected)
        {
            Assert.Equal(value, reopened.ReadRow(Db, Table, key));
        }
    }

    private static RowKey RandomKey(Random random) => random.Next(3) switch
    {
        0 => RowKey.FromGuid(Guid.NewGuid()),
        1 => -random.NextInt64(1, long.MaxValue),
        _ => random.NextInt64(LsmLatestRowStore.DenseLimit, long.MaxValue),
    };

    private static void InsertAll(LsmStorageEngine engine)
    {
        foreach (RowKey key in Keys)
        {
            Assert.True(engine.TryInsertRowWithKey(Db, Table, key, Value(key)));
        }
    }

    private static void AssertAll(LsmStorageEngine engine, string where)
    {
        foreach (RowKey key in Keys)
        {
            Assert.True(Value(key).AsSpan().SequenceEqual(engine.ReadRow(Db, Table, key)), $"{key} from the {where}");
        }
    }

    private static byte[] Value(RowKey key) => Encoding.UTF8.GetBytes($"row-{key}");
}
