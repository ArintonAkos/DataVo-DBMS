using DataVo.Core;
using System.Text;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

/// <summary>Persisted index tables inside the LSM engine (Phase 2 of the disk-resident read path).</summary>
public sealed class LsmIndexTableTests : IDisposable
{
    private const string Db = "db";
    private const string Table = "items";
    private const string Index = "ix_code";
    private const LsmWalDurabilityMode Relaxed = LsmWalDurabilityMode.RelaxedOsBuffer;
    private readonly string _root = TestStorageDirectories.Create("lsm_index_tables");

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
    public void PutLookupDelete_WithinAndAcrossWriteUnits(LsmReadMode mode)
    {
        using var engine = new LsmStorageEngine(_root, Relaxed, mode);
        byte[] seven = LsmIndexKeyCodec.EncodeInteger(7);

        engine.IndexPut(Db, Table, Index, seven, 1);
        engine.IndexPut(Db, Table, Index, seven, 2);
        engine.IndexPut(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(8), 3);
        Assert.Equal([1L, 2L], engine.IndexLookup(Db, Table, Index, seven));

        using (var batch = engine.BeginWriteBatch())
        {
            engine.IndexDelete(Db, Table, Index, seven, 1);
            Assert.Equal([2L], engine.IndexLookup(Db, Table, Index, seven)); // own staged change is visible
            batch.Complete();
        }

        Assert.Equal([2L], engine.IndexLookup(Db, Table, Index, seven));
        Assert.Empty(engine.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(9)));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Entries_SurviveCleanShutdown_AndDirtyShutdown(LsmReadMode mode)
    {
        using (var engine = new LsmStorageEngine(_root, Relaxed, mode))
        {
            engine.IndexPut(Db, Table, Index, LsmIndexKeyCodec.EncodeString("alpha"), 10);
        }

        // Dirty shutdown: never disposed, so the entry exists only in the commit log.
        var abandoned = new LsmStorageEngine(_root, Relaxed, mode);
        abandoned.IndexPut(Db, Table, Index, LsmIndexKeyCodec.EncodeString("beta"), 11);
        abandoned.IndexDelete(Db, Table, Index, LsmIndexKeyCodec.EncodeString("alpha"), 10);
        abandoned = null;
        GC.Collect();
        GC.WaitForPendingFinalizers();

        using var reopened = new LsmStorageEngine(_root, Relaxed, mode);
        Assert.Empty(reopened.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeString("alpha")));
        Assert.Equal([11L], reopened.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeString("beta")));
    }

    [Fact]
    public void RowAndIndexChanges_CommitInOneFrame()
    {
        using (var engine = new LsmStorageEngine(_root, Relaxed))
        {
            using (var batch = engine.BeginWriteBatch())
            {
                RowKey rowId = engine.InsertRow(Db, Table, Encoding.UTF8.GetBytes("row"));
                engine.IndexPut(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(42), rowId);
                batch.Complete();
            }

            using (var failed = engine.BeginWriteBatch())
            {
                RowKey rowId = engine.InsertRow(Db, Table, Encoding.UTF8.GetBytes("discarded"));
                engine.IndexPut(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(43), rowId);
                // not completed: the statement failed, nothing may persist
            }
        }

        using var reopened = new LsmStorageEngine(_root, Relaxed);
        Assert.Equal([1L], reopened.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(42)));
        Assert.Empty(reopened.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(43)));
        Assert.Equal(1, reopened.EstimateRowCount(Db, Table));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ManyFlushes_EvictAndCompact_WithoutLosingEntries(LsmReadMode mode)
    {
        using (var engine = new LsmStorageEngine(_root, Relaxed, mode) { MemTableFlushThresholdOverrideBytes = 16 * 1024 })
        {
            for (long rowId = 1; rowId <= 6_000; rowId++)
            {
                engine.IndexPut(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(rowId % 100), rowId);
            }

            for (long rowId = 1; rowId <= 6_000; rowId += 2)
            {
                engine.IndexDelete(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(rowId % 100), rowId);
            }

            // Key 42 holds even row ids only (kept), key 41 odd ones only (all deleted).
            Assert.Equal(60, engine.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(42)).Count);
            Assert.Empty(engine.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(41)));
        }

        using var reopened = new LsmStorageEngine(_root, Relaxed, mode);
        List<RowKey> rows = reopened.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(42));
        Assert.Equal(60, rows.Count);
        Assert.All(rows, rowId => Assert.Equal(42, (long)rowId % 100));
        Assert.Equal(3_000, reopened.IndexEntries(Db, Table, Index).Count);
    }

    [Fact]
    public void CommitLogPruning_KeepsFramesWhoseIndexEntriesAreNotFlushed()
    {
        var abandoned = new LsmStorageEngine(_root, Relaxed);
        using (var batch = abandoned.BeginWriteBatch())
        {
            RowKey rowId = abandoned.InsertRow(Db, Table, Encoding.UTF8.GetBytes("row"));
            abandoned.IndexPut(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(5), rowId);
            batch.Complete();
        }

        abandoned.FlushTableForTesting(Db, Table); // the row is flushed (and the log pruned), the index entry is not
        abandoned = null;
        GC.Collect();
        GC.WaitForPendingFinalizers();

        using var reopened = new LsmStorageEngine(_root, Relaxed);
        Assert.Equal([1L], reopened.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(5)));
    }

    [Fact]
    public void BuildIndex_ReplacesContents_AndMarksTheIndexBuilt()
    {
        using (var engine = new LsmStorageEngine(_root, Relaxed))
        {
            Assert.False(engine.IsIndexBuilt(Db, Table, Index));
            engine.IndexPut(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(1), 99); // replaced by the build
            engine.BuildIndex(Db, Table, Index, Enumerable.Range(1, 1_000).Select(i => (LsmIndexKeyCodec.EncodeInteger(i % 10), (RowKey)i)));
            Assert.True(engine.IsIndexBuilt(Db, Table, Index));
            Assert.Equal(100, engine.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(3)).Count);
            Assert.DoesNotContain(99L, engine.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(1)));

            engine.IndexPut(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(3), 5_000);
        }

        using (var reopened = new LsmStorageEngine(_root, Relaxed))
        {
            Assert.True(reopened.IsIndexBuilt(Db, Table, Index));
            Assert.Equal(101, reopened.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(3)).Count);
            reopened.DropIndexTable(Db, Table, Index);
            Assert.False(reopened.IsIndexBuilt(Db, Table, Index));
            Assert.Empty(reopened.IndexLookup(Db, Table, Index, LsmIndexKeyCodec.EncodeInteger(3)));
        }
    }
}
