using DataVo.Core;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Tests.Lsm;

/// <summary>
/// A columnar scan of an LSM table reads the columns of flushed rows from the SSTables' columnar segments and
/// only the unflushed rows from memory. These tests drive the storage engine directly and compare every scan
/// with a model of the rows: across inserts, updates and deletes, flushes, compaction and reopening, in both
/// read modes, and while another thread commits.
/// </summary>
public sealed class LsmOutOfCoreColumnarScanTests : IDisposable
{
    private const string Db = "db";
    private const string Table = "players";
    private const LsmWalDurabilityMode Relaxed = LsmWalDurabilityMode.RelaxedOsBuffer;

    private static readonly Column[] Columns =
    [
        new() { Name = "Level", Type = "INT" },
        new() { Name = "Gold", Type = "BIGINT" },
        new() { Name = "Name", Type = "VARCHAR" },
    ];

    private static readonly RowSerializer.StorageColumnType[] Kinds = [.. Columns.Select(static c => RowSerializer.ClassifyColumnType(c.Type))];

    private readonly string _root = TestStorageDirectories.Create("lsm_out_of_core_columnar");

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

    private static byte[] Row(int level, long? gold) => RowSerializer.SerializeCells(
        Columns,
        [CellValue.From(level), gold is { } value ? CellValue.From(value) : CellValue.Null, CellValue.From("p" + level)]);

    private static ColumnarIndex Build(LsmStorageEngine engine) => engine.BuildColumnar(Db, Table, Kinds, [0, 1], schemaVersion: 1, keyOrdinal: -1);

    /// <summary>What one scan answers: the rows with <c>Level &gt;= minLevel</c>, counted and folded.</summary>
    private readonly record struct Answer(int Count, long Levels, long Gold, int GoldValues, long MinGold, long MaxGold);

    private static bool TryScan(LsmStorageEngine engine, int minLevel, out Answer answer, int limit = 0)
    {
        ColumnarIndex index = engine.Columnar(Db, Table)!;
        ColumnarStep[] steps = minLevel == int.MinValue
            ? []
            : [new ColumnarStep { Kind = ColumnarStep.Compare, Column = 0, Test = ColumnarTest.GreaterOrEqual, ValueKind = 'i', Bits = minLevel }];
        Assert.True(ColumnarScan.TryBind(index, steps));
        ColumnarFold[] folds =
        [
            new() { Slot = 0, Kind = RowFoldKind.SumInt32 },
            new() { Slot = 1, Kind = RowFoldKind.SumInt64 },
            new() { Slot = 1, Kind = RowFoldKind.MinInt64 },
            new() { Slot = 1, Kind = RowFoldKind.MaxInt64 },
        ];
        var totals = new RowAggregate[folds.Length];
        bool answered = engine.TryScanColumnar(Db, Table, index, steps, folds, totals, limit, out int matched);
        answer = new Answer(matched, totals[0].Whole, totals[1].Whole, totals[1].Values, totals[2].Whole, totals[3].Whole);
        return answered;
    }

    private static Answer Expected(IEnumerable<(int Level, long? Gold)> rows, int minLevel)
    {
        List<(int Level, long? Gold)> kept = [.. rows.Where(row => row.Level >= minLevel)];
        List<long> gold = [.. kept.Where(static row => row.Gold is not null).Select(static row => row.Gold!.Value)];
        return new Answer(kept.Count, kept.Sum(static row => (long)row.Level), gold.Sum(), gold.Count, gold.Count == 0 ? 0 : gold.Min(), gold.Count == 0 ? 0 : gold.Max());
    }

    private static void AssertScans(LsmStorageEngine engine, Dictionary<RowKey, (int Level, long? Gold)> model)
    {
        foreach (int minLevel in (int[])[int.MinValue, 0, 50, 1_000_000])
        {
            Assert.True(TryScan(engine, minLevel, out Answer answer));
            Assert.Equal(Expected(model.Values, minLevel), answer);
        }

        // A plain count reads no column at all.
        ColumnarIndex index = engine.Columnar(Db, Table)!;
        Assert.True(engine.TryScanColumnar(Db, Table, index, default, default, default, 0, out int count));
        Assert.Equal(model.Count, count);
        Assert.True(engine.TryScanColumnar(Db, Table, index, default, default, default, 3, out int limited));
        Assert.Equal(Math.Min(3, model.Count), limited);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void RandomWrites_Flushes_Compactions_AndReopens_KeepEveryScanEqualToTheRows(LsmReadMode mode)
    {
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        var random = new Random(mode == LsmReadMode.MemoryMapped ? 21 : 22);
        LsmStorageEngine engine = Open(mode);
        try
        {
            Build(engine);
            for (int step = 0; step < 300; step++)
            {
                Mutate(engine, model, random, 1 + random.Next(120));
                switch (random.Next(12))
                {
                    case 0 or 1 or 2:
                        engine.FlushTableForTesting(Db, Table);
                        break;
                    case 3 when step % 4 == 0:
                        engine.CompactTable(Db, Table);
                        break;
                    case 4 when step % 5 == 0:
                        engine.Dispose();
                        engine = Open(mode);
                        Assert.Null(engine.Columnar(Db, Table));
                        Build(engine);
                        break;
                }

                if (step % 3 == 0)
                {
                    AssertScans(engine, model);
                }
            }

            AssertScans(engine, model);
            Assert.Equal(model.Count, engine.EstimateRowCount(Db, Table));
        }
        finally
        {
            engine.Dispose();
        }
    }

    [Fact]
    public void FilesWrittenBeforeTheIndex_AreRewrittenOnce_AndKeepTheirRowsAndOrder()
    {
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        var random = new Random(31);
        using (LsmStorageEngine engine = Open(LsmReadMode.Auto))
        {
            // Three level-0 files, each updating rows of the one before, and unflushed rows on top.
            for (int file = 0; file < 3; file++)
            {
                Mutate(engine, model, random, 2_500);
                engine.FlushTableForTesting(Db, Table);
            }

            Mutate(engine, model, random, 300);
            Assert.All(SstVersions(engine), static version => Assert.Equal(SsTableV2Format.WideVersion, version));

            Build(engine);
            Assert.All(SstVersions(engine), static version => Assert.Equal(SsTableV2Format.ColumnarVersion, version));
            AssertScans(engine, model);
            AssertRowsReadAsModel(engine, model);

            // Nothing is left to rewrite: building again touches no file.
            string[] files = SstFiles(engine);
            Build(engine);
            Assert.Equal(files, SstFiles(engine));
        }

        using (LsmStorageEngine engine = Open(LsmReadMode.Auto))
        {
            string[] files = SstFiles(engine);
            ColumnarIndex tail = Build(engine);
            Assert.Equal(0, tail.Count); // closing flushed every row: nothing is built in memory
            Assert.Equal(files, SstFiles(engine)); // every file already has its segment
            AssertScans(engine, model);
            AssertRowsReadAsModel(engine, model);
        }
    }

    [Fact]
    public void AFileWithoutItsSegment_SendsTheQueryToRows_AndAsksForARebuild()
    {
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        using LsmStorageEngine engine = Open(LsmReadMode.Auto);
        ColumnarIndex tail = Build(engine);
        Mutate(engine, model, new Random(5), 500);

        // A flush that ran without the spec, as one that was already under way when the index was created.
        engine.SetColumnarSpec(Db, Table, null);
        engine.FlushTableForTesting(Db, Table);
        engine.SetColumnarSpec(Db, Table, new ColumnarSpec(Kinds, [0, 1], -1));

        tail = engine.Columnar(Db, Table)!;
        Assert.False(tail.WantsRebuild);
        Assert.False(TryScan(engine, int.MinValue, out _));
        Assert.True(tail.WantsRebuild);

        Build(engine);
        AssertScans(engine, model);
    }

    [Fact]
    public void ADeleteThatIsNotFlushedYet_KeepsHidingTheFilesRow_AfterALaterFlush()
    {
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        using LsmStorageEngine engine = Open(LsmReadMode.Auto);
        engine.MemTableFlushThresholdOverrideBytes = null;
        Build(engine);
        List<RowKey> ids = engine.InsertRows(Db, Table, [.. Enumerable.Range(0, 100).Select(static i => Row(i, i))]);
        for (int i = 0; i < ids.Count; i++)
        {
            model[ids[i]] = (i, i);
        }

        engine.FlushTableForTesting(Db, Table);

        // A delete and an update of rows that live in the file: only the part in memory knows of them.
        engine.DeleteRow(Db, Table, ids[10]);
        model.Remove(ids[10]);
        Assert.True(engine.OverwriteRowForTesting(Db, Table, ids[20], Row(999, null)));
        model[ids[20]] = (999, null);
        AssertScans(engine, model);
        Assert.Equal(1, engine.Columnar(Db, Table)!.DeletedCount);

        engine.FlushTableForTesting(Db, Table); // the delete and the update are now in a second file
        Assert.Equal(0, engine.Columnar(Db, Table)!.DeletedCount);
        AssertScans(engine, model);

        // A delete made while an earlier one is still unflushed survives the tail being started again.
        engine.DeleteRow(Db, Table, ids[30]);
        model.Remove(ids[30]);
        engine.BuildColumnar(Db, Table, Kinds, [0, 1], schemaVersion: 1, keyOrdinal: -1);
        Assert.Equal(1, engine.Columnar(Db, Table)!.DeletedCount);
        AssertScans(engine, model);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ScansThatRaceCommitsAndFlushes_SeeWholeCommitsOnly(LsmReadMode mode)
    {
        const int Accounts = 3_000;
        const long Total = Accounts * 1_000L;
        using LsmStorageEngine engine = Open(mode, flushThreshold: 64 * 1024);
        Build(engine);
        List<RowKey> ids = engine.InsertRows(Db, Table, [.. Enumerable.Range(0, Accounts).Select(static i => Row(i, 1_000))]);
        long[] balance = [.. Enumerable.Repeat(1_000L, Accounts)];

        using var stop = new CancellationTokenSource();
        Task writer = Task.Run(() =>
        {
            // Each commit moves gold between two rows, so the table's total never changes.
            var random = new Random(77);
            while (!stop.IsCancellationRequested)
            {
                int from = random.Next(Accounts);
                int to = (from + 1 + random.Next(Accounts - 1)) % Accounts;
                long amount = random.Next(1, 50);
                using IWriteBatchScope batch = engine.BeginWriteBatch();
                engine.OverwriteRowForTesting(Db, Table, ids[from], Row(from, balance[from] -= amount));
                engine.OverwriteRowForTesting(Db, Table, ids[to], Row(to, balance[to] += amount));
                batch.Complete();
            }
        });

        int answered = 0;
        try
        {
            for (int scan = 0; scan < 400; scan++)
            {
                if (engine.Columnar(Db, Table) is { WantsRebuild: true })
                {
                    Build(engine); // what a query does when the part in memory is mostly dead positions
                }

                if (TryScan(engine, int.MinValue, out Answer answer))
                {
                    answered++;
                    Assert.Equal(Accounts, answer.Count);
                    Assert.Equal(Total, answer.Gold);
                }
            }
        }
        finally
        {
            stop.Cancel();
            writer.Wait();
        }

        Assert.True(answered > 100, $"Only {answered} of 400 scans were answered from columns.");
    }

    private LsmStorageEngine Open(LsmReadMode mode, long flushThreshold = 96 * 1024) =>
        new(_root, Relaxed, mode) { MemTableFlushThresholdOverrideBytes = flushThreshold };

    // Inserts, rewrites and deletes rows, the same way in the engine and in the model.
    private static void Mutate(LsmStorageEngine engine, Dictionary<RowKey, (int Level, long? Gold)> model, Random random, int operations)
    {
        for (int i = 0; i < operations; i++)
        {
            (int Level, long? Gold) row = (random.Next(-20, 120), random.Next(7) == 0 ? null : random.NextInt64(-1_000_000, 1_000_000));
            int action = random.Next(10);
            if (model.Count == 0 || action < 5)
            {
                model[engine.InsertRow(Db, Table, Row(row.Level, row.Gold))] = row;
            }
            else
            {
                RowKey id = model.Keys.ElementAt(random.Next(model.Count));
                if (action < 8)
                {
                    Assert.True(engine.OverwriteRowForTesting(Db, Table, id, Row(row.Level, row.Gold)));
                    model[id] = row;
                }
                else
                {
                    engine.DeleteRow(Db, Table, id);
                    model.Remove(id);
                }
            }
        }
    }

    private static void AssertRowsReadAsModel(LsmStorageEngine engine, Dictionary<RowKey, (int Level, long? Gold)> model)
    {
        Dictionary<RowKey, byte[]> rows = engine.ReadAllRows(Db, Table).ToDictionary(static row => row.RowId, static row => row.RawRow);
        Assert.Equal(model.Count, rows.Count);
        foreach ((RowKey id, (int level, long? gold)) in model)
        {
            Assert.Equal(Row(level, gold), rows[id]);
            Assert.Equal(Row(level, gold), engine.ReadRow(Db, Table, id));
        }
    }

    private string[] SstFiles(LsmStorageEngine engine)
    {
        _ = engine.ReadAllRows(Db, Table).Count(); // drops the engine's hold on replaced files
        return [.. Directory.GetFiles(_root, "*.sst", SearchOption.AllDirectories).Order()];
    }

    private List<ushort> SstVersions(LsmStorageEngine engine)
    {
        var versions = new List<ushort>();
        foreach (string path in SstFiles(engine))
        {
            using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead);
            versions.Add(reader.Version);
        }

        return versions;
    }
}
