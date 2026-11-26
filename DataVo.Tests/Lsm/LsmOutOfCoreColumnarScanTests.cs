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
public sealed partial class LsmOutOfCoreColumnarScanTests : IDisposable
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
    public async Task ScansThatRaceCommitsAndFlushes_SeeWholeCommitsOnly(LsmReadMode mode)
    {
        const int Accounts = 3_000;
        const long Total = Accounts * 1_000L;
        using LsmStorageEngine engine = Open(mode, flushThreshold: 64 * 1024);
        Build(engine);
        List<RowKey> ids = engine.InsertRows(Db, Table, [.. Enumerable.Range(0, Accounts).Select(static i => Row(i, 1_000))]);
        long[] balance = [.. Enumerable.Repeat(1_000L, Accounts)];

        const int TargetAnswered = 150;
        const int MaxAttempts = 4_000;
        long commits = 0;
        using var writerStarted = new ManualResetEventSlim(false);
        using var stop = new CancellationTokenSource();

        // Run the writer on a dedicated thread and wait for its first commit before scanning, so ThreadPool
        // saturation during parallel test runs cannot let all scans finish before the writer even starts.
        Task writer = Task.Factory.StartNew(() =>
        {
            try
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
                    if (Interlocked.Increment(ref commits) == 1)
                    {
                        writerStarted.Set();
                    }
                }
            }
            finally
            {
                writerStarted.Set();
            }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        int answered = 0;
        int attempts = 0;
        try
        {
            Assert.True(writerStarted.Wait(TimeSpan.FromSeconds(10)), "Writer thread did not start committing in time.");

            // Run until enough consistent columnar scans succeed under live writes and flushes, without depending
            // on a fixed single-call success ratio under CI vCPU contention.
            while (answered < TargetAnswered && attempts < MaxAttempts)
            {
                attempts++;
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
            await writer;
        }

        Assert.True(Volatile.Read(ref commits) > 0, "Expected concurrent commits while scanning.");
        Assert.True(answered >= TargetAnswered, $"Only {answered} of {attempts} scans were answered from columns.");
    }

    // ---- Zone-map pruning -------------------------------------------------------------------------------------
    // A table of three chunk groups with disjoint zones, flushed to one file:
    //   group 0: Level 0..4095,      Temp (DOUBLE) negative, Ratio (FLOAT) negative
    //   group 1: Level 4096..8191,   Temp 10..49 with a NaN, Ratio positive with a NaN
    //   group 2: Level 8192..12287,  Temp all NULL,           Ratio 1000+
    private const string Readings = "readings";
    private const int GroupRows = 4096;

    private static readonly Column[] ReadingColumns =
    [
        new() { Name = "Level", Type = "INT" },
        new() { Name = "Temp", Type = "DOUBLE" },
        new() { Name = "Ratio", Type = "FLOAT" },
    ];

    private static readonly RowSerializer.StorageColumnType[] ReadingKinds = [.. ReadingColumns.Select(static c => RowSerializer.ClassifyColumnType(c.Type))];

    private readonly record struct Reading(int Level, double? Temp, float Ratio);

    private static Reading ReadingAt(int i) => (i / GroupRows) switch
    {
        0 => new Reading(i, -100.5 - (i * 0.01), -1f - (i * 0.001f)),
        1 => new Reading(i, i == GroupRows + 5 ? double.NaN : 10 + (i % 40), i == GroupRows + 7 ? float.NaN : 1f + (i % 9)),
        _ => new Reading(i, null, 1000f + (i % 100)),
    };

    private static byte[] ReadingRow(Reading r) => RowSerializer.SerializeCells(
        ReadingColumns,
        [CellValue.From(r.Level), r.Temp is { } t ? CellValue.From(t) : CellValue.Null, CellValue.From((double)r.Ratio)]);

    private LsmStorageEngine OpenReadings(out List<Reading> rows, LsmReadMode mode = LsmReadMode.Auto)
    {
        LsmStorageEngine engine = Open(mode, flushThreshold: 1L << 30);
        engine.BuildColumnar(Db, Readings, ReadingKinds, [0, 1, 2], schemaVersion: 1, keyOrdinal: -1);
        rows = [.. Enumerable.Range(0, 3 * GroupRows).Select(ReadingAt)];
        engine.InsertRows(Db, Readings, [.. rows.Select(ReadingRow)]);
        engine.FlushTableForTesting(Db, Readings);
        return engine;
    }

    private static ColumnarStep Cmp(int column, ColumnarTest test, int value) =>
        new() { Kind = ColumnarStep.Compare, Column = column, Test = test, ValueKind = 'i', Bits = value, NullsMatch = test == ColumnarTest.NotEqual };

    private static ColumnarStep Cmp(int column, ColumnarTest test, double value, bool? nullsMatch = null) =>
        new() { Kind = ColumnarStep.Compare, Column = column, Test = test, ValueKind = 'd', Bits = BitConverter.DoubleToInt64Bits(value), NullsMatch = nullsMatch ?? test == ColumnarTest.NotEqual };

    private static ColumnarStep Cmp(int column, ColumnarTest test, float value) =>
        new() { Kind = ColumnarStep.Compare, Column = column, Test = test, ValueKind = 'f', Bits = BitConverter.SingleToInt32Bits(value), NullsMatch = test == ColumnarTest.NotEqual };

    private static ColumnarStep Null(int column, bool isNull) =>
        new() { Kind = ColumnarStep.NullTest, Column = column, Test = isNull ? ColumnarTest.Equal : ColumnarTest.NotEqual };

    private static readonly ColumnarStep AndStep = new() { Kind = ColumnarStep.And };
    private static readonly ColumnarStep OrStep = new() { Kind = ColumnarStep.Or };

    // Scans the readings table; returns the matches, the sum of Level over them, and how many chunk groups were read.
    private static (int Matched, long Levels, long Loads) ScanReadings(LsmStorageEngine engine, ColumnarStep[] steps, int limit = 0)
    {
        ColumnarIndex index = engine.Columnar(Db, Readings)!;
        Assert.True(ColumnarScan.TryBind(index, steps));
        ColumnarFold[] folds = [new() { Slot = 0, Kind = RowFoldKind.SumInt32 }];
        var totals = new RowAggregate[1];
        long before = SsTableColumnarChunkReader.LoadsOnThisThread;
        Assert.True(engine.TryScanColumnar(Db, Readings, index, steps, folds, totals, limit, out int matched));
        return (matched, totals[0].Whole, SsTableColumnarChunkReader.LoadsOnThisThread - before);
    }

    private static void AssertPruned(LsmStorageEngine engine, List<Reading> rows, ColumnarStep[] steps, Func<Reading, bool> model, long expectedLoads)
    {
        List<Reading> expected = [.. rows.Where(model)];
        (int matched, long levels, long loads) = ScanReadings(engine, steps);
        Assert.Equal(expected.Count, matched);
        Assert.Equal(expected.Sum(static r => (long)r.Level), levels);
        Assert.Equal(expectedLoads, loads);
    }

    [Fact]
    public void ZoneMaps_SkipChunksTheConditionCannotMatch_WithoutReadingThem()
    {
        using LsmStorageEngine engine = OpenReadings(out List<Reading> rows);
        AssertPruned(engine, rows, [Cmp(0, ColumnarTest.GreaterOrEqual, 2 * GroupRows)], static r => r.Level >= 2 * GroupRows, 1);
        AssertPruned(engine, rows, [Cmp(0, ColumnarTest.Less, 0)], static r => r.Level < 0, 0);
        AssertPruned(engine, rows, [Cmp(0, ColumnarTest.Equal, 5000)], static r => r.Level == 5000, 1);
        AssertPruned(engine, rows, [Cmp(0, ColumnarTest.NotEqual, 5000)], static r => r.Level != 5000, 3);
        AssertPruned(engine, rows, [Cmp(0, ColumnarTest.Greater, 12_287)], static r => r.Level > 12_287, 0);
        AssertPruned(engine, rows, [Cmp(0, ColumnarTest.LessOrEqual, 4095)], static r => r.Level <= 4095, 1);

        // A limit stops the scan at the first chunk that fills it; skipped chunks never count towards it.
        (int matched, _, long loads) = ScanReadings(engine, [Cmp(0, ColumnarTest.GreaterOrEqual, 2 * GroupRows)], limit: 10);
        Assert.Equal(10, matched);
        Assert.Equal(1, loads);
        (matched, _, loads) = ScanReadings(engine, [Cmp(0, ColumnarTest.GreaterOrEqual, 0)], limit: 100);
        Assert.Equal(100, matched);
        Assert.Equal(1, loads);
    }

    [Fact]
    public void ZoneMaps_JoinAndOrSteps_ByWhatEachChunkCanHold()
    {
        using LsmStorageEngine engine = OpenReadings(out List<Reading> rows);

        // AND: a chunk is out as soon as one side is.
        AssertPruned(engine, rows, [Cmp(0, ColumnarTest.GreaterOrEqual, 4000), Cmp(0, ColumnarTest.Less, 4200), AndStep], static r => r.Level is >= 4000 and < 4200, 2);
        AssertPruned(engine, rows, [Cmp(0, ColumnarTest.GreaterOrEqual, 2 * GroupRows), Null(1, isNull: true), AndStep], static r => r.Level >= 2 * GroupRows && r.Temp is null, 1);
        AssertPruned(engine, rows, [Cmp(0, ColumnarTest.Less, 100), Cmp(0, ColumnarTest.GreaterOrEqual, 200), AndStep], static _ => false, 1); // group 0's zone cannot tell the two apart

        // OR: a chunk is out only when both sides are.
        AssertPruned(engine, rows, [Cmp(0, ColumnarTest.Less, 10), Cmp(0, ColumnarTest.GreaterOrEqual, 12_000), OrStep], static r => r.Level is < 10 or >= 12_000, 2);
        AssertPruned(engine, rows, [Cmp(0, ColumnarTest.Less, 100), Cmp(1, ColumnarTest.Greater, 0.0), OrStep], static r => r.Level < 100 || r.Temp > 0, 2);

        // Nested: (Level < 10 OR Level >= 12000) AND Temp IS NULL.
        AssertPruned(
            engine,
            rows,
            [Cmp(0, ColumnarTest.Less, 10), Cmp(0, ColumnarTest.GreaterOrEqual, 12_000), OrStep, Null(1, isNull: true), AndStep],
            static r => (r.Level < 10 || r.Level >= 12_000) && r.Temp is null,
            1);
    }

    [Fact]
    public void ZoneMaps_ReadNullsAndAllNullChunks_AsTheStepsNullsMatchSays()
    {
        using LsmStorageEngine engine = OpenReadings(out List<Reading> rows);

        // IS NULL / IS NOT NULL: the all-NULL group answers both without being read.
        AssertPruned(engine, rows, [Null(1, isNull: true)], static r => r.Temp is null, 1);
        AssertPruned(engine, rows, [Null(1, isNull: false)], static r => r.Temp is not null, 2);

        // SQL: a NULL meets no comparison, so the all-NULL group is out.
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.NotEqual, 1000.0, nullsMatch: false)], static r => r.Temp is not null && r.Temp != 1000.0, 2);
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.Equal, 5.0)], static _ => false, 0);

        // C#: != counts a NULL as different, so the all-NULL group is all-matching and must be read.
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.NotEqual, 1000.0, nullsMatch: true)], static r => r.Temp is null || r.Temp != 1000.0, 3);
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.Equal, 5.0, nullsMatch: true)], static r => r.Temp is null, 1);
    }

    [Fact]
    public void ZoneMaps_NeverPrune_AwayANaNThatMeetsNotEqual_AndNeverMatchANaNElsewhere()
    {
        using LsmStorageEngine engine = OpenReadings(out List<Reading> rows);

        // Group 1's zone (10..49) excludes 1000, but its NaN differs from 1000, so it must be read for !=.
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.NotEqual, 1000.0, nullsMatch: false)], static r => r.Temp is { } t && t != 1000.0, 2);
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.NotEqual, 1000.0, nullsMatch: false), Cmp(1, ColumnarTest.NotEqual, 2000.0, nullsMatch: false), AndStep], static r => r.Temp is { } t && t != 1000.0 && t != 2000.0, 2);

        // A NaN meets none of < <= > >= ==, so a zone that rules the numbers out rules the group out.
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.GreaterOrEqual, 100.0)], static r => r.Temp >= 100.0, 0);
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.LessOrEqual, -1000.0)], static r => r.Temp <= -1000.0, 0);

        // A zone that covers the group is not enough to call it all-matching when it holds a NaN: it is read.
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.Less, 100.0)], static r => r.Temp < 100.0, 2);
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.Greater, 0.0)], static r => r.Temp > 0.0, 1);
        AssertPruned(engine, rows, [Cmp(2, ColumnarTest.Less, 100f)], static r => r.Ratio < 100f, 2);
        AssertPruned(engine, rows, [Cmp(2, ColumnarTest.GreaterOrEqual, 0f), Cmp(0, ColumnarTest.Less, 4200), AndStep], static r => r.Ratio >= 0f && r.Level < 4200, 1);

        // A NaN on the right-hand side matches nothing but !=; the zones cannot say so, except the all-NULL group.
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.Less, double.NaN)], static _ => false, 2);
    }

    [Fact]
    public void ZoneMaps_OrderNegativeDoublesAndFloats_AsNumbers()
    {
        using LsmStorageEngine engine = OpenReadings(out List<Reading> rows);

        // Group 0 holds -141.45 .. -100.5; compared as raw bits the order of negatives would flip.
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.Less, -120.0)], static r => r.Temp < -120.0, 1);
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.Greater, -120.0)], static r => r.Temp > -120.0, 2);
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.Less, -141.5)], static r => r.Temp < -141.5, 0);
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.GreaterOrEqual, -100.5)], static r => r.Temp >= -100.5, 2);
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.Equal, -100.5)], static r => r.Temp == -100.5, 1);
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.Greater, -100.4)], static r => r.Temp > -100.4, 1);
        AssertPruned(engine, rows, [Cmp(1, ColumnarTest.Less, -200.0)], static r => r.Temp < -200.0, 0);

        // FLOAT zones are stored widened to double; the value arrives as a float.
        AssertPruned(engine, rows, [Cmp(2, ColumnarTest.Less, -3f)], static r => r.Ratio < -3f, 1);
        AssertPruned(engine, rows, [Cmp(2, ColumnarTest.Less, -10f)], static r => r.Ratio < -10f, 0);
        AssertPruned(engine, rows, [Cmp(2, ColumnarTest.Greater, -0.5f)], static r => r.Ratio > -0.5f, 2);
        AssertPruned(engine, rows, [Cmp(2, ColumnarTest.GreaterOrEqual, 500f)], static r => r.Ratio >= 500f, 1);
    }

    [Fact]
    public void ZoneMaps_SkipChunksOfTheFile_ButStillSeeNewerRowsInMemory()
    {
        using LsmStorageEngine engine = OpenReadings(out List<Reading> rows);
        List<RowKey> keys = [.. engine.ReadAllRows(Db, Readings).Select(static row => row.RowId).Order()];

        // Rewrite a row of group 0 so it now has a Level that no zone of the file reaches, and delete one of group 2.
        Reading moved = new(99_999, -1.0, -1f);
        Assert.True(engine.OverwriteRowForTesting(Db, Readings, keys[3], ReadingRow(moved)));
        engine.DeleteRow(Db, Readings, keys[(2 * GroupRows) + 3]);
        rows[3] = moved;
        rows.RemoveAt((2 * GroupRows) + 3);

        AssertCounts(engine, rows, [Cmp(0, ColumnarTest.GreaterOrEqual, 99_999)], static r => r.Level >= 99_999);
        AssertCounts(engine, rows, [Cmp(0, ColumnarTest.GreaterOrEqual, 2 * GroupRows)], static r => r.Level >= 2 * GroupRows);
        AssertCounts(engine, rows, [Cmp(0, ColumnarTest.Less, 10)], static r => r.Level < 10);
    }

    private static void AssertCounts(LsmStorageEngine engine, List<Reading> rows, ColumnarStep[] steps, Func<Reading, bool> model)
    {
        List<Reading> expected = [.. rows.Where(model)];
        (int matched, long levels, _) = ScanReadings(engine, steps);
        Assert.Equal(expected.Count, matched);
        Assert.Equal(expected.Sum(static r => (long)r.Level), levels);
    }

    private LsmStorageEngine Open(LsmReadMode mode, long flushThreshold = 96 * 1024, string? root = null) =>
        new(root ?? _root, Relaxed, mode) { MemTableFlushThresholdOverrideBytes = flushThreshold };

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
