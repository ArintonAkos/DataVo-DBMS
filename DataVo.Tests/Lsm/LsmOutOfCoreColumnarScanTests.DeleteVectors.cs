using DataVo.Core;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

// Rows of a file that newer files replace are kept per file as a delete vector, and the next set of files starts
// from it. Each check compares three answers: the scan (vectors started from kept ones), the vectors worked out
// from nothing, and the rows themselves (the model). Pinned sets of files stand for scans that began before a
// later flush, compaction or backfill.
public sealed partial class LsmOutOfCoreColumnarScanTests
{
    [Fact]
    public void BackfillThenFlushThenCompaction_KeepsDeleteVectorsEqualToThoseWorkedOutFromNothing()
    {
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        var random = new Random(51);
        using LsmStorageEngine engine = Open(LsmReadMode.Auto, flushThreshold: 1L << 30);
        var pins = new List<IDisposable>();
        try
        {
            // Without a columnar index: one file at the bottom level and two level-0 files that update it.
            Mutate(engine, model, random, 8_000);
            engine.FlushTableForTesting(Db, Table);
            engine.CompactTable(Db, Table);
            for (int file = 0; file < 2; file++)
            {
                UpdateMany(engine, model, random, 1_500);
                engine.FlushTableForTesting(Db, Table);
            }

            // Creating the index rewrites every file, level 0 first: the bottom file, the oldest rows, ends up
            // with the highest file number, so only sequences say which file is newer.
            Build(engine);
            (long Number, int Level)[] files = LiveFiles();
            long bottom = files.Single(static f => f.Level > 0).Number;
            Assert.All(files.Where(static f => f.Level == 0), f => Assert.True(f.Number < bottom));
            Check(engine, model, pins);
            pins.Add(engine.PinFilesForTesting(Db, Table));

            // A flush: the bottom file starts from its kept vector (both level-0 files applied, now a third).
            UpdateMany(engine, model, random, 1_500);
            engine.FlushTableForTesting(Db, Table);
            Assert.True(Check(engine, model, pins) > 0);
            pins.Add(engine.PinFilesForTesting(Db, Table));

            // Level 0 into level 1: the files the bottom file's vector applied are gone; their keys are in the new one.
            engine.CompactLevelForTesting(Db, Table, 0);
            Assert.Contains(LiveFiles(), static f => f.Level == 1);
            Assert.True(Check(engine, model, pins) > 0);
            pins.Add(engine.PinFilesForTesting(Db, Table));

            // A file flushed without the segment and rewritten by the next index build: backfill again.
            UpdateMany(engine, model, random, 1_000);
            engine.SetColumnarSpec(Db, Table, null);
            engine.FlushTableForTesting(Db, Table);
            engine.SetColumnarSpec(Db, Table, new ColumnarSpec(Kinds, [0, 1], -1));
            Build(engine);
            Check(engine, model, pins);

            UpdateMany(engine, model, random, 1_000);
            engine.FlushTableForTesting(Db, Table);
            Assert.True(Check(engine, model, pins) > 0);
            AssertRowsReadAsModel(engine, model);
        }
        finally
        {
            pins.ForEach(static pin => pin.Dispose());
        }
    }

    [Theory]
    [InlineData(61)]
    [InlineData(62)]
    [InlineData(63)]
    public void RandomFlushesCompactionsBackfillsAndPins_KeepThreeWaysInAgreement(int seed)
    {
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        var random = new Random(seed);
        LsmStorageEngine engine = Open(seed == 62 ? LsmReadMode.PositionalRead : LsmReadMode.Auto, flushThreshold: 1L << 30);
        var pins = new List<IDisposable>();
        try
        {
            Build(engine);
            Mutate(engine, model, random, 5_000);
            engine.FlushTableForTesting(Db, Table);
            for (int step = 0; step < 45; step++)
            {
                UpdateMany(engine, model, random, 1 + random.Next(700));
                switch (random.Next(10))
                {
                    case 0 or 1 or 2 or 3:
                        engine.FlushTableForTesting(Db, Table);
                        break;
                    case 4:
                        engine.CompactLevelForTesting(Db, Table, 0);
                        break;
                    case 5 when step % 3 == 0:
                        engine.CompactTable(Db, Table);
                        break;
                    case 6:
                        // A flush without the segment, then the backfill that rewrites it and every newer level-0 file.
                        engine.SetColumnarSpec(Db, Table, null);
                        engine.FlushTableForTesting(Db, Table);
                        engine.SetColumnarSpec(Db, Table, new ColumnarSpec(Kinds, [0, 1], -1));
                        Build(engine);
                        break;
                    case 7 when pins.Count < 4:
                        pins.Add(engine.PinFilesForTesting(Db, Table));
                        break;
                    case 8 when pins.Count > 0:
                        pins[0].Dispose();
                        pins.RemoveAt(0);
                        break;
                    case 9 when step % 9 == 0:
                        pins.ForEach(static pin => pin.Dispose());
                        pins.Clear();
                        engine.Dispose();
                        engine = Open(seed == 62 ? LsmReadMode.PositionalRead : LsmReadMode.Auto, flushThreshold: 1L << 30);
                        Build(engine);
                        break;
                }

                Check(engine, model, pins);
            }

            AssertRowsReadAsModel(engine, model);
        }
        finally
        {
            pins.ForEach(static pin => pin.Dispose());
            engine.Dispose();
        }
    }

    // Scans against the model, then the current and every pinned set of files' vectors against vectors worked
    // out from nothing. Returns how many files of the current set started from a kept vector.
    private static int Check(LsmStorageEngine engine, Dictionary<RowKey, (int Level, long? Gold)> model, List<IDisposable> pins)
    {
        AssertScans(engine, model);
        int reused = engine.VerifyColumnarDeleteVectorsForTesting(Db, Table);
        foreach (IDisposable pin in pins)
        {
            engine.VerifyColumnarDeleteVectorsForTesting(Db, Table, pin);
        }

        return reused;
    }

    // Rewrites (and now and then deletes or adds) rows spread over the whole key range, so new files overlap old ones.
    private static void UpdateMany(LsmStorageEngine engine, Dictionary<RowKey, (int Level, long? Gold)> model, Random random, int count)
    {
        List<RowKey> keys = [.. model.Keys];
        for (int i = 0; i < count && keys.Count > 0; i++)
        {
            RowKey id = keys[random.Next(keys.Count)];
            switch (random.Next(10))
            {
                case 0 when model.ContainsKey(id):
                    engine.DeleteRow(Db, Table, id);
                    model.Remove(id);
                    break;
                case 1:
                    (int, long?) added = (random.Next(-20, 120), random.NextInt64(1_000));
                    model[engine.InsertRow(Db, Table, Row(added.Item1, added.Item2))] = added;
                    break;
                default:
                    if (model.ContainsKey(id))
                    {
                        (int, long?) row = (random.Next(-20, 120), random.Next(7) == 0 ? null : random.NextInt64(-1_000_000, 1_000_000));
                        Assert.True(engine.OverwriteRowForTesting(Db, Table, id, Row(row.Item1, row.Item2)));
                        model[id] = row;
                    }

                    break;
            }
        }
    }

    // The table's live files as its manifest lists them.
    private (long Number, int Level)[] LiveFiles()
    {
        string path = Directory.GetFiles(_root, "MANIFEST", SearchOption.AllDirectories).Single(static p => p.Contains(Table, StringComparison.Ordinal));
        var manifest = new LsmManifest(path);
        return [.. Enumerable.Range(0, 8).SelectMany(level => manifest.GetLiveFiles(level).Select(f => (f.FileNumber, f.Level)))];
    }
}
