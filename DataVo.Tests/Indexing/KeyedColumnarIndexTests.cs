using DataVo.Core;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Tests.Indexing;

/// <summary>
/// A columnar index whose rows are identified by key, as the rows of an LSM table are: stored rows are added,
/// replaced and removed by key, and every scan is checked against the rows held in a dictionary.
/// </summary>
public class KeyedColumnarIndexTests
{
    private static readonly Column[] Columns =
    [
        new() { Name = "Id", Type = "BIGINT" },
        new() { Name = "Level", Type = "INT" },
        new() { Name = "Gold", Type = "BIGINT" },
        new() { Name = "Name", Type = "VARCHAR" },
    ];

    private static readonly RowSerializer.StorageColumnType[] Kinds = [.. Columns.Select(static c => RowSerializer.ClassifyColumnType(c.Type))];

    // A clustered table stores the key outside the row: the row's own key cell is left out.
    private static byte[] Row(long id, int level, long? gold) => RowSerializer.SerializeCells(
        Columns,
        [CellValue.From(id), CellValue.From(level), gold is { } value ? CellValue.From(value) : CellValue.Null, CellValue.From("n" + id)],
        omittedOrdinal: 0);

    private static (int Count, long Ids, long Levels, long Gold, int GoldValues) Scan(ColumnarIndex index)
    {
        ColumnarFold[] folds =
        [
            new() { Slot = index.SlotOf(0), Kind = RowFoldKind.SumInt64 },
            new() { Slot = index.SlotOf(1), Kind = RowFoldKind.SumInt32 },
            new() { Slot = index.SlotOf(2), Kind = RowFoldKind.SumInt64 },
        ];
        var totals = new RowAggregate[3];
        Assert.True(ColumnarScan.TryRun(index, default, folds, totals, 0, out int matched));
        return (matched, totals[0].Whole, totals[1].Whole, totals[2].Whole, totals[2].Values);
    }

    private static void AssertMatches(ColumnarIndex index, Dictionary<long, (int Level, long? Gold)> expected)
    {
        (int count, long ids, long levels, long gold, int goldValues) = Scan(index);
        Assert.Equal(expected.Count, count);
        Assert.Equal(expected.Keys.Sum(), ids);
        Assert.Equal(expected.Values.Sum(static row => (long)row.Level), levels);
        Assert.Equal(expected.Values.Sum(static row => row.Gold ?? 0), gold);
        Assert.Equal(expected.Values.Count(static row => row.Gold is not null), goldValues);
        Assert.True(index.Usable);
    }

    [Fact]
    public void UpsertsAndDeletesByKey_KeepScansEqualToTheRows()
    {
        var index = new ColumnarIndex(Kinds, [0, 1, 2], schemaVersion: 1, keyed: true, keyOrdinal: 0);
        var expected = new Dictionary<long, (int Level, long? Gold)>();
        var random = new Random(5);
        for (int step = 0; step < 60_000; step++)
        {
            long id = random.Next(4) == 0 ? -random.Next(2_000) : random.Next(12_000);
            if (random.Next(5) == 0)
            {
                index.Delete(id);
                expected.Remove(id);
            }
            else
            {
                (int Level, long? Gold) row = (random.Next(100), random.Next(6) == 0 ? null : random.NextInt64(-1_000_000, 1_000_000));
                index.Upsert(id, Row(id, row.Level, row.Gold));
                expected[id] = row;
            }

            if (step % 7_500 == 0)
            {
                AssertMatches(index, expected);
            }
        }

        AssertMatches(index, expected);
        Assert.Equal(index.Count - expected.Count, index.Dead);
        Assert.True(index.WantsRebuild);
    }

    [Fact]
    public void KeyOrder_HoldsForAscendingInsertsAndDeletes_AndEndsAtAnUpdateOrAnEarlierKey()
    {
        var index = new ColumnarIndex(Kinds, [1, 2], schemaVersion: 1, keyed: true, keyOrdinal: 0);
        for (long id = -5; id < 100; id++)
        {
            index.Upsert(id, Row(id, 1, 1));
        }

        index.Delete(50);
        index.Delete(12345); // not there
        Assert.True(index.InKeyOrder);
        Assert.Equal(1, index.Dead);
        Assert.False(index.WantsRebuild);

        index.Upsert(40, Row(40, 2, 2));
        Assert.False(index.InKeyOrder);

        var late = new ColumnarIndex(Kinds, [1], schemaVersion: 1, keyed: true, keyOrdinal: 0);
        late.Upsert(10, Row(10, 1, 1));
        late.Upsert(9, Row(9, 1, 1));
        Assert.False(late.InKeyOrder);
    }

    [Fact]
    public void AnUpdate_IsNeverSeenTwiceOrNotAtAll_ByAConcurrentScan()
    {
        var index = new ColumnarIndex(Kinds, [1, 2], schemaVersion: 1, keyed: true, keyOrdinal: 0);
        const int Rows = 5_000;
        for (long id = 0; id < Rows; id++)
        {
            index.Upsert(id, Row(id, 1, 10));
        }

        // The writer only rewrites rows with the same values, so every consistent scan sees the same totals.
        bool stop = false;
        var writer = new Thread(() =>
        {
            var random = new Random(9);
            // Every rewrite leaves a dead position behind, so the writer is bounded to keep scans short.
            for (int rewrite = 0; rewrite < 100_000 && !Volatile.Read(ref stop); rewrite++)
            {
                long id = random.Next(Rows);
                index.Upsert(id, Row(id, 1, 10));
            }
        });
        writer.Start();
        try
        {
            ColumnarFold[] folds = [new() { Slot = index.SlotOf(2), Kind = RowFoldKind.SumInt64 }];
            var totals = new RowAggregate[1];
            int answered = 0;
            for (int pass = 0; pass < 20_000 && writer.IsAlive; pass++)
            {
                if (ColumnarScan.TryRun(index, default, folds, totals, 0, out int matched))
                {
                    Assert.Equal(Rows, matched);
                    Assert.Equal(Rows * 10L, totals[0].Whole);
                    answered++;
                }
            }

            Assert.True(answered > 0);
        }
        finally
        {
            Volatile.Write(ref stop, true);
            writer.Join();
        }
    }
}
