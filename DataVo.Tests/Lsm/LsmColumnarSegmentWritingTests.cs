using DataVo.Core;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Once a table has a columnar spec, every SSTable the LSM engine writes for it (by a flush or a compaction)
/// carries the spec's columns as a columnar segment that agrees with the file's rows, and the spec survives a
/// restart.
/// </summary>
public sealed class LsmColumnarSegmentWritingTests : IDisposable
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

    private static readonly ColumnarSpec Spec = new([.. Columns.Select(static c => RowSerializer.ClassifyColumnType(c.Type))], [0, 1], keyOrdinal: -1);

    private readonly string _root = TestStorageDirectories.Create("lsm_columnar_segment_writing");

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

    private static byte[] Row(int level, long gold) =>
        RowSerializer.SerializeCells(Columns, [CellValue.From(level), CellValue.From(gold), CellValue.From("p" + level)]);

    [Fact]
    public void Flush_WritesASegment_ThatHoldsTheFlushedRows()
    {
        using var engine = new LsmStorageEngine(_root, Relaxed);
        engine.InsertRow(Db, Table, Row(1, 1));
        engine.FlushTableForTesting(Db, Table);
        Assert.All(LiveFiles(engine), static file => Assert.Null(file.Rows)); // no spec yet: a version 6 file

        engine.SetColumnarSpec(Db, Table, Spec);
        List<RowKey> ids = engine.InsertRows(Db, Table, [.. Enumerable.Range(0, 9_000).Select(static i => Row(i, i * 10L))]);
        engine.DeleteRow(Db, Table, ids[5]);
        engine.FlushTableForTesting(Db, Table);

        (long Version, List<(RowKey Key, int Level, long Gold)>? Rows) flushed = LiveFiles(engine).Single(static file => file.Rows is not null);
        Assert.Equal(SsTableV2Format.ColumnarVersion, flushed.Version);
        Assert.Equal(
            ids.Where((_, i) => i != 5).Select(static id => id),
            flushed.Rows!.Select(static row => row.Key));
        Assert.Equal(Enumerable.Range(0, 9_000).Where(static i => i != 5).Select(static i => (i, i * 10L)), flushed.Rows!.Select(static row => (row.Level, row.Gold)));
    }

    [Fact]
    public void Compaction_WritesASegment_ThatHoldsTheNewestVersionOfEveryRow()
    {
        using var engine = new LsmStorageEngine(_root, Relaxed);
        engine.SetColumnarSpec(Db, Table, Spec);
        var expected = new SortedDictionary<RowKey, (int Level, long Gold)>();
        var random = new Random(12);
        for (int round = 0; round < 3; round++)
        {
            foreach (RowKey id in engine.InsertRows(Db, Table, [.. Enumerable.Range(0, 3_000).Select(i => Row((round * 3_000) + i, round))]))
            {
                expected[id] = ((int)(long)id - 1, round);
            }

            foreach (RowKey id in expected.Keys.Where(_ => random.Next(6) == 0).ToList())
            {
                if (random.Next(2) == 0)
                {
                    engine.DeleteRow(Db, Table, id);
                    expected.Remove(id);
                }
                else
                {
                    (int level, long gold) = (random.Next(1_000_000), random.NextInt64());
                    Assert.True(engine.OverwriteRowForTesting(Db, Table, id, Row(level, gold)));
                    expected[id] = (level, gold);
                }
            }

            engine.FlushTableForTesting(Db, Table);
        }

        Assert.Equal(3, LiveFiles(engine).Count);
        engine.CompactTable(Db, Table);

        (long Version, List<(RowKey Key, int Level, long Gold)>? Rows) merged = Assert.Single(LiveFiles(engine));
        Assert.Equal(SsTableV2Format.ColumnarVersion, merged.Version);
        Assert.Equal(expected.Select(static pair => (pair.Key, pair.Value.Level, pair.Value.Gold)), merged.Rows!);
    }

    [Fact]
    public void TheSpec_SurvivesARestart_AndStopsWhenItIsCleared()
    {
        using (var engine = new LsmStorageEngine(_root, Relaxed))
        {
            engine.SetColumnarSpec(Db, Table, Spec);
            engine.InsertRow(Db, Table, Row(1, 1));
        }

        using (var engine = new LsmStorageEngine(_root, Relaxed))
        {
            Assert.True(engine.ColumnarSpecOf(Db, Table)!.Matches(Spec));
            engine.InsertRow(Db, Table, Row(2, 2));
            engine.FlushTableForTesting(Db, Table);
            Assert.All(LiveFiles(engine), static file => Assert.NotNull(file.Rows));

            engine.SetColumnarSpec(Db, Table, null);
            engine.InsertRow(Db, Table, Row(3, 3));
            engine.FlushTableForTesting(Db, Table);
            Assert.Contains(LiveFiles(engine), static file => file.Rows is null);
        }

        using (var engine = new LsmStorageEngine(_root, Relaxed))
        {
            Assert.Null(engine.ColumnarSpecOf(Db, Table));
        }
    }

    // Every live SSTable of the table: its format version and, when it has a segment, the segment's rows.
    private List<(long Version, List<(RowKey Key, int Level, long Gold)>? Rows)> LiveFiles(LsmStorageEngine engine)
    {
        // A read drops the engine's hold on files that compaction replaced, so only live files remain on disk.
        _ = engine.ReadAllRows(Db, Table).Count();
        var files = new List<(long, List<(RowKey, int, long)>?)>();
        var chunk = new SsTableColumnarChunkReader();
        foreach (string path in Directory.GetFiles(_root, "*.sst", SearchOption.AllDirectories).Order())
        {
            using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead);
            if (reader.Columnar is not { } segment)
            {
                files.Add((reader.Version, null));
                continue;
            }

            Assert.True(segment.Usable);
            var rows = new List<(RowKey, int, long)>();
            for (int c = 0; c < segment.ChunkCount; c++)
            {
                chunk.Load(segment, c);
                for (int row = 0; row < chunk.Rows; row++)
                {
                    rows.Add((chunk.KeyAt(row), chunk.Values<int>(0)[row], chunk.Values<long>(1)[row]));
                }
            }

            files.Add((reader.Version, rows));
        }

        return files;
    }
}
