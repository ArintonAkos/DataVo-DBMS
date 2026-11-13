using DataVo.Core;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Memory.Flat;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Tests.Lsm;

/// <summary>
/// A version 8 SSTable carries some columns of its rows a second time as a columnar segment. These tests write
/// files through <see cref="SsTableV2Writer"/> and check the segment against the rows that went in: the newest
/// put of every key, the keys the file deletes, NULLs, every column type, key order, and what a damaged file
/// does.
/// </summary>
public sealed class SsTableColumnarSegmentTests : IDisposable
{
    private static readonly Column[] Columns =
    [
        new() { Name = "Id", Type = "BIGINT" },
        new() { Name = "Level", Type = "INT" },
        new() { Name = "Gold", Type = "BIGINT" },
        new() { Name = "Score", Type = "DOUBLE" },
        new() { Name = "Ratio", Type = "FLOAT" },
        new() { Name = "Active", Type = "BIT" },
        new() { Name = "Day", Type = "DATE" },
        new() { Name = "Name", Type = "VARCHAR" },
    ];

    private static readonly RowSerializer.StorageColumnType[] Kinds = [.. Columns.Select(static c => RowSerializer.ClassifyColumnType(c.Type))];

    // A clustered table: the key is column 0 and is not stored in the row.
    private static readonly ColumnarSpec Clustered = new(Kinds, [0, 1, 2, 3, 4, 5, 6], keyOrdinal: 0);

    // A table whose rows hold every column; the row key is no column.
    private static readonly ColumnarSpec Heap = new(Kinds, [1, 2, 3, 4, 5, 6], keyOrdinal: -1);

    private readonly string _directory = TestStorageDirectories.Create("sstable_columnar");
    private int _files;

    public static TheoryData<LsmReadMode> Modes => new() { LsmReadMode.MemoryMapped, LsmReadMode.PositionalRead };

    public void Dispose()
    {
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private sealed record Row(int Level, long? Gold, double Score, float Ratio, bool Active, int Day);

    private static Row RandomRow(Random random) => new(
        random.Next(-50, 100),
        random.Next(5) == 0 ? null : random.NextInt64(-1_000_000, 1_000_000),
        random.Next(40) == 0 ? double.NaN : (random.NextDouble() - 0.5) * 1000,
        (float)((random.NextDouble() - 0.5) * 10),
        random.Next(2) == 0,
        700_000 + random.Next(20_000));

    private static byte[] Bytes(long id, Row row, bool clustered) => RowSerializer.SerializeCells(
        Columns,
        [
            CellValue.From(id),
            CellValue.From(row.Level),
            row.Gold is { } gold ? CellValue.From(gold) : CellValue.Null,
            CellValue.From(row.Score),
            CellValue.From((double)row.Ratio),
            CellValue.From(row.Active),
            CellValue.From(DataVoDate.FromDayNumber(row.Day)),
            CellValue.From("n" + id),
        ],
        omittedOrdinal: clustered ? 0 : -1);

    private string Write(ColumnarSpec? spec, Action<SsTableV2Writer> fill)
    {
        string path = Path.Combine(_directory, $"{++_files:D6}.sst");
        using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write);
        using var writer = new SsTableV2Writer(stream, columnar: spec);
        fill(writer);
        writer.Finish();
        return path;
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Segment_HoldsTheNewestPutOfEveryKey_AndTheKeysTheFileDeletes(LsmReadMode mode)
    {
        var random = new Random(3);
        var live = new SortedDictionary<long, Row>();
        var deleted = new List<long>();
        string path = Write(Clustered, writer =>
        {
            for (long id = -6_000; id < 6_000; id++)
            {
                Row newest = RandomRow(random);
                switch (random.Next(8))
                {
                    case 0: // deleted after it was written
                        writer.Add(id, 9, LsmValueType.Deletion, default);
                        writer.Add(id, 4, LsmValueType.Put, Bytes(id, RandomRow(random), clustered: true));
                        deleted.Add(id);
                        continue;
                    case 1: // updated: only the newest version counts
                        writer.Add(id, 9, LsmValueType.Put, Bytes(id, newest, clustered: true));
                        writer.Add(id, 4, LsmValueType.Put, Bytes(id, RandomRow(random), clustered: true));
                        break;
                    case 2: // written again after a delete
                        writer.Add(id, 9, LsmValueType.Put, Bytes(id, newest, clustered: true));
                        writer.Add(id, 4, LsmValueType.Deletion, default);
                        break;
                    default:
                        writer.Add(id, 9, LsmValueType.Put, Bytes(id, newest, clustered: true));
                        break;
                }

                live[id] = newest;
            }
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        Assert.Equal(SsTableV2Format.ColumnarVersion, reader.Version);
        SsTableColumnarSegment segment = Assert.IsType<SsTableColumnarSegment>(reader.Columnar);
        Assert.True(segment.Usable);
        Assert.True(segment.Spec.Matches(Clustered));
        Assert.Equal(live.Count, segment.RowCount);
        Assert.Equal((live.Count + 4095) / 4096, segment.ChunkCount);
        Assert.True(segment.AnyNull(2));
        Assert.False(segment.AnyNull(1));
        Assert.True(segment.AnyNaN(3));

        AssertRows(segment, [.. live.Select(static pair => ((RowKey)pair.Key, pair.Value))], keySlot: 0, firstSlot: 1);
        Assert.Equal(deleted, Tombstones(segment).Select(static key => (long)key));

        // The rows themselves read as they do in a file without a segment.
        long probe = live.Keys.ElementAt(live.Count / 2);
        Assert.Equal(Bytes(probe, live[probe], clustered: true), reader.Lookup(probe, ulong.MaxValue, 0, static (type, _, value, _) => value.ToArray(), []));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void IntegerAndGuidKeys_KeepRowKeyOrder_AndEachChunkItsOwnKeyWidth(LsmReadMode mode)
    {
        var random = new Random(8);
        var rows = new List<(RowKey Key, Row Row)>();
        for (long id = -9_000; id < 9_000; id += 1 + random.Next(3))
        {
            rows.Add((id, RandomRow(random)));
        }

        var guid = new byte[16];
        for (int i = 0; i < 5_000; i++)
        {
            random.NextBytes(guid);
            rows.Add((RowKey.FromGuid(new Guid(guid)), RandomRow(random)));
        }

        rows = [.. rows.DistinctBy(static row => row.Key).OrderBy(static row => row.Key)];
        List<RowKey> deleted = [rows[10].Key, rows[^10].Key];
        string path = Write(Heap, writer =>
        {
            foreach ((RowKey key, Row row) in rows)
            {
                bool gone = deleted.Contains(key);
                writer.Add(key, 5, gone ? LsmValueType.Deletion : LsmValueType.Put, gone ? default : Bytes(0, row, clustered: false));
            }
        });

        rows.RemoveAll(row => deleted.Contains(row.Key));
        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        SsTableColumnarSegment segment = reader.Columnar!;
        Assert.True(segment.Usable);
        Assert.Equal(8, segment.Chunk(0).KeyWidth);
        Assert.Equal(16, segment.Chunk(segment.ChunkCount - 1).KeyWidth);
        Assert.Equal(16, segment.TombstoneKeyWidth);
        AssertRows(segment, rows, keySlot: -1, firstSlot: 0);
        Assert.Equal(deleted, Tombstones(segment));

        // Every key is found in the chunk the directory points at, and a key between two rows is not.
        var chunk = new SsTableColumnarChunkReader();
        foreach ((RowKey key, _) in rows.Where((_, i) => i % 97 == 0))
        {
            int at = segment.FindChunk(key);
            chunk.Load(segment, at);
            Assert.Equal(key, chunk.KeyAt(chunk.Find(key)));
        }

        chunk.Load(segment, segment.FindChunk(deleted[0]));
        Assert.Equal(-1, chunk.Find(deleted[0]));
        Assert.Equal(segment.ChunkCount, segment.FindChunk(new RowKey(ulong.MaxValue, ulong.MaxValue)));
    }

    [Fact]
    public void ZoneMaps_OrderNegativeNumbers_AndLeaveOutNullsAndNaNs()
    {
        Row[] rows =
        [
            new(-7, null, -2.5, -0.5f, true, 10),
            new(3, -40, double.NaN, 1.5f, false, 30),
            new(-9, 12, 8.25, -3.25f, true, 20),
        ];
        string path = Write(Heap, writer =>
        {
            for (int i = 0; i < rows.Length; i++)
            {
                writer.Add(i + 1, 1, LsmValueType.Put, Bytes(0, rows[i], clustered: false));
            }
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path);
        SsTableColumnarSegment segment = reader.Columnar!;
        Assert.Equal((-9L, 3L, (byte)0), Zone(segment, 0));
        Assert.Equal((-40L, 12L, SsTableColumnarFormat.AnyNull), Zone(segment, 1));
        Assert.Equal((BitConverter.DoubleToInt64Bits(-2.5), BitConverter.DoubleToInt64Bits(8.25), SsTableColumnarFormat.AnyNaN), Zone(segment, 2));
        Assert.Equal((BitConverter.DoubleToInt64Bits(-3.25), BitConverter.DoubleToInt64Bits(1.5), (byte)0), Zone(segment, 3));
        Assert.Equal((0L, 1L, (byte)0), Zone(segment, 4));
        Assert.Equal((10L, 30L, (byte)0), Zone(segment, 5));
    }

    [Fact]
    public void RowsTheSegmentCannotHold_MarkItUnusable_AndTheFileStillReads()
    {
        // A clustered table's key column can only be indexed for integer keys.
        RowKey guid = RowKey.FromGuid(Guid.NewGuid());
        string wideKey = Write(Clustered, writer => writer.Add(guid, 1, LsmValueType.Put, Bytes(1, RandomRow(new Random(1)), clustered: true)));
        using (SsTableV2Reader reader = SsTableV2Reader.Open(wideKey))
        {
            Assert.False(reader.Columnar!.Usable);
            Assert.True(reader.MayContain(guid));
        }

        string malformed = Write(Heap, writer => writer.Add(1, 1, LsmValueType.Put, [0, 1, 2]));
        using (SsTableV2Reader reader = SsTableV2Reader.Open(malformed))
        {
            Assert.False(reader.Columnar!.Usable);
            Assert.Equal(1, reader.Columnar.RowCount);
        }
    }

    [Fact]
    public void AFileWithoutASpec_StaysVersion6_AndAFileOfOnlyDeletions_HasAnEmptySegment()
    {
        string plain = Write(null, writer => writer.Add(1, 1, LsmValueType.Put, [1, 2, 3]));
        using (SsTableV2Reader reader = SsTableV2Reader.Open(plain))
        {
            Assert.Equal(SsTableV2Format.WideVersion, reader.Version);
            Assert.Null(reader.Columnar);
        }

        string deletions = Write(Heap, writer =>
        {
            writer.Add(-4, 1, LsmValueType.Deletion, default);
            writer.Add(9, 1, LsmValueType.Deletion, default);
        });
        using (SsTableV2Reader reader = SsTableV2Reader.Open(deletions))
        {
            Assert.Equal(0, reader.Columnar!.ChunkCount);
            Assert.Equal(8, reader.Columnar.TombstoneKeyWidth);
            Assert.Equal([(RowKey)(-4), (RowKey)9], Tombstones(reader.Columnar));
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void DamagedBytes_AreRejected_WhenTheyAreRead(LsmReadMode mode)
    {
        var random = new Random(4);
        string path = Write(Heap, writer =>
        {
            for (long id = 1; id <= 5_000; id++)
            {
                writer.Add(id, 1, id % 50 == 0 ? LsmValueType.Deletion : LsmValueType.Put, id % 50 == 0 ? default : Bytes(0, RandomRow(random), clustered: false));
            }
        });

        long chunkOffset;
        long tombstoneOffset;
        long directoryOffset;
        using (SsTableV2Reader reader = SsTableV2Reader.Open(path, mode))
        {
            chunkOffset = reader.Columnar!.Chunk(1).Offset;
            tombstoneOffset = reader.Columnar.Chunk(1).Offset + reader.Columnar.Chunk(1).Length;
            directoryOffset = tombstoneOffset + (reader.Columnar.TombstoneCount * 8);
        }

        string chunk = Damage(path, chunkOffset + 100);
        using (SsTableV2Reader reader = SsTableV2Reader.Open(chunk, mode))
        {
            var chunkReader = new SsTableColumnarChunkReader();
            chunkReader.Load(reader.Columnar!, 0);
            Assert.Throws<InvalidDataException>(() => chunkReader.Load(reader.Columnar!, 1));
        }

        string tombstones = Damage(path, tombstoneOffset + 3);
        using (SsTableV2Reader reader = SsTableV2Reader.Open(tombstones, mode))
        {
            Assert.Throws<InvalidDataException>(() => reader.Columnar!.Tombstones().Length);
        }

        string directory = Damage(path, directoryOffset + 20);
        Assert.Throws<InvalidDataException>(() => SsTableV2Reader.Open(directory, mode));
    }

    [Fact]
    public void Spec_RoundTrips_AndMatchesOnlyTheSameColumns()
    {
        var bytes = new byte[Clustered.EncodedLength];
        Clustered.WriteTo(bytes);
        ColumnarSpec read = ColumnarSpec.ReadFrom(bytes);
        Assert.True(read.Matches(Clustered));
        Assert.Equal(Clustered.Hash, read.Hash);
        Assert.False(read.Matches(Heap));
        Assert.False(new ColumnarSpec(Kinds, [1, 2], -1).Matches(new ColumnarSpec(Kinds, [2, 1], -1)));
        Assert.Throws<InvalidDataException>(() => ColumnarSpec.ReadFrom(bytes.AsSpan(0, bytes.Length - 1)));
        Assert.Throws<ArgumentException>(() => new ColumnarSpec(Kinds, [7], -1)); // a VARCHAR column
    }

    private string Damage(string path, long offset)
    {
        string copy = Path.Combine(_directory, $"{++_files:D6}.sst");
        byte[] bytes = File.ReadAllBytes(path);
        bytes[offset] ^= 0x40;
        File.WriteAllBytes(copy, bytes);
        return copy;
    }

    private static (long Min, long Max, byte Flags) Zone(SsTableColumnarSegment segment, int slot)
    {
        SsTableColumnarColumn column = segment.Column(0, slot);
        return (column.Min, column.Max, column.Flags);
    }

    private static List<RowKey> Tombstones(SsTableColumnarSegment segment)
    {
        var keys = new List<RowKey>();
        ReadOnlySpan<byte> bytes = segment.Tombstones();
        for (int i = 0; i < segment.TombstoneCount; i++)
        {
            keys.Add(SsTableColumnarSegment.TombstoneAt(bytes, segment.TombstoneKeyWidth, i));
        }

        return keys;
    }

    // Reads every chunk and compares each row's key and columns with what was written.
    private static void AssertRows(SsTableColumnarSegment segment, List<(RowKey Key, Row Row)> expected, int keySlot, int firstSlot)
    {
        var chunk = new SsTableColumnarChunkReader();
        int at = 0;
        for (int c = 0; c < segment.ChunkCount; c++)
        {
            chunk.Load(segment, c);
            Assert.Equal(expected[at].Key, segment.Chunk(c).FirstKey);
            Assert.Equal(expected[at + chunk.Rows - 1].Key, segment.Chunk(c).LastKey);
            for (int row = 0; row < chunk.Rows; row++, at++)
            {
                (RowKey key, Row want) = expected[at];
                Assert.Equal(key, chunk.KeyAt(row));
                if (keySlot >= 0)
                {
                    Assert.Equal((long)key, chunk.Values<long>(keySlot)[row]);
                }

                Assert.Equal(want.Level, chunk.Values<int>(firstSlot)[row]);
                bool goldNull = chunk.HasNulls(firstSlot + 1) && Bitmask.Test(chunk.Nulls(firstSlot + 1), row);
                Assert.Equal(want.Gold is null, goldNull);
                if (want.Gold is { } gold)
                {
                    Assert.Equal(gold, chunk.Values<long>(firstSlot + 1)[row]);
                }

                Assert.Equal(want.Score, chunk.Values<double>(firstSlot + 2)[row]);
                Assert.Equal(want.Ratio, chunk.Values<float>(firstSlot + 3)[row]);
                Assert.Equal(want.Active, Bitmask.Test(chunk.Bits(firstSlot + 4), row));
                Assert.Equal(want.Day, chunk.Values<int>(firstSlot + 5)[row]);
            }
        }

        Assert.Equal(expected.Count, at);
    }
}
