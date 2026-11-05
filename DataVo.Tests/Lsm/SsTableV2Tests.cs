using System.Text;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

public sealed class SsTableV2Tests : IDisposable
{
    private readonly string _directory = TestStorageDirectories.Create("sstable_v2");

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

    [Theory]
    [MemberData(nameof(Modes))]
    public void RoundTrip_FindsEveryRowAndNothingElse(LsmReadMode mode)
    {
        string path = Write(writer =>
        {
            for (long rowId = 1; rowId <= 20_000; rowId++)
            {
                writer.Add(rowId, (ulong)rowId, LsmValueType.Put, Value(rowId));
            }

            writer.SetNextRowId(20_001, 20_000);
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        Assert.Equal(mode == LsmReadMode.MemoryMapped, reader.IsMapped);
        Assert.Equal(20_000, reader.Properties.EntryCount);
        Assert.Equal(20_001, reader.Properties.NextRowId);
        Assert.Equal(1, reader.Properties.SmallestRowId);
        Assert.Equal(20_000, reader.Properties.LargestRowId);
        Assert.True(reader.BlockCount > 1);

        for (long rowId = 1; rowId <= 20_000; rowId += 7)
        {
            Assert.Equal($"row-{rowId}", Get(reader, rowId));
        }

        Assert.Null(Get(reader, 0));
        Assert.Null(Get(reader, 20_001));
        Assert.Null(Get(reader, -5));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Blocks_ArePageAligned(LsmReadMode mode)
    {
        string path = Write(writer =>
        {
            for (long rowId = 1; rowId <= 5_000; rowId++)
            {
                writer.Add(rowId, 1, LsmValueType.Put, Value(rowId));
            }
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        for (int block = 0; block < reader.BlockCount; block++)
        {
            SsTableV2BlockInfo info = reader.GetBlockInfo(block);
            Assert.Equal(0, info.Offset % reader.Alignment);
            Assert.True(info.Length <= reader.Alignment, "a block of small rows must fit in one page");
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Versions_NewestVisibleWins_AndSnapshotsSeeOlderVersions(LsmReadMode mode)
    {
        string path = Write(writer =>
        {
            writer.Add(10, 30, LsmValueType.Deletion, ReadOnlySpan<byte>.Empty);
            writer.Add(10, 20, LsmValueType.Put, "v20"u8);
            writer.Add(10, 10, LsmValueType.Put, "v10"u8);
            writer.Add(11, 5, LsmValueType.Put, "eleven"u8);
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        Assert.Equal("<deleted>", Get(reader, 10));
        Assert.Equal("v20", Get(reader, 10, snapshot: 25));
        Assert.Equal("v10", Get(reader, 10, snapshot: 15));
        Assert.Null(Get(reader, 10, snapshot: 5));
        Assert.Equal("eleven", Get(reader, 11));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void VersionsSpanningBlocks_AreFound(LsmReadMode mode)
    {
        // Enough versions of one row id to spill over several blocks.
        string path = Write(writer =>
        {
            writer.Add(1, 1, LsmValueType.Put, "first"u8);
            for (ulong sequence = 5_000; sequence >= 10; sequence--)
            {
                writer.Add(2, sequence, LsmValueType.Put, Encoding.UTF8.GetBytes($"v{sequence}"));
            }

            writer.Add(3, 1, LsmValueType.Put, "third"u8);
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        Assert.True(reader.BlockCount > 2);
        Assert.Equal("v5000", Get(reader, 2));
        Assert.Equal("v10", Get(reader, 2, snapshot: 10)); // the oldest version, in the last block of the run
        Assert.Null(Get(reader, 2, snapshot: 9));
        Assert.Equal("third", Get(reader, 3));
        Assert.Equal("first", Get(reader, 1));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void RowLargerThanABlock_GetsItsOwnPageRoundedBlock(LsmReadMode mode)
    {
        byte[] big = new byte[50_000];
        new Random(7).NextBytes(big);
        string path = Write(writer =>
        {
            writer.Add(1, 1, LsmValueType.Put, "small"u8);
            writer.Add(2, 1, LsmValueType.Put, big);
            writer.Add(3, 1, LsmValueType.Put, "after"u8);
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        byte[]? read = reader.Lookup(2, ulong.MaxValue, 0, static (type, _, value, _) => type == LsmValueType.Put ? value.ToArray() : null, null);
        Assert.Equal(big, read);
        Assert.Equal("after", Get(reader, 3));
        for (int block = 0; block < reader.BlockCount; block++)
        {
            Assert.Equal(0, reader.GetBlockInfo(block).Offset % reader.Alignment);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Cursor_VisitsEveryEntryInKeyOrder(LsmReadMode mode)
    {
        string path = Write(writer =>
        {
            for (long rowId = 1; rowId <= 3_000; rowId++)
            {
                writer.Add(rowId, 2, LsmValueType.Put, Value(rowId));
                if (rowId % 3 == 0)
                {
                    writer.Add(rowId, 1, LsmValueType.Deletion, ReadOnlySpan<byte>.Empty);
                }
            }
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        SsTableV2Cursor cursor = reader.OpenCursor();
        long previousRow = long.MinValue;
        ulong previousSequence = ulong.MaxValue;
        int count = 0;
        while (cursor.MoveNext())
        {
            Assert.True(cursor.RowId > previousRow || (cursor.RowId == previousRow && cursor.Sequence < previousSequence));
            if (cursor.Type == LsmValueType.Put)
            {
                Assert.Equal($"row-{cursor.RowId}", Encoding.UTF8.GetString(cursor.Value));
            }

            previousRow = cursor.RowId;
            previousSequence = cursor.Sequence;
            count++;
        }

        Assert.Equal(4_000, count);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void CorruptDataBlock_IsDetectedByItsChecksum(LsmReadMode mode)
    {
        string path = Write(writer =>
        {
            for (long rowId = 1; rowId <= 2_000; rowId++)
            {
                writer.Add(rowId, 1, LsmValueType.Put, Value(rowId));
            }
        });

        long target;
        using (SsTableV2Reader probe = SsTableV2Reader.Open(path, mode))
        {
            SsTableV2BlockInfo info = probe.GetBlockInfo(0);
            target = info.Offset + info.Length - 3; // inside the value bytes
        }

        byte[] bytes = File.ReadAllBytes(path);
        bytes[target] ^= 0x5A;
        File.WriteAllBytes(path, bytes);

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        Assert.Throws<InvalidDataException>(() => Get(reader, 1));
    }

    [Fact]
    public void TruncatedFile_IsRejectedAtOpen()
    {
        string path = Write(writer => writer.Add(1, 1, LsmValueType.Put, "x"u8));
        byte[] bytes = File.ReadAllBytes(path);
        File.WriteAllBytes(path, bytes.AsSpan(0, bytes.Length - 10).ToArray());

        Assert.Throws<InvalidDataException>(() => SsTableV2Reader.Open(path, LsmReadMode.PositionalRead));
    }

    [Fact]
    public void Writer_RejectsOutOfOrderEntries()
    {
        using var stream = new MemoryStream();
        using var writer = new SsTableV2Writer(stream);
        writer.Add(5, 1, LsmValueType.Put, "a"u8);
        Assert.Throws<InvalidOperationException>(() => writer.Add(4, 1, LsmValueType.Put, "b"u8));
        Assert.Throws<InvalidOperationException>(() => writer.Add(5, 1, LsmValueType.Put, "c"u8)); // same sequence
    }

    [Fact]
    public void BloomFilter_HasNoFalseNegatives_AndALowFalsePositiveRate()
    {
        const int keys = 100_000;
        byte[] blocks = new byte[BlockedBloomFilter.ComputeBlockCount(keys, 10) * BlockedBloomFilter.BlockBytes];
        for (long key = 0; key < keys; key++)
        {
            BlockedBloomFilter.Add(blocks, key * 2);
        }

        for (long key = 0; key < keys; key++)
        {
            Assert.True(BlockedBloomFilter.MightContain(blocks, key * 2));
        }

        int falsePositives = 0;
        for (long key = 0; key < keys; key++)
        {
            if (BlockedBloomFilter.MightContain(blocks, (key * 2) + 1))
            {
                falsePositives++;
            }
        }

        double rate = falsePositives / (double)keys;
        Assert.True(rate < 0.02, $"false-positive rate {rate:P2}");
    }

    private string Write(Action<SsTableV2Writer> fill)
    {
        string path = Path.Combine(_directory, $"{Guid.NewGuid():N}.sst");
        using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write);
        using var writer = new SsTableV2Writer(stream);
        fill(writer);
        writer.Finish();
        return path;
    }

    private static byte[] Value(long rowId) => Encoding.UTF8.GetBytes($"row-{rowId}");

    private static string? Get(SsTableV2Reader reader, long rowId, ulong snapshot = ulong.MaxValue) =>
        reader.Lookup(
            rowId,
            snapshot,
            0,
            static (type, _, value, _) => type == LsmValueType.Deletion ? "<deleted>" : Encoding.UTF8.GetString(value),
            (string?)null);
}
