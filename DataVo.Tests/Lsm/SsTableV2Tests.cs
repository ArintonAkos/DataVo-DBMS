using System.Text;
using DataVo.Core;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

/// <summary>Fixed 24-byte entries (format version 2): files written before compact blocks.</summary>
public sealed class SsTableV2FixedTests : SsTableV2Tests
{
    private protected override SsTableRowFormat Format => SsTableRowFormat.Fixed;
}

/// <summary>Compact entries with restart points and integer keys (format version 4).</summary>
public sealed class SsTableV4CompactTests : SsTableV2Tests
{
    private protected override SsTableRowFormat Format => SsTableRowFormat.Compact;

    [Theory]
    [MemberData(nameof(Modes))]
    public void ManyVersionsOfOneRow_SpanRestartPoints_AndSnapshotsFindTheRightOne(LsmReadMode mode)
    {
        // 40 versions of row 50 cross two restart points; lookups must start the scan early enough.
        string path = Write(writer =>
        {
            for (long rowId = 1; rowId < 50; rowId++)
            {
                writer.Add(rowId, 1, LsmValueType.Put, Value(rowId));
            }

            for (ulong sequence = 40; sequence >= 1; sequence--)
            {
                writer.Add(50, sequence, LsmValueType.Put, Encoding.UTF8.GetBytes($"v{sequence}"));
            }

            writer.Add(51, 1, LsmValueType.Deletion, ReadOnlySpan<byte>.Empty);
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        Assert.Equal("v40", Get(reader, 50));
        Assert.Equal("v17", Get(reader, 50, snapshot: 17));
        Assert.Equal("v1", Get(reader, 50, snapshot: 1));
        Assert.Equal("<deleted>", Get(reader, 51));
        Assert.Equal("row-49", Get(reader, 49));
    }

    [Fact]
    public void RowIdsAcrossTheWholeLongRange_ShareABlock()
    {
        // Adjacent entries at both ends of the range: their delta does not fit in a long.
        long[] rowIds = [long.MinValue + 1, long.MaxValue - 1];
        string path = Write(writer =>
        {
            foreach (long rowId in rowIds)
            {
                writer.Add(rowId, 1, LsmValueType.Put, Value(rowId));
            }
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead);
        Assert.Equal(1, reader.BlockCount);
        foreach (long rowId in rowIds)
        {
            Assert.Equal($"row-{rowId}", Get(reader, rowId));
        }

        var scanned = new List<long>();
        SsTableV2Cursor cursor = reader.OpenCursor();
        while (cursor.MoveNext())
        {
            scanned.Add((long)cursor.Key);
        }

        Assert.Equal(rowIds, scanned);
    }

    [Fact]
    public void CompactFiles_AreSmallerThanFixedOnes()
    {
        static void Fill(SsTableV2Writer writer)
        {
            for (long rowId = 1; rowId <= 20_000; rowId++)
            {
                writer.Add(rowId, 9, LsmValueType.Put, Value(rowId));
            }
        }

        long compact = new FileInfo(WriteWith(Fill, SsTableRowFormat.Compact)).Length;
        long fixedSize = new FileInfo(WriteWith(Fill, SsTableRowFormat.Fixed)).Length;
        Assert.True(compact < fixedSize * 0.7, $"compact {compact} B vs fixed {fixedSize} B");
    }

    [Fact]
    public void CompactBlock_WithValidChecksumButBrokenEntries_IsRejectedNotMisread()
    {
        string path = Write(writer =>
        {
            for (long rowId = 1; rowId <= 200; rowId++)
            {
                writer.Add(rowId, 1, LsmValueType.Put, Value(rowId));
            }
        });

        long offset;
        int length;
        using (SsTableV2Reader probe = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead))
        {
            SsTableV2BlockInfo info = probe.GetBlockInfo(0);
            (offset, length) = (info.Offset, info.Length);
        }

        // Make the second entry's value length huge, then recompute the checksum: only the structural
        // check can catch it, and it must report corruption instead of reading out of range.
        byte[] bytes = File.ReadAllBytes(path);
        Span<byte> block = bytes.AsSpan((int)offset, length);
        int second = 16 + 8 + 1 + 1 + Value(1).Length; // after restart entry 0: row id, tag, length, value
        block[second + 2] = 0xFF;
        block[second + 3] = 0x7F;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(block, DataVo.Core.Transactions.WalCrc32C.HashToUInt32(block[4..]));
        File.WriteAllBytes(path, bytes);

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead);
        Assert.Throws<InvalidDataException>(() => Get(reader, 1));
    }
}

/// <summary>Compact entries keyed by 16-byte row keys (format version 6), the format the engine writes.</summary>
public sealed class SsTableV6WideTests : SsTableV2Tests
{
    private protected override SsTableRowFormat Format => SsTableRowFormat.Wide;

    [Theory]
    [MemberData(nameof(Modes))]
    public void GuidKeys_RoundTrip_InKeyOrder(LsmReadMode mode)
    {
        var random = new Random(11);
        List<RowKey> keys = [.. Enumerable.Range(0, 5_000)
            .Select(_ => RowKey.FromGuid(RandomGuid(random)))
            .Distinct()
            .Order()];
        string path = Write(writer =>
        {
            foreach (RowKey key in keys)
            {
                writer.Add(key, 3, LsmValueType.Put, Value(key));
            }
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        Assert.True(reader.BlockCount > 1);
        Assert.Equal(keys[0], reader.Properties.SmallestKey);
        Assert.Equal(keys[^1], reader.Properties.LargestKey);
        foreach (RowKey key in keys)
        {
            Assert.Equal($"row-{key}", Get(reader, key));
        }

        for (int i = 0; i < 200; i++)
        {
            RowKey absent = RowKey.FromGuid(RandomGuid(random));
            if (keys.BinarySearch(absent) < 0)
            {
                Assert.Null(Get(reader, absent));
            }
        }

        Assert.Equal(keys, Scan(reader));
    }

    [Fact]
    public void EveryKeyStep_RoundTrips()
    {
        // Same high half with a small and a 62-bit-overflowing low step; a small and a huge high step;
        // negative, positive and extreme integers before the GUID-like keys.
        RowKey[] keys =
        [
            long.MinValue,
            -1,
            0,
            1,
            2,
            long.MaxValue,
            new RowKey(1, 0),
            new RowKey(1, ulong.MaxValue),
            new RowKey(2, 5),
            new RowKey(0x4000_0000_0000_0000UL, 7),
            new RowKey(ulong.MaxValue - 1, 0),
            new RowKey(ulong.MaxValue, ulong.MaxValue),
        ];
        string path = Write(writer =>
        {
            foreach (RowKey key in keys)
            {
                writer.Add(key, 1, LsmValueType.Put, Value(key));
            }
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead);

        // Integer keys sort first and get a v4-encoded block; the rest get a wide block.
        Assert.Equal(2, reader.BlockCount);
        Assert.Equal((RowKey)long.MaxValue, reader.GetBlockInfo(0).LastKey);
        Assert.Equal(new RowKey(1, 0), reader.GetBlockInfo(1).FirstKey);
        foreach (RowKey key in keys)
        {
            Assert.Equal($"row-{key}", Get(reader, key));
        }

        Assert.Equal(keys, Scan(reader));
        Assert.Null(Get(reader, new RowKey(1, 1)));
        Assert.Null(Get(reader, 3));
    }

    [Fact]
    public void IntegerKeys_CostAboutWhatTheyCostInV4()
    {
        static void Fill(SsTableV2Writer writer)
        {
            for (long rowId = 1; rowId <= 20_000; rowId++)
            {
                writer.Add(rowId, 9, LsmValueType.Put, Value(rowId));
            }
        }

        long wide = new FileInfo(WriteWith(Fill, SsTableRowFormat.Wide)).Length;
        long compact = new FileInfo(WriteWith(Fill, SsTableRowFormat.Compact)).Length;
        Assert.True(wide <= compact * 1.05, $"v6 {wide} B vs v4 {compact} B");
    }

    [Fact]
    public void TimeOrderedGuids_AreSmallerThanTheirRawKeys()
    {
        // Sorted version 7 GUIDs share their timestamp bits with the previous key: a step plus the low half.
        var random = new Random(3);
        long millis = 1_700_000_000_000;
        List<RowKey> keys = [.. Enumerable.Range(0, 20_000)
            .Select(i => RowKey.FromGuid(Version7Guid(millis + (i / 50), random)))
            .Distinct()
            .Order()];
        string path = Write(writer =>
        {
            foreach (RowKey key in keys)
            {
                writer.Add(key, 9, LsmValueType.Put, ReadOnlySpan<byte>.Empty);
            }
        });

        long length = new FileInfo(path).Length;
        Assert.True(length < keys.Count * 16L, $"{length} B for {keys.Count} keys");
    }

    [Fact]
    public void IntegerFormats_RejectNonIntegerKeys()
    {
        using var stream = new MemoryStream();
        using var writer = new SsTableV2Writer(stream, format: SsTableRowFormat.Compact);
        Assert.Throws<ArgumentException>(() => writer.Add(new RowKey(1, 0), 1, LsmValueType.Put, "a"u8));
    }

    [Fact]
    public void WideBlock_WithValidChecksumButBrokenEntries_IsRejectedNotMisread()
    {
        // Keys with a non-zero high half, so the block uses the wide encoding.
        string path = Write(writer =>
        {
            for (ulong low = 1; low <= 200; low++)
            {
                writer.Add(new RowKey(1, low), 1, LsmValueType.Put, Value(1));
            }
        });

        long offset;
        int length;
        using (SsTableV2Reader probe = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead))
        {
            SsTableV2BlockInfo info = probe.GetBlockInfo(0);
            (offset, length) = (info.Offset, info.Length);
        }

        // The second entry is a one-byte key step, a one-byte tag delta, then its value length: make the
        // length huge and fix the checksum, so only the structural check can catch it.
        byte[] bytes = File.ReadAllBytes(path);
        Span<byte> block = bytes.AsSpan((int)offset, length);
        int second = 16 + 1 + 8 + 1 + 1 + Value(1).Length; // after restart entry 0: key (high varint, low), tag, length, value
        Assert.Equal(SsTableWideRowBlock.WideFlag, System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(block[8..]));
        block[second + 2] = 0xFF;
        block[second + 3] = 0x7F;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(block, DataVo.Core.Transactions.WalCrc32C.HashToUInt32(block[4..]));
        File.WriteAllBytes(path, bytes);

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead);
        Assert.Throws<InvalidDataException>(() => Get(reader, new RowKey(1, 1)));
    }

    private static List<RowKey> Scan(SsTableV2Reader reader)
    {
        var scanned = new List<RowKey>();
        SsTableV2Cursor cursor = reader.OpenCursor();
        while (cursor.MoveNext())
        {
            scanned.Add(cursor.Key);
        }

        return scanned;
    }

    private static Guid RandomGuid(Random random)
    {
        byte[] bytes = new byte[16];
        random.NextBytes(bytes);
        return new Guid(bytes);
    }

    private static Guid Version7Guid(long unixMillis, Random random)
    {
        // RFC 9562 layout: 48-bit timestamp, version 7, then random bits.
        Span<byte> bytes = stackalloc byte[16];
        random.NextBytes(bytes);
        for (int i = 0; i < 6; i++)
        {
            bytes[i] = (byte)(unixMillis >> (40 - (8 * i)));
        }

        bytes[6] = (byte)(0x70 | (bytes[6] & 0x0F));
        bytes[8] = (byte)(0x80 | (bytes[8] & 0x3F));
        return new Guid(bytes, bigEndian: true);
    }
}

public abstract class SsTableV2Tests : IDisposable
{
    private readonly string _directory = TestStorageDirectories.Create("sstable_v2");

    private protected abstract SsTableRowFormat Format { get; }

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

            writer.SetTableMetadata(20_001, 20_000, 20_000);
        });

        using SsTableV2Reader reader = SsTableV2Reader.Open(path, mode);
        Assert.Equal(mode == LsmReadMode.MemoryMapped, reader.IsMapped);
        Assert.Equal(20_000, reader.Properties.EntryCount);
        Assert.Equal(20_001, reader.Properties.NextRowId);
        Assert.Equal(20_000, reader.Properties.LiveRowCount);
        Assert.Equal((ushort)Format, reader.Version);
        Assert.Equal((RowKey)1, reader.Properties.SmallestKey);
        Assert.Equal((RowKey)20_000, reader.Properties.LargestKey);
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
        RowKey previousRow = default;
        ulong previousSequence = ulong.MaxValue;
        int count = 0;
        while (cursor.MoveNext())
        {
            Assert.True(count == 0 || cursor.Key > previousRow || (cursor.Key == previousRow && cursor.Sequence < previousSequence));
            if (cursor.Type == LsmValueType.Put)
            {
                Assert.Equal($"row-{cursor.Key}", Encoding.UTF8.GetString(cursor.Value));
            }

            previousRow = cursor.Key;
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

    private protected string Write(Action<SsTableV2Writer> fill)
    {
        return WriteWith(fill, Format);
    }

    private protected string WriteWith(Action<SsTableV2Writer> fill, SsTableRowFormat format)
    {
        string path = Path.Combine(_directory, $"{Guid.NewGuid():N}.sst");
        using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write);
        using var writer = new SsTableV2Writer(stream, format: format);
        fill(writer);
        writer.Finish();
        return path;
    }

    private protected static byte[] Value(RowKey rowId) => Encoding.UTF8.GetBytes($"row-{rowId}");

    private protected static string? Get(SsTableV2Reader reader, RowKey rowId, ulong snapshot = ulong.MaxValue) =>
        reader.Lookup(
            rowId,
            snapshot,
            0,
            static (type, _, value, _) => type == LsmValueType.Deletion ? "<deleted>" : Encoding.UTF8.GetString(value),
            (string?)null);
}
