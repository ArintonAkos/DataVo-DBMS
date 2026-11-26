using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Byte layout of row-keyed SSTables (format versions 6 and 8). All integers are little-endian.
/// </summary>
/// <remarks>
/// <code>
/// file   := data block* | block index | bloom filter | properties | footer
/// block  := header(16) | prefix-compressed entries | restart offsets | u32 restart count | zero padding to the block alignment
/// header := u32 crc32c(bytes 4 .. 16 + payload) | u32 entry count | u32 flags | u32 payload length
/// index  := u32 block count | u32 reserved | (16-byte first key | 16-byte last key | i64 offset | i32 length | i32 entry count)[n]
/// bloom  := u32 block count | u32 probe count | u64 reserved | 64-byte blocks (64-byte aligned in the file)
/// footer := index handle | bloom handle | properties handle | u32 block alignment | u16 version | u32 magic
/// </code>
/// Blocks start at multiples of the block alignment (the OS page size, at least 4 KiB), so reading one
/// block faults in whole pages and never shares a page with a neighbour. Entries are sorted by row key
/// ascending, then sequence descending, so the first entry of a row key is its newest version.
/// <para>
/// Rows are keyed by 16-byte <see cref="RowKey"/>s: blocks of integer keys use the integer encoding
/// (<see cref="SsTableCompactRowBlock"/>) and blocks of other keys the wide encoding
/// (<see cref="SsTableWideRowBlock"/>); the block flag tells them apart. Properties hold 16-byte smallest
/// and largest keys (128 B).
/// </para>
/// <para>
/// Version 8 is version 6 with a columnar segment (<see cref="SsTableColumnarFormat"/>) between the last data
/// block and the block index, and a fourth handle in the footer, after the properties handle, that points at
/// the segment's directory. Everything a point lookup reads is unchanged.
/// </para>
/// </remarks>
internal static class SsTableV2Format
{
    /// <summary>Compact blocks keyed by 16-byte row keys (<see cref="SsTableWideRowBlock"/>).</summary>
    public const ushort WideVersion = 6;

    /// <summary>Version 6 with a columnar segment and a footer that points at it.</summary>
    public const ushort ColumnarVersion = 8;

    /// <summary>Whether a footer version is a row-keyed SSTable this code reads (v6 or v8).</summary>
    public static bool IsRowKeyedVersion(ushort version) => version is WideVersion or ColumnarVersion;

    /// <summary>File magic ("SLVD") shared by the row-keyed and keyed layouts; the version field tells them apart.</summary>
    public const uint Magic = 0x44564C53U;

    public const int BlockHeaderSize = 16;
    public const int IndexHeaderSize = 8;
    public const int IndexEntrySize = 48;
    public const int BloomHeaderSize = 16;
    public const int PropertiesSize = 128;
    public const int BlockHandleSize = 12;
    public const int FooterSize = (BlockHandleSize * 3) + sizeof(uint) + sizeof(ushort) + sizeof(uint);

    /// <summary>The footer of a version 8 file: one more handle, for the columnar segment's directory.</summary>
    public const int ColumnarFooterSize = FooterSize + BlockHandleSize;
    public const int MinimumAlignment = 4096;
    public const int BloomBitsPerKey = 10;

    /// <summary>Block alignment for files written on this machine: the OS page size, at least 4 KiB.</summary>
    public static int DefaultAlignment => Math.Max(MinimumAlignment, Environment.SystemPageSize);

    /// <summary>Reads the format version of any SSTable footer (row-keyed and keyed both end with u16 version | u32 magic).</summary>
    public static bool TryReadVersion(ReadOnlySpan<byte> tail, out ushort version)
    {
        version = 0;
        if (tail.Length < sizeof(ushort) + sizeof(uint)
            || BinaryPrimitives.ReadUInt32LittleEndian(tail[^sizeof(uint)..]) != Magic)
        {
            return false;
        }

        version = BinaryPrimitives.ReadUInt16LittleEndian(tail.Slice(tail.Length - sizeof(uint) - sizeof(ushort), sizeof(ushort)));
        return true;
    }

    /// <summary>Whether the file at <paramref name="path"/> is a row-keyed (v6 or v8) SSTable; reads only its last 6 bytes.</summary>
    public static bool IsV2File(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        if (stream.Length < FooterSize)
        {
            return false;
        }

        Span<byte> tail = stackalloc byte[sizeof(ushort) + sizeof(uint)];
        stream.Position = stream.Length - tail.Length;
        int read = 0;
        while (read < tail.Length)
        {
            int n = stream.Read(tail[read..]);
            if (n == 0)
            {
                return false;
            }

            read += n;
        }

        return TryReadVersion(tail, out ushort version) && IsRowKeyedVersion(version);
    }

    public static ulong PackTag(ulong sequence, LsmValueType type) => (sequence << 8) | (byte)type;

    public static ulong SequenceOf(ulong tag) => tag >> 8;

    public static LsmValueType TypeOf(ulong tag) => (LsmValueType)(byte)tag;

    public static long AlignUp(long value, int alignment) => (value + alignment - 1) / alignment * alignment;

    /// <summary>Footer size of a file version.</summary>
    public static int FooterSizeOf(ushort version) => version == ColumnarVersion ? ColumnarFooterSize : FooterSize;

    /// <summary>
    /// The 64-bit Bloom filter key of a row key: the halves folded with a multiplicative mix (an integer
    /// key folds to its low half).
    /// </summary>
    public static long WideBloomKey(RowKey key) => unchecked((long)((key.High * 0x9E37_79B9_7F4A_7C15UL) ^ key.Low));

    /// <summary>Writes a row key as two little-endian halves (high first), the on-disk form.</summary>
    public static void WriteKey(Span<byte> destination, RowKey key)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(destination, key.High);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[8..], key.Low);
    }

    /// <summary>Reads a row key written by <see cref="WriteKey"/>.</summary>
    public static RowKey ReadKey(ReadOnlySpan<byte> source) =>
        new(BinaryPrimitives.ReadUInt64LittleEndian(source), BinaryPrimitives.ReadUInt64LittleEndian(source[8..]));
}

/// <summary>Per-file statistics stored in the properties block.</summary>
internal readonly record struct SsTableV2Properties(
    RowKey SmallestKey,
    RowKey LargestKey,
    ulong SmallestSequence,
    ulong LargestSequence,
    long EntryCount,
    long PutCount,
    long TombstoneCount,
    long NextRowId,
    ulong NextRowIdSequence,
    int BlockCount,
    long LiveRowCount)
{
    /// <summary>Writes the properties block: two 16-byte keys, then the counters.</summary>
    public void WriteTo(Span<byte> destination)
    {
        destination[..SsTableV2Format.PropertiesSize].Clear();
        SsTableV2Format.WriteKey(destination[0..], SmallestKey);
        SsTableV2Format.WriteKey(destination[16..], LargestKey);
        const int rest = 32;

        BinaryPrimitives.WriteUInt64LittleEndian(destination[rest..], SmallestSequence);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[(rest + 8)..], LargestSequence);
        BinaryPrimitives.WriteInt64LittleEndian(destination[(rest + 16)..], EntryCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[(rest + 24)..], PutCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[(rest + 32)..], TombstoneCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[(rest + 40)..], NextRowId);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[(rest + 48)..], NextRowIdSequence);
        BinaryPrimitives.WriteInt32LittleEndian(destination[(rest + 56)..], BlockCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[(rest + 64)..], LiveRowCount);
    }

    /// <summary>Reads a properties block written by <see cref="WriteTo"/>.</summary>
    public static SsTableV2Properties ReadFrom(ReadOnlySpan<byte> source)
    {
        const int rest = 32;
        return new(
            SsTableV2Format.ReadKey(source[0..]),
            SsTableV2Format.ReadKey(source[16..]),
            BinaryPrimitives.ReadUInt64LittleEndian(source[rest..]),
            BinaryPrimitives.ReadUInt64LittleEndian(source[(rest + 8)..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[(rest + 16)..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[(rest + 24)..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[(rest + 32)..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[(rest + 40)..]),
            BinaryPrimitives.ReadUInt64LittleEndian(source[(rest + 48)..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[(rest + 56)..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[(rest + 64)..]));
    }
}

/// <summary>One block-index record.</summary>
internal readonly record struct SsTableV2BlockInfo(RowKey FirstKey, RowKey LastKey, long Offset, int Length, int EntryCount)
{
    /// <summary>Writes the record: two 16-byte keys, then the block handle and entry count.</summary>
    public void WriteTo(Span<byte> destination)
    {
        SsTableV2Format.WriteKey(destination[0..], FirstKey);
        SsTableV2Format.WriteKey(destination[16..], LastKey);
        const int rest = 32;

        BinaryPrimitives.WriteInt64LittleEndian(destination[rest..], Offset);
        BinaryPrimitives.WriteInt32LittleEndian(destination[(rest + 8)..], Length);
        BinaryPrimitives.WriteInt32LittleEndian(destination[(rest + 12)..], EntryCount);
    }

    /// <summary>Reads a record written by <see cref="WriteTo"/>.</summary>
    public static SsTableV2BlockInfo ReadFrom(ReadOnlySpan<byte> source)
    {
        const int rest = 32;
        return new(
            SsTableV2Format.ReadKey(source[0..]),
            SsTableV2Format.ReadKey(source[16..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[rest..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[(rest + 8)..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[(rest + 12)..]));
    }
}
