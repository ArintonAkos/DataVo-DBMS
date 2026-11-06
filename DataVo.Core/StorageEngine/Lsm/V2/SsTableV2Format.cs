using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Byte layout of row-keyed SSTables (format versions 2, 4 and 6). All integers are little-endian.
/// </summary>
/// <remarks>
/// <code>
/// file   := data block* | block index | bloom filter | properties | footer
/// block  := header(16) | entry(24)[count] | value bytes | zero padding to the block alignment
/// header := u32 crc32c(bytes 4 .. 16 + payload) | u32 entry count | u32 flags | u32 payload length
/// entry  := i64 row id | u64 tag (sequence &lt;&lt; 8 | type) | u32 value offset (from block start) | u32 value length
/// index  := u32 block count | u32 reserved | (i64 first row id | i64 last row id | i64 offset | i32 length | i32 entry count)[n]
/// bloom  := u32 block count | u32 probe count | u64 reserved | 64-byte blocks (64-byte aligned in the file)
/// footer := index handle | bloom handle | properties handle | u32 block alignment | u16 version | u32 magic
/// </code>
/// Blocks start at multiples of the block alignment (the OS page size, at least 4 KiB), so reading one
/// block faults in whole pages and never shares a page with a neighbour. Entries are sorted by row id
/// ascending, then sequence descending, so the first entry of a row id is its newest version.
/// <para>
/// Version 4 keeps this layout with compact blocks (<see cref="SsTableCompactRowBlock"/>). Version 6 keys
/// rows by 16-byte <see cref="RowKey"/>s: blocks of integer keys in the v4 encoding and blocks of other
/// keys in the wide encoding (<see cref="SsTableWideRowBlock"/>; the block flag tells them apart), index
/// records holding 16-byte first and last keys (48 B), and properties holding 16-byte smallest and largest
/// keys (128 B). Versions 2 and 4 hold integer keys only and are read as such.
/// </para>
/// </remarks>
internal static class SsTableV2Format
{
    public const ushort Version = 2;

    /// <summary>Same layout with compact data blocks (<see cref="SsTableCompactRowBlock"/>).</summary>
    public const ushort CompactVersion = 4;

    /// <summary>Compact blocks keyed by 16-byte row keys (<see cref="SsTableWideRowBlock"/>).</summary>
    public const ushort WideVersion = 6;

    /// <summary>Whether a footer version is a row-keyed SSTable this code reads (v2, v4 or v6).</summary>
    public static bool IsRowKeyedVersion(ushort version) => version is Version or CompactVersion or WideVersion;

    /// <summary>Same magic as v1 ("SLVD"); the version field tells the layouts apart.</summary>
    public const uint Magic = SsTableFormat.Magic;

    public const int BlockHeaderSize = 16;
    public const int EntrySize = 24;
    public const int IndexHeaderSize = 8;
    public const int IndexEntrySize = 32;
    public const int WideIndexEntrySize = 48;
    public const int BloomHeaderSize = 16;
    public const int PropertiesSize = 96;
    public const int WidePropertiesSize = 128;
    public const int BlockHandleSize = 12;
    public const int FooterSize = (BlockHandleSize * 3) + sizeof(uint) + sizeof(ushort) + sizeof(uint);
    public const int MinimumAlignment = 4096;
    public const int BloomBitsPerKey = 10;

    /// <summary>Block alignment for files written on this machine: the OS page size, at least 4 KiB.</summary>
    public static int DefaultAlignment => Math.Max(MinimumAlignment, Environment.SystemPageSize);

    /// <summary>Reads the format version of any SSTable footer (v1 and v2 both end with u16 version | u32 magic).</summary>
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

    /// <summary>Whether the file at <paramref name="path"/> is a row-keyed (v2, v4 or v6) SSTable; reads only its last 6 bytes.</summary>
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

    /// <summary>Properties block size of a file version.</summary>
    public static int PropertiesSizeOf(ushort version) => version == WideVersion ? WidePropertiesSize : PropertiesSize;

    /// <summary>Block index record size of a file version.</summary>
    public static int IndexEntrySizeOf(ushort version) => version == WideVersion ? WideIndexEntrySize : IndexEntrySize;

    /// <summary>
    /// The 64-bit Bloom filter key of a row key in a v6 file: the halves folded with a multiplicative
    /// mix (an integer key folds to its low half). v2 and v4 files hash the integer row id itself.
    /// </summary>
    public static long WideBloomKey(RowKey key) => unchecked((long)((key.High * 0x9E37_79B9_7F4A_7C15UL) ^ key.Low));

    /// <summary>Writes a row key as two little-endian halves (high first), the v6 on-disk form.</summary>
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
    /// <summary>Marks an unknown live-row count (files upgraded from v1); the engine recounts once.</summary>
    public const long UnknownLiveRowCount = -1;

    /// <summary>Writes the layout of <paramref name="version"/>: v6 stores 16-byte keys, so its other fields sit 16 bytes later.</summary>
    public void WriteTo(Span<byte> destination, ushort version)
    {
        destination[..SsTableV2Format.PropertiesSizeOf(version)].Clear();
        int rest;
        if (version == SsTableV2Format.WideVersion)
        {
            SsTableV2Format.WriteKey(destination[0..], SmallestKey);
            SsTableV2Format.WriteKey(destination[16..], LargestKey);
            rest = 32;
        }
        else
        {
            BinaryPrimitives.WriteInt64LittleEndian(destination[0..], SmallestKey.AsInt64());
            BinaryPrimitives.WriteInt64LittleEndian(destination[8..], LargestKey.AsInt64());
            rest = 16;
        }

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

    public static SsTableV2Properties ReadFrom(ReadOnlySpan<byte> source, ushort version)
    {
        bool wide = version == SsTableV2Format.WideVersion;
        int rest = wide ? 32 : 16;
        return new(
            wide ? SsTableV2Format.ReadKey(source[0..]) : BinaryPrimitives.ReadInt64LittleEndian(source[0..]),
            wide ? SsTableV2Format.ReadKey(source[16..]) : BinaryPrimitives.ReadInt64LittleEndian(source[8..]),
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
    public void WriteTo(Span<byte> destination, ushort version)
    {
        int rest;
        if (version == SsTableV2Format.WideVersion)
        {
            SsTableV2Format.WriteKey(destination[0..], FirstKey);
            SsTableV2Format.WriteKey(destination[16..], LastKey);
            rest = 32;
        }
        else
        {
            BinaryPrimitives.WriteInt64LittleEndian(destination[0..], FirstKey.AsInt64());
            BinaryPrimitives.WriteInt64LittleEndian(destination[8..], LastKey.AsInt64());
            rest = 16;
        }

        BinaryPrimitives.WriteInt64LittleEndian(destination[rest..], Offset);
        BinaryPrimitives.WriteInt32LittleEndian(destination[(rest + 8)..], Length);
        BinaryPrimitives.WriteInt32LittleEndian(destination[(rest + 12)..], EntryCount);
    }

    public static SsTableV2BlockInfo ReadFrom(ReadOnlySpan<byte> source, ushort version)
    {
        bool wide = version == SsTableV2Format.WideVersion;
        int rest = wide ? 32 : 16;
        return new(
            wide ? SsTableV2Format.ReadKey(source[0..]) : BinaryPrimitives.ReadInt64LittleEndian(source[0..]),
            wide ? SsTableV2Format.ReadKey(source[16..]) : BinaryPrimitives.ReadInt64LittleEndian(source[8..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[rest..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[(rest + 8)..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[(rest + 12)..]));
    }
}
