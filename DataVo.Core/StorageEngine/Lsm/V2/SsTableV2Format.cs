using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Byte layout of row-keyed SSTables (format version 2). All integers are little-endian.
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
/// </remarks>
internal static class SsTableV2Format
{
    public const ushort Version = 2;

    /// <summary>Same magic as v1 ("SLVD"); the version field tells the layouts apart.</summary>
    public const uint Magic = SsTableFormat.Magic;

    public const int BlockHeaderSize = 16;
    public const int EntrySize = 24;
    public const int IndexHeaderSize = 8;
    public const int IndexEntrySize = 32;
    public const int BloomHeaderSize = 16;
    public const int PropertiesSize = 96;
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

    /// <summary>Whether the file at <paramref name="path"/> is a v2 SSTable; reads only its last 6 bytes.</summary>
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

        return TryReadVersion(tail, out ushort version) && version == Version;
    }

    public static ulong PackTag(ulong sequence, LsmValueType type) => (sequence << 8) | (byte)type;

    public static ulong SequenceOf(ulong tag) => tag >> 8;

    public static LsmValueType TypeOf(ulong tag) => (LsmValueType)(byte)tag;

    public static long AlignUp(long value, int alignment) => (value + alignment - 1) / alignment * alignment;
}

/// <summary>Per-file statistics stored in the properties block.</summary>
internal readonly record struct SsTableV2Properties(
    long SmallestRowId,
    long LargestRowId,
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

    public void WriteTo(Span<byte> destination)
    {
        destination[..SsTableV2Format.PropertiesSize].Clear();
        BinaryPrimitives.WriteInt64LittleEndian(destination[0..], SmallestRowId);
        BinaryPrimitives.WriteInt64LittleEndian(destination[8..], LargestRowId);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[16..], SmallestSequence);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[24..], LargestSequence);
        BinaryPrimitives.WriteInt64LittleEndian(destination[32..], EntryCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[40..], PutCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[48..], TombstoneCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[56..], NextRowId);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[64..], NextRowIdSequence);
        BinaryPrimitives.WriteInt32LittleEndian(destination[72..], BlockCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[80..], LiveRowCount);
    }

    public static SsTableV2Properties ReadFrom(ReadOnlySpan<byte> source) => new(
        BinaryPrimitives.ReadInt64LittleEndian(source[0..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[8..]),
        BinaryPrimitives.ReadUInt64LittleEndian(source[16..]),
        BinaryPrimitives.ReadUInt64LittleEndian(source[24..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[32..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[40..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[48..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[56..]),
        BinaryPrimitives.ReadUInt64LittleEndian(source[64..]),
        BinaryPrimitives.ReadInt32LittleEndian(source[72..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[80..]));
}

/// <summary>One block-index record.</summary>
internal readonly record struct SsTableV2BlockInfo(long FirstRowId, long LastRowId, long Offset, int Length, int EntryCount)
{
    public void WriteTo(Span<byte> destination)
    {
        BinaryPrimitives.WriteInt64LittleEndian(destination[0..], FirstRowId);
        BinaryPrimitives.WriteInt64LittleEndian(destination[8..], LastRowId);
        BinaryPrimitives.WriteInt64LittleEndian(destination[16..], Offset);
        BinaryPrimitives.WriteInt32LittleEndian(destination[24..], Length);
        BinaryPrimitives.WriteInt32LittleEndian(destination[28..], EntryCount);
    }

    public static SsTableV2BlockInfo ReadFrom(ReadOnlySpan<byte> source) => new(
        BinaryPrimitives.ReadInt64LittleEndian(source[0..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[8..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[16..]),
        BinaryPrimitives.ReadInt32LittleEndian(source[24..]),
        BinaryPrimitives.ReadInt32LittleEndian(source[28..]));
}
