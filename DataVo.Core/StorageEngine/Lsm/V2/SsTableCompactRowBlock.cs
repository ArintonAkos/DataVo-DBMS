using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Compact encoding of a row-keyed data block (format version 4): entries store differences to the
/// previous entry instead of fixed 24-byte records, with a full "restart" entry every
/// <see cref="RestartInterval"/> entries so a lookup can still binary search.
/// </summary>
/// <remarks>
/// <code>
/// payload  := entry* | u32 restart offset[restarts] | u32 restarts
/// restart  := i64 row id | varint tag | varint value length | value
/// entry    := varint row id delta | varint zigzag(tag delta) | varint value length | value
/// </code>
/// Offsets are from the block start. The tag is (sequence &lt;&lt; 8 | type) as in v2; rows of one
/// commit share a sequence, so the tag delta is usually 0 and a small row needs 3 bytes of header
/// instead of 24. This is the LevelDB/RocksDB restart-point scheme, applied to integer keys.
/// </remarks>
internal static class SsTableCompactRowBlock
{
    public const int RestartInterval = 16;

    /// <summary>Block flag (header bytes 8..12) marking this encoding; v2 blocks have flags 0.</summary>
    public const int CompactFlag = 1;

    public static bool IsCompact(ReadOnlySpan<byte> block) =>
        (BinaryPrimitives.ReadInt32LittleEndian(block[8..]) & CompactFlag) != 0;

    public static int RestartCount(ReadOnlySpan<byte> block) =>
        BinaryPrimitives.ReadInt32LittleEndian(block[^sizeof(int)..]);

    public static int RestartsStart(ReadOnlySpan<byte> block) =>
        block.Length - sizeof(int) - (RestartCount(block) * sizeof(int));

    public static int RestartOffset(ReadOnlySpan<byte> block, int restart) =>
        BinaryPrimitives.ReadInt32LittleEndian(block[(RestartsStart(block) + (restart * sizeof(int)))..]);

    /// <summary>Row id of a restart entry (stored in full, first 8 bytes of the entry).</summary>
    public static long RestartRowId(ReadOnlySpan<byte> block, int restart) =>
        BinaryPrimitives.ReadInt64LittleEndian(block[RestartOffset(block, restart)..]);

    public static int WriteVarint(Span<byte> destination, ulong value)
    {
        int written = 0;
        while (value >= 0x80)
        {
            destination[written++] = (byte)(value | 0x80);
            value >>= 7;
        }

        destination[written++] = (byte)value;
        return written;
    }

    public static int VarintSize(ulong value)
    {
        int size = 1;
        while (value >= 0x80)
        {
            value >>= 7;
            size++;
        }

        return size;
    }

    public static ulong ReadVarint(ReadOnlySpan<byte> source, ref int position, int limit)
    {
        ulong result = 0;
        for (int shift = 0; shift < 64; shift += 7)
        {
            if (position >= limit)
            {
                throw new InvalidDataException("Compact SSTable entry is truncated.");
            }

            byte b = source[position++];
            result |= (ulong)(b & 0x7F) << shift;
            if (b < 0x80)
            {
                return result;
            }
        }

        throw new InvalidDataException("Compact SSTable varint is too long.");
    }

    public static ulong ZigZag(long value) => (ulong)((value << 1) ^ (value >> 63));

    public static long UnZigZag(ulong value) => (long)(value >> 1) ^ -(long)(value & 1);
}

/// <summary>
/// Sequential decoder over one compact block. A mutable struct: callers keep it in a local or field and
/// pass the block bytes to each call, so decoding allocates nothing.
/// </summary>
internal struct CompactRowBlockReader
{
    private int _position;
    private int _limit;
    private int _count;

    public int Index;
    public int EntryOffset;
    public long RowId;
    public ulong Tag;
    public int ValueOffset;
    public int ValueLength;

    /// <summary>Positions before the first entry of restart <paramref name="restart"/>.</summary>
    public void Seek(ReadOnlySpan<byte> block, int restart)
    {
        _count = BinaryPrimitives.ReadInt32LittleEndian(block[4..]);
        _limit = SsTableCompactRowBlock.RestartsStart(block);
        _position = SsTableCompactRowBlock.RestartOffset(block, restart);
        Index = (restart * SsTableCompactRowBlock.RestartInterval) - 1;
    }

    /// <summary>Decodes the next entry; false at the end of the block.</summary>
    public bool MoveNext(ReadOnlySpan<byte> block)
    {
        if (Index + 1 >= _count)
        {
            return false;
        }

        Index++;
        EntryOffset = _position;
        if (Index % SsTableCompactRowBlock.RestartInterval == 0)
        {
            if (_position > _limit - sizeof(long))
            {
                throw new InvalidDataException("Compact SSTable restart entry is truncated.");
            }

            RowId = BinaryPrimitives.ReadInt64LittleEndian(block[_position..]);
            _position += sizeof(long);
            Tag = SsTableCompactRowBlock.ReadVarint(block, ref _position, _limit);
        }
        else
        {
            // Unsigned wrap-around: a delta between a very negative and a very positive id exceeds long.
            RowId = unchecked((long)((ulong)RowId + SsTableCompactRowBlock.ReadVarint(block, ref _position, _limit)));
            Tag = (ulong)((long)Tag + SsTableCompactRowBlock.UnZigZag(SsTableCompactRowBlock.ReadVarint(block, ref _position, _limit)));
        }

        ulong length = SsTableCompactRowBlock.ReadVarint(block, ref _position, _limit);
        if (length > (ulong)(_limit - _position))
        {
            throw new InvalidDataException("Compact SSTable value points outside its block.");
        }

        ValueOffset = _position;
        ValueLength = (int)length;
        _position += ValueLength;
        return true;
    }

    /// <summary>Whether every entry has been decoded and the entries end exactly at the restart array.</summary>
    public readonly bool EndsAtRestarts => Index + 1 == _count && _position == _limit;
}
