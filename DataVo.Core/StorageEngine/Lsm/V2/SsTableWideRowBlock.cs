using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Compact data block keyed by 16-byte <see cref="RowKey"/>s (format version 6). Same restart scheme as
/// <see cref="SsTableCompactRowBlock"/>; a key is stored relative to the previous one.
/// </summary>
/// <remarks>
/// A v6 file uses this encoding only for keys that are not integers (GUIDs): blocks of integer keys keep
/// the v4 encoding, which is a little smaller and faster to search, and the block flag says which one a
/// block uses. Keys ascend, so a file's integer blocks all come before its wide blocks.
/// </remarks>
/// <remarks>
/// <code>
/// payload  := entry* | u32 restart offset[restarts] | u32 restarts
/// restart  := varint high | u64 low | varint tag | varint value length | value
/// entry    := varint key step | [key bytes] | varint zigzag(tag delta) | varint value length | value
/// key step := low-delta &lt;&lt; 2 | 0              same high half, low half grows by low-delta
///           | high-delta &lt;&lt; 2 | 1, u64 low    high half grows by high-delta, low half stored in full
///           | 2, u64 high, u64 low              key stored in full (a delta does not fit in 62 bits)
/// </code>
/// Integer keys (high half zero) cost what they cost in v4: a step of 1..31 is one byte, and a restart
/// key is 9 bytes (a zero high half is one varint byte). Time-ordered GUIDs share most of their high half
/// with the previous key, random GUIDs little; neither is worse than storing the 16 bytes (a restart may
/// take two bytes more). Fixed-width halves are little-endian like every other integer in the file.
/// </remarks>
internal static class SsTableWideRowBlock
{
    public const int RestartInterval = SsTableCompactRowBlock.RestartInterval;

    /// <summary>Block flag (header bytes 8..12) marking this encoding.</summary>
    public const int WideFlag = 2;

    public const int KeySize = 16;

    private const ulong MaxDelta = (1UL << 62) - 1;

    /// <summary>
    /// A varint read whose one-byte case (integer keys' zero high half, small steps, tag deltas, short
    /// lengths) stays inline in the lookup loop; longer ones take the shared decoder.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static ulong ReadVarint(ReadOnlySpan<byte> block, ref int position, int limit)
    {
        if ((uint)position < (uint)limit && block[position] < 0x80)
        {
            return block[position++];
        }

        return SsTableCompactRowBlock.ReadVarint(block, ref position, limit);
    }

    public static bool IsWide(ReadOnlySpan<byte> block) =>
        (BinaryPrimitives.ReadInt32LittleEndian(block[8..]) & WideFlag) != 0;

    /// <summary>Key of a restart entry (stored in full at the start of the entry).</summary>
    public static RowKey RestartKey(ReadOnlySpan<byte> block, int restart)
    {
        int position = SsTableCompactRowBlock.RestartOffset(block, restart);
        return ReadRestartKey(block, ref position, block.Length);
    }

    /// <summary>Bytes a restart key takes.</summary>
    public static int RestartKeySize(RowKey key) => SsTableCompactRowBlock.VarintSize(key.High) + sizeof(ulong);

    public static int WriteRestartKey(Span<byte> destination, RowKey key)
    {
        int written = SsTableCompactRowBlock.WriteVarint(destination, key.High);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[written..], key.Low);
        return written + sizeof(ulong);
    }

    public static RowKey ReadRestartKey(ReadOnlySpan<byte> block, ref int position, int limit)
    {
        ulong high = ReadVarint(block, ref position, limit);
        if (position > limit - sizeof(ulong))
        {
            throw new InvalidDataException("Wide SSTable restart key is truncated.");
        }

        ulong low = BinaryPrimitives.ReadUInt64LittleEndian(block[position..]);
        position += sizeof(ulong);
        return new RowKey(high, low);
    }

    /// <summary>Bytes the key of a non-restart entry takes after <paramref name="previous"/>.</summary>
    public static int StepSize(RowKey previous, RowKey key)
    {
        if (key.High == previous.High && key.Low - previous.Low <= MaxDelta)
        {
            return SsTableCompactRowBlock.VarintSize((key.Low - previous.Low) << 2);
        }

        return key.High - previous.High <= MaxDelta
            ? SsTableCompactRowBlock.VarintSize(((key.High - previous.High) << 2) | 1) + sizeof(ulong)
            : 1 + KeySize;
    }

    /// <summary>Writes the key of a non-restart entry; keys ascend, so both deltas are non-negative.</summary>
    public static int WriteStep(Span<byte> destination, RowKey previous, RowKey key)
    {
        if (key.High == previous.High && key.Low - previous.Low <= MaxDelta)
        {
            return SsTableCompactRowBlock.WriteVarint(destination, (key.Low - previous.Low) << 2);
        }

        if (key.High - previous.High <= MaxDelta)
        {
            int written = SsTableCompactRowBlock.WriteVarint(destination, ((key.High - previous.High) << 2) | 1);
            BinaryPrimitives.WriteUInt64LittleEndian(destination[written..], key.Low);
            return written + sizeof(ulong);
        }

        destination[0] = 2;
        SsTableV2Format.WriteKey(destination[1..], key);
        return 1 + KeySize;
    }
}

/// <summary>
/// Sequential decoder over one wide block. A mutable struct like <see cref="CompactRowBlockReader"/>:
/// callers keep it in a local or field and pass the block bytes to each call, so decoding allocates nothing.
/// </summary>
internal struct WideRowBlockReader
{
    private int _position;
    private int _limit;
    private int _count;

    public int Index;
    public int EntryOffset;
    public RowKey Key;
    public ulong Tag;
    public int ValueOffset;
    public int ValueLength;

    /// <summary>Positions before the first entry of restart <paramref name="restart"/>.</summary>
    public void Seek(ReadOnlySpan<byte> block, int restart)
    {
        _count = BinaryPrimitives.ReadInt32LittleEndian(block[4..]);
        _limit = SsTableCompactRowBlock.RestartsStart(block);
        _position = SsTableCompactRowBlock.RestartOffset(block, restart);
        Index = (restart * SsTableWideRowBlock.RestartInterval) - 1;
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
        if (Index % SsTableWideRowBlock.RestartInterval == 0)
        {
            Key = SsTableWideRowBlock.ReadRestartKey(block, ref _position, _limit);
            Tag = SsTableWideRowBlock.ReadVarint(block, ref _position, _limit);
        }
        else
        {
            ulong step = SsTableWideRowBlock.ReadVarint(block, ref _position, _limit);
            switch (step & 3)
            {
                case 0:
                    Key = new RowKey(Key.High, unchecked(Key.Low + (step >> 2)));
                    break;
                case 1:
                    if (_position > _limit - sizeof(ulong))
                    {
                        throw new InvalidDataException("Wide SSTable entry key is truncated.");
                    }

                    Key = new RowKey(unchecked(Key.High + (step >> 2)), BinaryPrimitives.ReadUInt64LittleEndian(block[_position..]));
                    _position += sizeof(ulong);
                    break;
                default:
                    if (step != 2)
                    {
                        throw new InvalidDataException("Wide SSTable entry has an unknown key step.");
                    }

                    Key = ReadFullKey(block);
                    break;
            }

            Tag = (ulong)((long)Tag + SsTableCompactRowBlock.UnZigZag(SsTableWideRowBlock.ReadVarint(block, ref _position, _limit)));
        }

        ulong length = SsTableWideRowBlock.ReadVarint(block, ref _position, _limit);
        if (length > (ulong)(_limit - _position))
        {
            throw new InvalidDataException("Wide SSTable value points outside its block.");
        }

        ValueOffset = _position;
        ValueLength = (int)length;
        _position += ValueLength;
        return true;
    }

    /// <summary>Whether every entry has been decoded and the entries end exactly at the restart array.</summary>
    public readonly bool EndsAtRestarts => Index + 1 == _count && _position == _limit;

    private RowKey ReadFullKey(ReadOnlySpan<byte> block)
    {
        if (_position > _limit - SsTableWideRowBlock.KeySize)
        {
            throw new InvalidDataException("Wide SSTable entry key is truncated.");
        }

        RowKey key = SsTableV2Format.ReadKey(block[_position..]);
        _position += SsTableWideRowBlock.KeySize;
        return key;
    }
}
