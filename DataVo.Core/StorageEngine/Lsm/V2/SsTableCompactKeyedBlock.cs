using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Compact encoding of a keyed (index) data block, format version 5: each key stores only the bytes it
/// does not share with the previous key, with a full key every <see cref="RestartInterval"/> entries
/// so lookups still binary search (the LevelDB/RocksDB block format).
/// </summary>
/// <remarks>
/// <code>
/// payload := entry* | u32 restart offset[restarts] | u32 restarts
/// entry   := varint shared | varint unshared | varint value length | varint tag (restart) or
///            varint zigzag(tag delta) | unshared key bytes | value
/// </code>
/// Restart entries have shared = 0, so their key bytes are contiguous and can be compared in place.
/// Index keys of neighbouring entries usually differ only in their last bytes: a 19-byte integer key
/// plus its 8-byte tag and 4-byte offset (37 bytes in v3) shrink to about 6 bytes.
/// </remarks>
internal static class SsTableCompactKeyedBlock
{
    public const int RestartInterval = 16;
    public const int CompactFlag = 1;

    public static bool IsCompact(ReadOnlySpan<byte> block) =>
        (BinaryPrimitives.ReadInt32LittleEndian(block[8..]) & CompactFlag) != 0;

    public static int RestartCount(ReadOnlySpan<byte> block) =>
        BinaryPrimitives.ReadInt32LittleEndian(block[^sizeof(int)..]);

    public static int RestartsStart(ReadOnlySpan<byte> block) =>
        block.Length - sizeof(int) - (RestartCount(block) * sizeof(int));

    public static int RestartOffset(ReadOnlySpan<byte> block, int restart) =>
        BinaryPrimitives.ReadInt32LittleEndian(block[(RestartsStart(block) + (restart * sizeof(int)))..]);

    /// <summary>The full key of a restart entry, read in place.</summary>
    public static ReadOnlySpan<byte> RestartKey(ReadOnlySpan<byte> block, int restart)
    {
        int position = RestartOffset(block, restart);
        int limit = RestartsStart(block);
        SsTableCompactRowBlock.ReadVarint(block, ref position, limit); // shared (0)
        int length = checked((int)SsTableCompactRowBlock.ReadVarint(block, ref position, limit));
        SsTableCompactRowBlock.ReadVarint(block, ref position, limit); // value length
        SsTableCompactRowBlock.ReadVarint(block, ref position, limit); // tag
        if (length > limit - position)
        {
            throw new InvalidDataException("Compact keyed SSTable restart key points outside its block.");
        }

        return block.Slice(position, length);
    }
}

/// <summary>
/// Sequential decoder over one compact keyed block. Keys are rebuilt into <see cref="Key"/>, which the
/// next <see cref="MoveNext"/> overwrites: copy a key that must outlive the step.
/// </summary>
internal struct CompactKeyedBlockReader
{
    private int _position;
    private int _limit;
    private int _count;
    private byte[] _key;

    public int Index;
    public int EntryOffset;
    public int KeyLength;
    public ulong Tag;
    public int ValueOffset;
    public int ValueLength;

    public readonly ReadOnlySpan<byte> Key => _key.AsSpan(0, KeyLength);

    public void Seek(ReadOnlySpan<byte> block, int restart)
    {
        _key ??= new byte[64];
        _count = BinaryPrimitives.ReadInt32LittleEndian(block[4..]);
        _limit = SsTableCompactKeyedBlock.RestartsStart(block);
        _position = SsTableCompactKeyedBlock.RestartOffset(block, restart);
        Index = (restart * SsTableCompactKeyedBlock.RestartInterval) - 1;
        KeyLength = 0;
    }

    public bool MoveNext(ReadOnlySpan<byte> block)
    {
        if (Index + 1 >= _count)
        {
            return false;
        }

        Index++;
        EntryOffset = _position;
        bool restart = Index % SsTableCompactKeyedBlock.RestartInterval == 0;
        ulong shared = SsTableCompactRowBlock.ReadVarint(block, ref _position, _limit);
        ulong unshared = SsTableCompactRowBlock.ReadVarint(block, ref _position, _limit);
        ulong valueLength = SsTableCompactRowBlock.ReadVarint(block, ref _position, _limit);
        ulong tag = SsTableCompactRowBlock.ReadVarint(block, ref _position, _limit);
        if ((restart && shared != 0)
            || shared > (ulong)KeyLength
            || unshared > (ulong)(_limit - _position)
            || shared + unshared > ushort.MaxValue)
        {
            throw new InvalidDataException("Compact keyed SSTable entry key is corrupt.");
        }

        Tag = restart ? tag : (ulong)((long)Tag + SsTableCompactRowBlock.UnZigZag(tag));
        int keyLength = (int)(shared + unshared);
        if (keyLength > _key.Length)
        {
            Array.Resize(ref _key, Math.Max(keyLength, _key.Length * 2));
        }

        block.Slice(_position, (int)unshared).CopyTo(_key.AsSpan((int)shared));
        KeyLength = keyLength;
        _position += (int)unshared;
        if (valueLength > (ulong)(_limit - _position))
        {
            throw new InvalidDataException("Compact keyed SSTable value points outside its block.");
        }

        ValueOffset = _position;
        ValueLength = (int)valueLength;
        _position += ValueLength;
        return true;
    }

    public readonly bool EndsAtRestarts => Index + 1 == _count && _position == _limit;
}
