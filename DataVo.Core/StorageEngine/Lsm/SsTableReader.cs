using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Minimal v1 SSTable reader for point lookups against writer-produced byte images.</summary>
public sealed class SsTableReader
{
    private readonly byte[] _bytes;
    private readonly BloomFilter _filter;
    private readonly SsTableBlockHandle _dataBlock;
    private readonly byte[] _firstInternalKey;
    private readonly byte[] _lastInternalKey;

    private SsTableReader(
        byte[] bytes,
        BloomFilter filter,
        SsTableBlockHandle dataBlock,
        byte[] firstInternalKey,
        byte[] lastInternalKey)
    {
        _bytes = bytes;
        _filter = filter;
        _dataBlock = dataBlock;
        _firstInternalKey = firstInternalKey;
        _lastInternalKey = lastInternalKey;
    }

    /// <summary>Diagnostic count of data-block scans performed by this reader.</summary>
    public int DataBlocksScanned { get; private set; }

    /// <summary>Loads and validates the v1 SSTable footer, index block, and filter block.</summary>
    public static SsTableReader Load(ReadOnlyMemory<byte> source)
    {
        if (!SsTableFormat.TryReadFooter(source.Span, out SsTableBlockHandle indexBlock, out SsTableBlockHandle filterBlock))
        {
            throw new InvalidDataException("SSTable footer is missing, corrupt, or points outside the table.");
        }

        byte[] bytes = source.ToArray();
        IndexEntry indexEntry = ReadIndexBlock(SliceBlock(bytes, indexBlock));
        BloomFilter filter;
        try
        {
            filter = BloomFilter.FromBytes(SliceBlock(bytes, filterBlock));
        }
        catch (ArgumentException ex)
        {
            throw new InvalidDataException("SSTable filter block is corrupt.", ex);
        }

        ValidateDataBlock(SliceBlock(bytes, indexEntry.DataBlock), filter, indexEntry.FirstInternalKey, indexEntry.LastInternalKey);
        return new SsTableReader(
            bytes,
            filter,
            indexEntry.DataBlock,
            indexEntry.FirstInternalKey,
            indexEntry.LastInternalKey);
    }

    /// <summary>
    /// Looks up the newest visible version for <paramref name="userKey"/> at
    /// <paramref name="snapshotSeqno"/>. Returns false for absent keys and deletion markers.
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> userKey, ulong snapshotSeqno, out byte[] value, out bool isTombstone)
    {
        value = [];
        isTombstone = false;

        if (!_filter.MightContain(userKey))
        {
            return false;
        }

        if (UserKeyPrecedesFirst(userKey) || UserKeyFollowsLast(userKey))
        {
            return false;
        }

        DataBlocksScanned++;
        ReadOnlySpan<byte> dataBlock = SliceBlock(_bytes, _dataBlock);
        int offset = 0;
        while (offset < dataBlock.Length)
        {
            ReadEntry(dataBlock, ref offset, out ReadOnlySpan<byte> internalKey, out ReadOnlySpan<byte> entryValue);
            ReadOnlySpan<byte> entryUserKey = InternalKey.UserKey(internalKey);
            int byUserKey = entryUserKey.SequenceCompareTo(userKey);
            if (byUserKey < 0)
            {
                continue;
            }

            if (byUserKey > 0)
            {
                return false;
            }

            if (InternalKey.Sequence(internalKey) > snapshotSeqno)
            {
                continue;
            }

            LsmValueType valueType = InternalKey.ValueType(internalKey);
            if (valueType == LsmValueType.Deletion)
            {
                isTombstone = true;
                return false;
            }

            if (valueType != LsmValueType.Put)
            {
                throw new InvalidDataException($"Unsupported LSM value type {valueType}.");
            }

            value = entryValue.ToArray();
            return true;
        }

        return false;
    }

    private bool UserKeyPrecedesFirst(ReadOnlySpan<byte> userKey) =>
        userKey.SequenceCompareTo(InternalKey.UserKey(_firstInternalKey)) < 0;

    private bool UserKeyFollowsLast(ReadOnlySpan<byte> userKey) =>
        userKey.SequenceCompareTo(InternalKey.UserKey(_lastInternalKey)) > 0;

    private static IndexEntry ReadIndexBlock(ReadOnlySpan<byte> indexBlock)
    {
        int offset = 0;
        int count = ReadInt32(indexBlock, ref offset);
        if (count != 1)
        {
            throw new InvalidDataException($"SSTable v1 reader expects exactly one data block, found {count}.");
        }

        long dataOffset = ReadInt64(indexBlock, ref offset);
        int dataLength = ReadInt32(indexBlock, ref offset);
        int firstKeyLength = ReadInt32(indexBlock, ref offset);
        int lastKeyLength = ReadInt32(indexBlock, ref offset);
        if (firstKeyLength < InternalKey.TagSize || lastKeyLength < InternalKey.TagSize)
        {
            throw new InvalidDataException("SSTable index block contains a malformed internal key.");
        }

        byte[] firstKey = ReadBytes(indexBlock, ref offset, firstKeyLength);
        byte[] lastKey = ReadBytes(indexBlock, ref offset, lastKeyLength);
        if (offset != indexBlock.Length)
        {
            throw new InvalidDataException("SSTable index block contains trailing bytes.");
        }

        if (InternalKey.Compare(firstKey, lastKey) > 0)
        {
            throw new InvalidDataException("SSTable index key range is not ordered.");
        }

        return new IndexEntry(new SsTableBlockHandle(dataOffset, dataLength), firstKey, lastKey);
    }

    private static void ValidateDataBlock(
        ReadOnlySpan<byte> dataBlock,
        BloomFilter filter,
        ReadOnlySpan<byte> expectedFirstInternalKey,
        ReadOnlySpan<byte> expectedLastInternalKey)
    {
        int offset = 0;
        byte[]? previousInternalKey = null;
        byte[]? actualFirstInternalKey = null;
        while (offset < dataBlock.Length)
        {
            ReadEntry(dataBlock, ref offset, out ReadOnlySpan<byte> internalKey, out _);
            if (previousInternalKey is not null && InternalKey.Compare(previousInternalKey, internalKey) > 0)
            {
                throw new InvalidDataException("SSTable data block entries are not in internal-key order.");
            }

            LsmValueType valueType = InternalKey.ValueType(internalKey);
            if (valueType is not (LsmValueType.Put or LsmValueType.Deletion))
            {
                throw new InvalidDataException($"Unsupported LSM value type {valueType}.");
            }

            if (!filter.MightContain(InternalKey.UserKey(internalKey)))
            {
                throw new InvalidDataException("SSTable Bloom filter has a false negative for a data-block key.");
            }

            actualFirstInternalKey ??= internalKey.ToArray();
            previousInternalKey = internalKey.ToArray();
        }

        if (actualFirstInternalKey is null || previousInternalKey is null)
        {
            throw new InvalidDataException("SSTable data block is empty.");
        }

        if (!actualFirstInternalKey.AsSpan().SequenceEqual(expectedFirstInternalKey)
            || !previousInternalKey.AsSpan().SequenceEqual(expectedLastInternalKey))
        {
            throw new InvalidDataException("SSTable index key bounds do not match the data block.");
        }
    }

    private static void ReadEntry(
        ReadOnlySpan<byte> dataBlock,
        ref int offset,
        out ReadOnlySpan<byte> internalKey,
        out ReadOnlySpan<byte> value)
    {
        int keyLength = ReadInt32(dataBlock, ref offset);
        int valueLength = ReadInt32(dataBlock, ref offset);
        if (keyLength < InternalKey.TagSize || valueLength < 0)
        {
            throw new InvalidDataException("SSTable data block contains a malformed entry length.");
        }

        internalKey = ReadSpan(dataBlock, ref offset, keyLength);
        value = ReadSpan(dataBlock, ref offset, valueLength);
    }

    private static ReadOnlySpan<byte> SliceBlock(byte[] source, SsTableBlockHandle handle)
    {
        if (handle.Offset < 0
            || handle.Length <= 0
            || handle.Offset > int.MaxValue
            || handle.Offset + handle.Length > source.Length)
        {
            throw new InvalidDataException("SSTable block handle points outside the table.");
        }

        return source.AsSpan((int)handle.Offset, handle.Length);
    }

    private static int ReadInt32(ReadOnlySpan<byte> source, ref int offset)
    {
        ReadOnlySpan<byte> span = ReadSpan(source, ref offset, sizeof(int));
        return BinaryPrimitives.ReadInt32LittleEndian(span);
    }

    private static long ReadInt64(ReadOnlySpan<byte> source, ref int offset)
    {
        ReadOnlySpan<byte> span = ReadSpan(source, ref offset, sizeof(long));
        return BinaryPrimitives.ReadInt64LittleEndian(span);
    }

    private static byte[] ReadBytes(ReadOnlySpan<byte> source, ref int offset, int length) =>
        ReadSpan(source, ref offset, length).ToArray();

    private static ReadOnlySpan<byte> ReadSpan(ReadOnlySpan<byte> source, ref int offset, int length)
    {
        if (length < 0 || offset < 0 || offset > source.Length - length)
        {
            throw new InvalidDataException("SSTable block is truncated.");
        }

        ReadOnlySpan<byte> span = source.Slice(offset, length);
        offset += length;
        return span;
    }

    private sealed record IndexEntry(SsTableBlockHandle DataBlock, byte[] FirstInternalKey, byte[] LastInternalKey);
}
