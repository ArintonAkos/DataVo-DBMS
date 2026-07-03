using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Builds a v1 SSTable byte image from copied LSM internal-key/value entries.</summary>
public sealed class SsTableWriter
{
    private readonly List<Entry> _entries;
    private readonly int _bitsPerKey;
    private bool _finished;

    /// <summary>Creates an SSTable writer sized for the expected entry count and Bloom filter density.</summary>
    public SsTableWriter(int expectedEntries = 0, int bitsPerKey = 10)
    {
        if (expectedEntries < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(expectedEntries));
        }

        if (bitsPerKey <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bitsPerKey));
        }

        _entries = new List<Entry>(expectedEntries);
        _bitsPerKey = bitsPerKey;
    }

    /// <summary>Copies one entry into the writer. The input spans are not retained.</summary>
    public void Add(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    {
        if (_finished)
        {
            throw new InvalidOperationException("Cannot add entries after Finish.");
        }

        if (internalKey.Length < InternalKey.TagSize)
        {
            throw new ArgumentException("Internal key must include the tag trailer.", nameof(internalKey));
        }

        _entries.Add(new Entry(internalKey.ToArray(), value.ToArray()));
    }

    /// <summary>Returns a complete SSTable byte image: data block, index block, filter block, footer.</summary>
    public byte[] Finish()
    {
        if (_finished)
        {
            throw new InvalidOperationException("Finish can only be called once.");
        }

        _finished = true;
        if (_entries.Count == 0)
        {
            throw new InvalidOperationException("Cannot finish an empty SSTable.");
        }

        _entries.Sort(static (left, right) => InternalKey.Compare(left.InternalKey, right.InternalKey));

        int dataLength = 0;
        foreach (Entry entry in _entries)
        {
            dataLength = checked(dataLength + sizeof(int) + sizeof(int) + entry.InternalKey.Length + entry.Value.Length);
        }

        byte[] firstKey = _entries[0].InternalKey;
        byte[] lastKey = _entries[^1].InternalKey;
        int indexLength = checked(
            sizeof(int)
            + SsTableFormat.BlockHandleSize
            + sizeof(int)
            + sizeof(int)
            + firstKey.Length
            + lastKey.Length);

        BloomFilter filter = BloomFilter.Create(_entries.Count, _bitsPerKey);
        foreach (Entry entry in _entries)
        {
            filter.Add(InternalKey.UserKey(entry.InternalKey));
        }

        byte[] filterBytes = filter.ToBytes();
        int totalLength = checked(dataLength + indexLength + filterBytes.Length + SsTableFormat.FooterSize);
        byte[] sstable = new byte[totalLength];

        int offset = 0;
        WriteDataBlock(sstable.AsSpan(offset, dataLength));
        var dataBlock = new SsTableBlockHandle(offset, dataLength);
        offset += dataLength;

        WriteIndexBlock(sstable.AsSpan(offset, indexLength), dataBlock, firstKey, lastKey);
        var indexBlock = new SsTableBlockHandle(offset, indexLength);
        offset += indexLength;

        filterBytes.CopyTo(sstable.AsSpan(offset));
        var filterBlock = new SsTableBlockHandle(offset, filterBytes.Length);
        offset += filterBytes.Length;

        SsTableFormat.WriteFooter(sstable.AsSpan(offset, SsTableFormat.FooterSize), indexBlock, filterBlock);
        return sstable;
    }

    /// <summary>
    /// Synchronously streams a MemTable into a complete SSTable byte image. The skiplist's level-0
    /// chain is already in <see cref="InternalKey"/> order, so entries are measured and written
    /// directly from arena memory — no per-entry key/value copies and no re-sort. Byte-identical to
    /// adding every entry to a writer and calling <see cref="Finish"/>.
    /// </summary>
    public static byte[] Write(MemTable memTable) =>
        WriteCore(memTable, static required => new byte[required], out _);

    /// <summary>
    /// Same as <see cref="Write"/> but into a buffer rented from
    /// <see cref="System.Buffers.ArrayPool{T}.Shared"/> (which may be longer than the image).
    /// The caller owns the buffer and must return it to the pool; <paramref name="length"/> is the
    /// image's exact byte count.
    /// </summary>
    internal static byte[] WriteRented(MemTable memTable, out int length) =>
        WriteCore(memTable, static required => System.Buffers.ArrayPool<byte>.Shared.Rent(required), out length);

    private static byte[] WriteCore(MemTable memTable, Func<int, byte[]> allocate, out int length)
    {
        ArgumentNullException.ThrowIfNull(memTable);
        if (memTable.Count == 0)
        {
            throw new InvalidOperationException("Cannot finish an empty SSTable.");
        }

        // Pass 1 — measure the data block, note key bound lengths, and populate the Bloom filter.
        BloomFilter filter = BloomFilter.Create(memTable.Count, bitsPerKey: 10);
        int dataLength = 0;
        int firstKeyLength = 0;
        int lastKeyLength = 0;
        foreach (MemTableEntry entry in memTable)
        {
            if (firstKeyLength == 0)
            {
                firstKeyLength = entry.InternalKey.Length;
            }

            lastKeyLength = entry.InternalKey.Length;
            dataLength = checked(dataLength + sizeof(int) + sizeof(int) + entry.InternalKey.Length + entry.Value.Length);
            filter.Add(InternalKey.UserKey(entry.InternalKey));
        }

        byte[] filterBytes = filter.ToBytes();
        int indexLength = checked(
            sizeof(int)
            + SsTableFormat.BlockHandleSize
            + sizeof(int)
            + sizeof(int)
            + firstKeyLength
            + lastKeyLength);
        int totalLength = checked(dataLength + indexLength + filterBytes.Length + SsTableFormat.FooterSize);
        length = totalLength;
        byte[] sstable = allocate(totalLength);

        // Pass 2 — write the data block in enumeration (= key) order, tracking the last key's offset.
        int offset = 0;
        int lastKeyOffset = 0;
        foreach (MemTableEntry entry in memTable)
        {
            BinaryPrimitives.WriteInt32LittleEndian(sstable.AsSpan(offset, sizeof(int)), entry.InternalKey.Length);
            offset += sizeof(int);
            BinaryPrimitives.WriteInt32LittleEndian(sstable.AsSpan(offset, sizeof(int)), entry.Value.Length);
            offset += sizeof(int);
            lastKeyOffset = offset;
            entry.InternalKey.CopyTo(sstable.AsSpan(offset, entry.InternalKey.Length));
            offset += entry.InternalKey.Length;
            entry.Value.CopyTo(sstable.AsSpan(offset, entry.Value.Length));
            offset += entry.Value.Length;
        }

        var dataBlock = new SsTableBlockHandle(0, dataLength);
        ReadOnlySpan<byte> firstKey = sstable.AsSpan(sizeof(int) + sizeof(int), firstKeyLength);
        ReadOnlySpan<byte> lastKey = sstable.AsSpan(lastKeyOffset, lastKeyLength);
        WriteIndexBlock(sstable.AsSpan(dataLength, indexLength), dataBlock, firstKey, lastKey);
        var indexBlock = new SsTableBlockHandle(dataLength, indexLength);

        int filterOffset = dataLength + indexLength;
        filterBytes.CopyTo(sstable.AsSpan(filterOffset));
        var filterBlock = new SsTableBlockHandle(filterOffset, filterBytes.Length);

        SsTableFormat.WriteFooter(
            sstable.AsSpan(filterOffset + filterBytes.Length, SsTableFormat.FooterSize),
            indexBlock,
            filterBlock);
        return sstable;
    }

    private void WriteDataBlock(Span<byte> destination)
    {
        int offset = 0;
        foreach (Entry entry in _entries)
        {
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset, sizeof(int)), entry.InternalKey.Length);
            offset += sizeof(int);
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset, sizeof(int)), entry.Value.Length);
            offset += sizeof(int);
            entry.InternalKey.CopyTo(destination.Slice(offset, entry.InternalKey.Length));
            offset += entry.InternalKey.Length;
            entry.Value.CopyTo(destination.Slice(offset, entry.Value.Length));
            offset += entry.Value.Length;
        }
    }

    private static void WriteIndexBlock(
        Span<byte> destination,
        SsTableBlockHandle dataBlock,
        ReadOnlySpan<byte> firstInternalKey,
        ReadOnlySpan<byte> lastInternalKey)
    {
        int offset = 0;
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset, sizeof(int)), 1);
        offset += sizeof(int);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(offset, sizeof(long)), dataBlock.Offset);
        offset += sizeof(long);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset, sizeof(int)), dataBlock.Length);
        offset += sizeof(int);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset, sizeof(int)), firstInternalKey.Length);
        offset += sizeof(int);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset, sizeof(int)), lastInternalKey.Length);
        offset += sizeof(int);
        firstInternalKey.CopyTo(destination.Slice(offset, firstInternalKey.Length));
        offset += firstInternalKey.Length;
        lastInternalKey.CopyTo(destination.Slice(offset, lastInternalKey.Length));
    }

    private sealed record Entry(byte[] InternalKey, byte[] Value);
}
