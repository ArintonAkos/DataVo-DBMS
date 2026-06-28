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

    /// <summary>Synchronously consumes a MemTable enumerator and returns a complete SSTable byte image.</summary>
    public static byte[] Write(MemTable memTable)
    {
        ArgumentNullException.ThrowIfNull(memTable);

        var writer = new SsTableWriter(memTable.Count);
        foreach (MemTableEntry entry in memTable)
        {
            writer.Add(entry.InternalKey, entry.Value);
        }

        return writer.Finish();
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
