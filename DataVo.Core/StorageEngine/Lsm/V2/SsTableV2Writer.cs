using System.Buffers;
using System.Buffers.Binary;
using DataVo.Core.Transactions;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>Data block encoding of a row-keyed SSTable.</summary>
internal enum SsTableRowFormat
{
    /// <summary>Version 2: fixed 24-byte entries, integer keys.</summary>
    Fixed = 2,

    /// <summary>Version 4: compact entries, integer keys (<see cref="SsTableCompactRowBlock"/>).</summary>
    Compact = 4,

    /// <summary>
    /// Version 6: 16-byte keys in the index and properties; blocks of integer keys use the v4 encoding,
    /// blocks of other keys the wide one (<see cref="SsTableWideRowBlock"/>).
    /// </summary>
    Wide = 6,
}

/// <summary>
/// Streams a row-keyed SSTable to <see cref="Stream"/> block by block: only the current block, the
/// block index (one record per block), and the Bloom keys (8 B per row) are held in memory,
/// never the whole file image.
/// </summary>
/// <remarks>
/// Entries must arrive sorted by row key ascending, then sequence descending (MemTable and merge order).
/// A block is closed when the next entry would not fit in the target size (the block alignment); a row
/// larger than that gets a block of its own, rounded up to whole pages.
/// </remarks>
internal sealed class SsTableV2Writer : IDisposable
{
    private static readonly byte[] Zeros = new byte[SsTableV2Format.MinimumAlignment];

    private readonly Stream _output;
    private readonly int _alignment;
    private readonly List<SsTableV2BlockInfo> _blocks = [];
    private readonly List<long> _bloomKeys;

    private byte[] _entries;
    private byte[] _values;
    private int _entryCount;
    private int _valuesLength;
    private RowKey _blockFirstKey;
    private long _position;

    private bool _hasLast;
    private RowKey _lastKey;
    private ulong _lastSequence;

    private RowKey _smallestKey;
    private RowKey _largestKey;
    private ulong _smallestSequence = ulong.MaxValue;
    private ulong _largestSequence;
    private long _entryTotal;
    private long _putCount;
    private long _tombstoneCount;
    private long _nextRowId;
    private ulong _nextRowIdSequence;
    private long _liveRowCount = SsTableV2Properties.UnknownLiveRowCount;
    private bool _hasMetadata;
    private bool _finished;

    // Compact (v4) and wide (v6) blocks: entries are encoded into _entries with their values inline.
    private readonly SsTableRowFormat _format;
    private readonly List<int> _restartOffsets = [];
    private int _entriesLength;
    private RowKey _previousKeyInBlock;
    private bool _blockWide; // the current block uses the wide encoding (v6 files, non-integer keys)
    private ulong _previousTagInBlock;

    /// <param name="format">
    /// The block encoding. The engine writes <see cref="SsTableRowFormat.Wide"/>; the integer-keyed
    /// formats (versions 2 and 4) remain for reading old files and for format tests.
    /// </param>
    public SsTableV2Writer(Stream output, int alignment = 0, int expectedEntries = 0, SsTableRowFormat format = SsTableRowFormat.Wide)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(output);
        if (!Enum.IsDefined(typeof(SsTableRowFormat), format))
        {
            throw new ArgumentOutOfRangeException(nameof(format), format, "Unknown row SSTable format.");
        }

        _output = output;
        _format = format;
        _alignment = alignment == 0 ? SsTableV2Format.DefaultAlignment : alignment;
        if (_alignment < SsTableV2Format.MinimumAlignment || (_alignment & (_alignment - 1)) != 0)
        {
            throw new ArgumentOutOfRangeException(nameof(alignment), alignment, "Block alignment must be a power of two of at least 4 KiB.");
        }

        _bloomKeys = new List<long>(Math.Max(0, expectedEntries));
        _entries = ArrayPool<byte>.Shared.Rent(_alignment);
        _values = ArrayPool<byte>.Shared.Rent(_alignment);
    }

    /// <summary>Total bytes written so far (the file length after <see cref="Finish"/>).</summary>
    public long Length => _position;

    public long EntryCount => _entryTotal;

    private ushort FileVersion => (ushort)_format;

    private bool Wide => _format == SsTableRowFormat.Wide;

    /// <summary>Appends one version of a row. Order: row key ascending, sequence descending.</summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveOptimization)]
    public void Add(RowKey key, ulong sequence, LsmValueType type, ReadOnlySpan<byte> value)
    {
        if (_finished)
        {
            throw new InvalidOperationException("Cannot add entries after Finish.");
        }

        if (sequence > InternalKey.MaxSequenceNumber)
        {
            throw new ArgumentOutOfRangeException(nameof(sequence));
        }

        if (_hasLast && (key < _lastKey || (key == _lastKey && sequence >= _lastSequence)))
        {
            throw new InvalidOperationException(
                $"SSTable entries must be sorted by row key ascending, then sequence descending (got {key}/{sequence} after {_lastKey}/{_lastSequence}).");
        }

        if (!Wide && !key.IsInt64)
        {
            throw new ArgumentException($"Row key {key} is not an integer; format version {FileVersion} stores integer keys only.", nameof(key));
        }

        if (type == LsmValueType.Deletion && !value.IsEmpty)
        {
            throw new ArgumentException("A deletion carries no value.", nameof(value));
        }

        ulong tag = SsTableV2Format.PackTag(sequence, type);
        switch (_format)
        {
            case SsTableRowFormat.Fixed:
                AddFixed(key.AsInt64(), tag, value);
                break;
            default:
                AddCompact(key, tag, value);
                break;
        }

        if (!_hasLast || key != _lastKey)
        {
            _bloomKeys.Add(Wide ? SsTableV2Format.WideBloomKey(key) : key.AsInt64());
        }

        if (!_hasLast)
        {
            _smallestKey = key; // entries ascend: the first key is the smallest, the last the largest
        }

        _hasLast = true;
        _lastKey = key;
        _lastSequence = sequence;
        _largestKey = key;
        _smallestSequence = Math.Min(_smallestSequence, sequence);
        _largestSequence = Math.Max(_largestSequence, sequence);
        _entryTotal++;
        if (type == LsmValueType.Put)
        {
            _putCount++;
        }
        else
        {
            _tombstoneCount++;
        }
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveOptimization)]
    private void AddCompact(RowKey key, ulong tag, ReadOnlySpan<byte> value)
    {
        // A v6 file stores integer keys in v4 blocks (they read as fast as v4 files) and other keys in
        // wide blocks. Keys ascend, so every integer key (high half zero) precedes every other key and the
        // encoding changes at most once, at a block boundary.
        bool keyWide = Wide && !key.IsInt64;
        if (_entryCount > 0 && keyWide != _blockWide)
        {
            FlushBlock();
        }

        if (_entryCount == 0)
        {
            _blockWide = keyWide;
        }

        bool restart = _entryCount % SsTableCompactRowBlock.RestartInterval == 0;
        int restarts = _restartOffsets.Count + (restart ? 1 : 0);
        long blockWithEntry = SsTableV2Format.BlockHeaderSize + _entriesLength + CompactEntrySize(key, tag, value.Length, restart) + ((restarts + 1) * sizeof(int));
        if (_entryCount > 0 && blockWithEntry > _alignment)
        {
            FlushBlock();
            restart = true;
        }

        if (_entryCount == 0)
        {
            _blockFirstKey = key;
        }

        int size = CompactEntrySize(key, tag, value.Length, restart);
        EnsureCapacity(ref _entries, _entriesLength + size);
        Span<byte> entry = _entries.AsSpan(_entriesLength, size);
        int written;
        if (restart)
        {
            _restartOffsets.Add(_entriesLength);
            if (_blockWide)
            {
                written = SsTableWideRowBlock.WriteRestartKey(entry, key);
            }
            else
            {
                BinaryPrimitives.WriteInt64LittleEndian(entry, key.AsInt64());
                written = sizeof(long);
            }

            written += SsTableCompactRowBlock.WriteVarint(entry[written..], tag);
        }
        else
        {
            written = _blockWide
                ? SsTableWideRowBlock.WriteStep(entry, _previousKeyInBlock, key)
                : SsTableCompactRowBlock.WriteVarint(entry, IntegerStep(_previousKeyInBlock, key));
            written += SsTableCompactRowBlock.WriteVarint(entry[written..], SsTableCompactRowBlock.ZigZag((long)tag - (long)_previousTagInBlock));
        }

        written += SsTableCompactRowBlock.WriteVarint(entry[written..], (ulong)value.Length);
        value.CopyTo(entry[written..]);
        _entriesLength += size;
        _entryCount++;
        _previousKeyInBlock = key;
        _previousTagInBlock = tag;
    }

    // v4 steps are unsigned differences of the integer ids (they wrap across the whole long range).
    private static ulong IntegerStep(RowKey previous, RowKey key) =>
        unchecked((ulong)key.AsInt64() - (ulong)previous.AsInt64());

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveOptimization)]
    private int CompactEntrySize(RowKey key, ulong tag, int valueLength, bool restart)
    {
        int keyAndTag;
        if (restart)
        {
            keyAndTag = (_blockWide ? SsTableWideRowBlock.RestartKeySize(key) : sizeof(long)) + SsTableCompactRowBlock.VarintSize(tag);
        }
        else
        {
            keyAndTag = (_blockWide
                    ? SsTableWideRowBlock.StepSize(_previousKeyInBlock, key)
                    : SsTableCompactRowBlock.VarintSize(IntegerStep(_previousKeyInBlock, key)))
                + SsTableCompactRowBlock.VarintSize(SsTableCompactRowBlock.ZigZag((long)tag - (long)_previousTagInBlock));
        }

        return keyAndTag + SsTableCompactRowBlock.VarintSize((ulong)valueLength) + valueLength;
    }

    private void AddFixed(long rowId, ulong tag, ReadOnlySpan<byte> value)
    {
        int payloadWithEntry = SsTableV2Format.BlockHeaderSize
            + ((_entryCount + 1) * SsTableV2Format.EntrySize)
            + _valuesLength
            + value.Length;
        if (_entryCount > 0 && payloadWithEntry > _alignment)
        {
            FlushBlock();
        }

        if (_entryCount == 0)
        {
            _blockFirstKey = rowId;
        }

        EnsureCapacity(ref _entries, (_entryCount + 1) * SsTableV2Format.EntrySize);
        EnsureCapacity(ref _values, _valuesLength + value.Length);

        Span<byte> entry = _entries.AsSpan(_entryCount * SsTableV2Format.EntrySize, SsTableV2Format.EntrySize);
        BinaryPrimitives.WriteInt64LittleEndian(entry[0..], rowId);
        BinaryPrimitives.WriteUInt64LittleEndian(entry[8..], tag);
        BinaryPrimitives.WriteInt32LittleEndian(entry[16..], _valuesLength); // rebased when the block is written
        BinaryPrimitives.WriteInt32LittleEndian(entry[20..], value.Length);
        value.CopyTo(_values.AsSpan(_valuesLength));
        _valuesLength += value.Length;
        _entryCount++;
    }

    /// <summary>
    /// Records the table's metadata as of <paramref name="sequence"/>: the next row id and the number of
    /// live rows (<see cref="SsTableV2Properties.UnknownLiveRowCount"/> when not known). The newest call wins.
    /// </summary>
    public void SetTableMetadata(long nextRowId, long liveRowCount, ulong sequence)
    {
        if (!_hasMetadata || sequence >= _nextRowIdSequence)
        {
            _nextRowId = nextRowId;
            _liveRowCount = liveRowCount;
            _nextRowIdSequence = sequence;
            _hasMetadata = true;
        }
    }

    /// <summary>Writes the remaining block, the index, the Bloom filter, properties, and the footer.</summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveOptimization)]
    public long Finish()
    {
        if (_finished)
        {
            throw new InvalidOperationException("Finish can only be called once.");
        }

        if (_entryTotal == 0)
        {
            throw new InvalidOperationException("Cannot finish an empty SSTable.");
        }

        _finished = true;
        if (_entryCount > 0)
        {
            FlushBlock();
        }

        // Block index.
        PadTo(8);
        long indexOffset = _position;
        int indexEntrySize = SsTableV2Format.IndexEntrySizeOf(FileVersion);
        int indexLength = SsTableV2Format.IndexHeaderSize + (_blocks.Count * indexEntrySize);
        byte[] index = ArrayPool<byte>.Shared.Rent(indexLength);
        try
        {
            BinaryPrimitives.WriteInt32LittleEndian(index.AsSpan(0), _blocks.Count);
            BinaryPrimitives.WriteInt32LittleEndian(index.AsSpan(4), 0);
            for (int i = 0; i < _blocks.Count; i++)
            {
                _blocks[i].WriteTo(index.AsSpan(SsTableV2Format.IndexHeaderSize + (i * indexEntrySize)), FileVersion);
            }

            Write(index.AsSpan(0, indexLength));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(index);
        }

        // Bloom filter: the header sits right before the 64-byte-aligned blocks.
        int bloomBlocks = BlockedBloomFilter.ComputeBlockCount(_bloomKeys.Count, SsTableV2Format.BloomBitsPerKey);
        int bloomBytes = bloomBlocks * BlockedBloomFilter.BlockBytes;
        PadTo(BlockedBloomFilter.BlockBytes);
        PadBytes(BlockedBloomFilter.BlockBytes - SsTableV2Format.BloomHeaderSize);
        long bloomOffset = _position;
        byte[] bloom = ArrayPool<byte>.Shared.Rent(SsTableV2Format.BloomHeaderSize + bloomBytes);
        try
        {
            Span<byte> bloomSpan = bloom.AsSpan(0, SsTableV2Format.BloomHeaderSize + bloomBytes);
            bloomSpan.Clear();
            BinaryPrimitives.WriteInt32LittleEndian(bloomSpan[0..], bloomBlocks);
            BinaryPrimitives.WriteInt32LittleEndian(bloomSpan[4..], BlockedBloomFilter.ProbeCount);
            Span<byte> bits = bloomSpan[SsTableV2Format.BloomHeaderSize..];
            foreach (long key in _bloomKeys)
            {
                BlockedBloomFilter.Add(bits, key);
            }

            Write(bloomSpan);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bloom);
        }

        int bloomLength = SsTableV2Format.BloomHeaderSize + bloomBytes;

        // Properties.
        PadTo(8);
        long propertiesOffset = _position;
        Span<byte> properties = stackalloc byte[SsTableV2Format.PropertiesSizeOf(FileVersion)];
        new SsTableV2Properties(
            _smallestKey,
            _largestKey,
            _smallestSequence,
            _largestSequence,
            _entryTotal,
            _putCount,
            _tombstoneCount,
            _nextRowId,
            _nextRowIdSequence,
            _blocks.Count,
            _liveRowCount).WriteTo(properties, FileVersion);
        Write(properties);

        // Footer.
        Span<byte> footer = stackalloc byte[SsTableV2Format.FooterSize];
        WriteHandle(footer[0..], indexOffset, indexLength);
        WriteHandle(footer[12..], bloomOffset, bloomLength);
        WriteHandle(footer[24..], propertiesOffset, properties.Length);
        BinaryPrimitives.WriteInt32LittleEndian(footer[36..], _alignment);
        BinaryPrimitives.WriteUInt16LittleEndian(footer[40..], FileVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(footer[42..], SsTableV2Format.Magic);
        Write(footer);
        _output.Flush();
        return _position;
    }

    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(_entries);
        ArrayPool<byte>.Shared.Return(_values);
        _entries = [];
        _values = [];
    }

    private void FlushBlock()
    {
        if (_format != SsTableRowFormat.Fixed)
        {
            FlushCompactBlock();
            return;
        }

        int entriesLength = _entryCount * SsTableV2Format.EntrySize;
        int valuesStart = SsTableV2Format.BlockHeaderSize + entriesLength;
        int payloadLength = entriesLength + _valuesLength;
        int blockLength = SsTableV2Format.BlockHeaderSize + payloadLength;

        // Rebase value offsets from "within the values buffer" to "from the block start".
        for (int i = 0; i < _entryCount; i++)
        {
            Span<byte> offsetField = _entries.AsSpan((i * SsTableV2Format.EntrySize) + 16, sizeof(int));
            BinaryPrimitives.WriteInt32LittleEndian(offsetField, BinaryPrimitives.ReadInt32LittleEndian(offsetField) + valuesStart);
        }

        byte[] block = ArrayPool<byte>.Shared.Rent(blockLength);
        try
        {
            Span<byte> span = block.AsSpan(0, blockLength);
            BinaryPrimitives.WriteInt32LittleEndian(span[4..], _entryCount);
            BinaryPrimitives.WriteInt32LittleEndian(span[8..], 0); // flags (reserved for compression)
            BinaryPrimitives.WriteInt32LittleEndian(span[12..], payloadLength);
            _entries.AsSpan(0, entriesLength).CopyTo(span[SsTableV2Format.BlockHeaderSize..]);
            _values.AsSpan(0, _valuesLength).CopyTo(span[valuesStart..]);
            BinaryPrimitives.WriteUInt32LittleEndian(span[0..], WalCrc32C.HashToUInt32(span[4..]));

            long blockOffset = _position;
            Write(span);
            PadTo(_alignment);

            long lastRowId = BinaryPrimitives.ReadInt64LittleEndian(_entries.AsSpan((_entryCount - 1) * SsTableV2Format.EntrySize));
            _blocks.Add(new SsTableV2BlockInfo(_blockFirstKey, lastRowId, blockOffset, blockLength, _entryCount));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(block);
        }

        _entryCount = 0;
        _valuesLength = 0;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveOptimization)]
    private void FlushCompactBlock()
    {
        int payloadLength = _entriesLength + ((_restartOffsets.Count + 1) * sizeof(int));
        int blockLength = SsTableV2Format.BlockHeaderSize + payloadLength;
        byte[] block = ArrayPool<byte>.Shared.Rent(blockLength);
        try
        {
            Span<byte> span = block.AsSpan(0, blockLength);
            BinaryPrimitives.WriteInt32LittleEndian(span[4..], _entryCount);
            BinaryPrimitives.WriteInt32LittleEndian(span[8..], _blockWide ? SsTableWideRowBlock.WideFlag : SsTableCompactRowBlock.CompactFlag);
            BinaryPrimitives.WriteInt32LittleEndian(span[12..], payloadLength);
            _entries.AsSpan(0, _entriesLength).CopyTo(span[SsTableV2Format.BlockHeaderSize..]);
            int restarts = SsTableV2Format.BlockHeaderSize + _entriesLength;
            for (int i = 0; i < _restartOffsets.Count; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(span[(restarts + (i * sizeof(int)))..], SsTableV2Format.BlockHeaderSize + _restartOffsets[i]);
            }

            BinaryPrimitives.WriteInt32LittleEndian(span[^sizeof(int)..], _restartOffsets.Count);
            BinaryPrimitives.WriteUInt32LittleEndian(span[0..], WalCrc32C.HashToUInt32(span[4..]));

            long blockOffset = _position;
            Write(span);
            PadTo(_alignment);
            _blocks.Add(new SsTableV2BlockInfo(_blockFirstKey, _previousKeyInBlock, blockOffset, blockLength, _entryCount));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(block);
        }

        _entryCount = 0;
        _entriesLength = 0;
        _restartOffsets.Clear();
    }

    private void Write(ReadOnlySpan<byte> bytes)
    {
        _output.Write(bytes);
        _position += bytes.Length;
    }

    private void PadTo(int alignment) => PadBytes((int)(SsTableV2Format.AlignUp(_position, alignment) - _position));

    private void PadBytes(int count)
    {
        while (count > 0)
        {
            int chunk = Math.Min(count, Zeros.Length);
            Write(Zeros.AsSpan(0, chunk));
            count -= chunk;
        }
    }

    private static void WriteHandle(Span<byte> destination, long offset, int length)
    {
        BinaryPrimitives.WriteInt64LittleEndian(destination[0..], offset);
        BinaryPrimitives.WriteInt32LittleEndian(destination[8..], length);
    }

    private static void EnsureCapacity(ref byte[] buffer, int required)
    {
        if (buffer.Length >= required)
        {
            return;
        }

        byte[] grown = ArrayPool<byte>.Shared.Rent(Math.Max(required, buffer.Length * 2));
        buffer.AsSpan().CopyTo(grown);
        ArrayPool<byte>.Shared.Return(buffer);
        buffer = grown;
    }
}
