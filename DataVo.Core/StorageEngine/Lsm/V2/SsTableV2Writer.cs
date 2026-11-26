using System.Buffers;
using System.Buffers.Binary;
using DataVo.Core.Transactions;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Streams a row-keyed (v6 or v8) SSTable to <see cref="Stream"/> block by block: only the current block, the
/// block index (one record per block), and the Bloom keys (8 B per row) are held in memory,
/// never the whole file image. With <see cref="SsTableSpillOptions"/>, the Bloom keys and the columnar
/// segment move to temporary files once they outgrow a limit, and only the Bloom filter itself
/// (about 1.25 B per row) is built in memory, at <see cref="Finish"/>.
/// </summary>
/// <remarks>
/// Entries must arrive sorted by row key ascending, then sequence descending (MemTable and merge order).
/// Blocks of integer keys use the integer encoding (<see cref="SsTableCompactRowBlock"/>), blocks of other
/// keys the wide one (<see cref="SsTableWideRowBlock"/>).
/// A block is closed when the next entry would not fit in the target size (the block alignment); a row
/// larger than that gets a block of its own, rounded up to whole pages.
/// </remarks>
internal sealed class SsTableV2Writer : IDisposable
{
    private static readonly byte[] Zeros = new byte[SsTableV2Format.MinimumAlignment];

    private readonly Stream _output;
    private readonly int _alignment;
    private readonly List<SsTableV2BlockInfo> _blocks = [];
    private readonly SsTableSpillBuffer _bloomKeys;
    private long _bloomKeyCount;

    private byte[] _entries;
    private int _entryCount;
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
    private long _liveRowCount;
    private bool _hasMetadata;
    private bool _finished;

    // Entries are encoded into _entries with their values inline.
    private readonly List<int> _restartOffsets = [];
    private int _entriesLength;
    private RowKey _previousKeyInBlock;
    private bool _blockWide; // the current block uses the wide encoding (non-integer keys)
    private ulong _previousTagInBlock;

    // Set for a version 8 file: collects the columnar segment written after the last data block.
    private readonly SsTableColumnarWriter? _columnar;

    /// <param name="output">The stream the file is written to.</param>
    /// <param name="alignment">The block size; 0 for the default.</param>
    /// <param name="columnar">
    /// The columns to write a second time as a columnar segment, which makes the file a version 8 file; null
    /// for a version 6 file without one.
    /// </param>
    /// <param name="spill">Where to keep what outgrows memory until <see cref="Finish"/>; null keeps it all in memory.</param>
    public SsTableV2Writer(
        Stream output,
        int alignment = 0,
        DataVo.Core.Indexing.Columnar.ColumnarSpec? columnar = null,
        SsTableSpillOptions? spill = null)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(output);
        _output = output;
        _columnar = columnar is null ? null : new SsTableColumnarWriter(columnar, spill);
        _alignment = alignment == 0 ? SsTableV2Format.DefaultAlignment : alignment;
        if (_alignment < SsTableV2Format.MinimumAlignment || (_alignment & (_alignment - 1)) != 0)
        {
            throw new ArgumentOutOfRangeException(nameof(alignment), alignment, "Block alignment must be a power of two of at least 4 KiB.");
        }

        _bloomKeys = new SsTableSpillBuffer(spill);
        _entries = ArrayPool<byte>.Shared.Rent(_alignment);
    }

    /// <summary>Total bytes written so far (the file length after <see cref="Finish"/>).</summary>
    public long Length => _position;

    public long EntryCount => _entryTotal;

    private ushort FileVersion => _columnar is null ? SsTableV2Format.WideVersion : SsTableV2Format.ColumnarVersion;

    /// <summary>Appends one version of a row. Order: row key ascending, sequence descending.</summary>
    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
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

        if (type == LsmValueType.Deletion && !value.IsEmpty)
        {
            throw new ArgumentException("A deletion carries no value.", nameof(value));
        }

        AddEntry(key, SsTableV2Format.PackTag(sequence, type), value);

        if (!_hasLast || key != _lastKey)
        {
            AddBloomKey(SsTableV2Format.WideBloomKey(key));
            _columnar?.Add(key, type, value); // entries descend by sequence: the first of a key is its newest
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

    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
    private void AddEntry(RowKey key, ulong tag, ReadOnlySpan<byte> value)
    {
        // Integer keys go in integer blocks (8-byte restart keys, varint steps) and other keys in wide
        // blocks. Keys ascend, so every integer key (high half zero) precedes every other key and the
        // encoding changes at most once, at a block boundary.
        bool keyWide = !key.IsInt64;
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

    // Integer-block steps are unsigned differences of the integer ids (they wrap across the whole long range).
    private static ulong IntegerStep(RowKey previous, RowKey key) =>
        unchecked((ulong)key.AsInt64() - (ulong)previous.AsInt64());

    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
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

    /// <summary>
    /// Records the table's metadata as of <paramref name="sequence"/>: the next row id and the number of
    /// live rows. The newest call wins.
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
    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
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

        (long columnarOffset, int columnarLength) = WriteColumnarSegment();

        // Block index.
        PadTo(8);
        long indexOffset = _position;
        const int indexEntrySize = SsTableV2Format.IndexEntrySize;
        int indexLength = SsTableV2Format.IndexHeaderSize + (_blocks.Count * indexEntrySize);
        byte[] index = ArrayPool<byte>.Shared.Rent(indexLength);
        try
        {
            BinaryPrimitives.WriteInt32LittleEndian(index.AsSpan(0), _blocks.Count);
            BinaryPrimitives.WriteInt32LittleEndian(index.AsSpan(4), 0);
            for (int i = 0; i < _blocks.Count; i++)
            {
                _blocks[i].WriteTo(index.AsSpan(SsTableV2Format.IndexHeaderSize + (i * indexEntrySize)));
            }

            Write(index.AsSpan(0, indexLength));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(index);
        }

        // Bloom filter: the header sits right before the 64-byte-aligned blocks.
        int bloomBlocks = BlockedBloomFilter.ComputeBlockCount(checked((int)_bloomKeyCount), SsTableV2Format.BloomBitsPerKey);
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
            AddBloomKeysTo(bloomSpan[SsTableV2Format.BloomHeaderSize..]);

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
        Span<byte> properties = stackalloc byte[SsTableV2Format.PropertiesSize];
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
            _liveRowCount).WriteTo(properties);
        Write(properties);

        // Footer.
        Span<byte> footer = stackalloc byte[SsTableV2Format.FooterSizeOf(FileVersion)];
        WriteHandle(footer[0..], indexOffset, indexLength);
        WriteHandle(footer[12..], bloomOffset, bloomLength);
        WriteHandle(footer[24..], propertiesOffset, properties.Length);
        Span<byte> tail = footer[(SsTableV2Format.BlockHandleSize * 3)..];
        if (_columnar is not null)
        {
            WriteHandle(tail, columnarOffset, columnarLength);
            tail = tail[SsTableV2Format.BlockHandleSize..];
        }

        BinaryPrimitives.WriteInt32LittleEndian(tail[0..], _alignment);
        BinaryPrimitives.WriteUInt16LittleEndian(tail[4..], FileVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(tail[6..], SsTableV2Format.Magic);
        Write(footer);
        _output.Flush();
        return _position;
    }

    // Writes the columnar segment of a version 8 file after the data blocks and returns its directory's handle.
    private (long Offset, int Length) WriteColumnarSegment()
    {
        if (_columnar is null)
        {
            return (0, 0);
        }

        PadTo(SsTableColumnarFormat.Alignment);
        (long offset, int length, long end) = _columnar.WriteTo(_output, _position);
        _position = end;
        return (offset, length);
    }

    // Bloom keys are kept as raw little-endian longs, in memory or spilled, until the filter is sized.
    private void AddBloomKey(long key)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, key);
        _bloomKeys.Write(bytes);
        _bloomKeyCount++;
    }

    private void AddBloomKeysTo(Span<byte> bits)
    {
        _bloomKeys.Rewind();
        byte[] buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            int read;
            while ((read = _bloomKeys.Read(buffer.AsSpan(0, 64 * 1024))) > 0)
            {
                for (int at = 0; at < read; at += sizeof(long))
                {
                    BlockedBloomFilter.Add(bits, BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(at)));
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public void Dispose()
    {
        _bloomKeys.Dispose();
        _columnar?.Dispose();
        ArrayPool<byte>.Shared.Return(_entries);
        _entries = [];
    }

    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
    private void FlushBlock()
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
