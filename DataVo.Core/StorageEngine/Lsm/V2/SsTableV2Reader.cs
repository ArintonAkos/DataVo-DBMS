using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using DataVo.Core.Transactions;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>Receives the newest visible version of a row; the span is valid only during the call.</summary>
internal delegate TResult RowVersionFunc<TState, TResult>(LsmValueType type, ulong sequence, ReadOnlySpan<byte> value, TState state);

/// <summary>
/// Reads a row-keyed SSTable (v6 or v8) through a <see cref="SsTableBlockSource"/>. Opening validates the footer, block
/// index, Bloom filter, and properties (O(metadata)); no data block is read until a lookup needs it.
/// </summary>
/// <remarks>
/// A point lookup costs: a key range check, one Bloom probe (one cache line), a binary search over the
/// block index (L1/L2-resident), and a binary search over the block's restart points. Each block's
/// CRC32C is verified the first time any reader touches it.
/// </remarks>
internal sealed class SsTableV2Reader : IDisposable, ISsTableBlockVerifier
{
    private readonly SsTableBlockSource _source;
    private readonly ReadOnlyMemory<byte> _index;
    private readonly ReadOnlyMemory<byte> _bloomBits;
    private readonly int[] _verifiedBlocks;
    private readonly bool _ownsSource;

    // The last key of every block, decoded once at open (16 B per block; a 1M-row file has ~2K
    // blocks), so the block search compares keys instead of decoding index records on every step.
    private readonly RowKey[] _blockLastKeys;

    private SsTableV2Reader(
        SsTableBlockSource source,
        bool ownsSource,
        ReadOnlyMemory<byte> index,
        ReadOnlyMemory<byte> bloomBits,
        SsTableV2Properties properties,
        int alignment,
        ushort version,
        SsTableColumnarSegment? columnar)
    {
        _source = source;
        Columnar = columnar;
        Version = version;
        _blockLastKeys = new RowKey[properties.BlockCount];
        ReadOnlySpan<byte> records = index.Span;
        for (int block = 0; block < _blockLastKeys.Length; block++)
        {
            _blockLastKeys[block] = SsTableV2Format.ReadKey(records[(SsTableV2Format.IndexHeaderSize + (block * SsTableV2Format.IndexEntrySize) + SsTableWideRowBlock.KeySize)..]);
        }

        _ownsSource = ownsSource;
        _index = index;
        _bloomBits = bloomBits;
        Properties = properties;
        Alignment = alignment;
        _verifiedBlocks = new int[(properties.BlockCount + 31) / 32];
    }

    public SsTableV2Properties Properties { get; }

    /// <summary>The file's columnar segment, or null when it has none (a version 6 file).</summary>
    public SsTableColumnarSegment? Columnar { get; }

    /// <summary>The file's format version (6 or 8).</summary>
    public ushort Version { get; }

    public int Alignment { get; }

    public int BlockCount => Properties.BlockCount;

    public bool IsMapped => _source.IsMapped;

    public long FileLength => _source.Length;

    /// <summary>Opens <paramref name="path"/> with the given read mode.</summary>
    public static SsTableV2Reader Open(string path, LsmReadMode mode = LsmReadMode.Auto, LsmBlockCache? cache = null)
    {
        SsTableBlockSource source = SsTableBlockSource.Open(path, mode, cache);
        try
        {
            return Open(source, ownsSource: true);
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    /// <summary>Validates the metadata of <paramref name="source"/> and returns a reader over it.</summary>
    public static SsTableV2Reader Open(SsTableBlockSource source, bool ownsSource)
    {
        long length = source.Length;
        if (length < SsTableV2Format.FooterSize)
        {
            throw new InvalidDataException("SSTable is shorter than its footer.");
        }

        const int TailSize = sizeof(ushort) + sizeof(uint);
        if (!SsTableV2Format.TryReadVersion(source.GetRegion(length - TailSize, TailSize).Span, out ushort version)
            || !SsTableV2Format.IsRowKeyedVersion(version)
            || length < SsTableV2Format.FooterSizeOf(version))
        {
            throw new InvalidDataException("SSTable footer is not a row-keyed (v6 or v8) footer.");
        }

        int footerSize = SsTableV2Format.FooterSizeOf(version);
        ReadOnlySpan<byte> footer = source.GetRegion(length - footerSize, footerSize).Span;
        (long indexOffset, int indexLength) = ReadHandle(footer[0..]);
        (long bloomOffset, int bloomLength) = ReadHandle(footer[12..]);
        (long propertiesOffset, int propertiesLength) = ReadHandle(footer[24..]);
        int alignment = BinaryPrimitives.ReadInt32LittleEndian(footer[(footerSize - TailSize - sizeof(int))..]);
        long dataEnd = length - footerSize;

        if (alignment < SsTableV2Format.MinimumAlignment || (alignment & (alignment - 1)) != 0)
        {
            throw new InvalidDataException($"SSTable block alignment {alignment} is invalid.");
        }

        ValidateHandle(indexOffset, indexLength, dataEnd, "index");
        ValidateHandle(bloomOffset, bloomLength, dataEnd, "Bloom filter");
        ValidateHandle(propertiesOffset, propertiesLength, dataEnd, "properties");
        if (propertiesLength != SsTableV2Format.PropertiesSize)
        {
            throw new InvalidDataException("SSTable properties block has an unexpected size.");
        }

        SsTableV2Properties properties = SsTableV2Properties.ReadFrom(source.GetRegion(propertiesOffset, propertiesLength).Span);

        ReadOnlyMemory<byte> index = source.GetRegion(indexOffset, indexLength);
        ValidateIndex(index.Span, properties, indexOffset, alignment);

        ReadOnlySpan<byte> bloomHeader = source.GetRegion(bloomOffset, SsTableV2Format.BloomHeaderSize).Span;
        int bloomBlocks = BinaryPrimitives.ReadInt32LittleEndian(bloomHeader[0..]);
        int probes = BinaryPrimitives.ReadInt32LittleEndian(bloomHeader[4..]);
        if (bloomBlocks <= 0
            || probes != BlockedBloomFilter.ProbeCount
            || SsTableV2Format.BloomHeaderSize + ((long)bloomBlocks * BlockedBloomFilter.BlockBytes) != bloomLength)
        {
            throw new InvalidDataException("SSTable Bloom filter header is corrupt.");
        }

        ReadOnlyMemory<byte> bloomBits = source.GetRegion(
            bloomOffset + SsTableV2Format.BloomHeaderSize,
            bloomBlocks * BlockedBloomFilter.BlockBytes);

        SsTableColumnarSegment? columnar = null;
        if (version == SsTableV2Format.ColumnarVersion)
        {
            (long columnarOffset, int columnarLength) = ReadHandle(footer[36..]);
            ValidateHandle(columnarOffset, columnarLength, indexOffset, "columnar directory");
            columnar = SsTableColumnarSegment.Open(source, columnarOffset, columnarLength);
        }

        return new SsTableV2Reader(source, ownsSource, index, bloomBits, properties, alignment, version, columnar);
    }

    /// <summary>Block <paramref name="block"/> of the index.</summary>
    public SsTableV2BlockInfo GetBlockInfo(int block) =>
        SsTableV2BlockInfo.ReadFrom(_index.Span.Slice(SsTableV2Format.IndexHeaderSize + (block * SsTableV2Format.IndexEntrySize), SsTableV2Format.IndexEntrySize));

    /// <summary>False when <paramref name="key"/> is certainly not in this file.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MayContain(RowKey key) =>
        key >= Properties.SmallestKey
        && key <= Properties.LargestKey
        && BlockedBloomFilter.MightContain(_bloomBits.Span, SsTableV2Format.WideBloomKey(key));

    /// <summary>
    /// Finds the newest version of <paramref name="key"/> with sequence at most
    /// <paramref name="snapshotSequence"/>. Calls <paramref name="onFound"/> with it (a put or a
    /// deletion) and returns its result; returns <paramref name="missing"/> when the file has none.
    /// </summary>
    public TResult Lookup<TState, TResult>(
        RowKey key,
        ulong snapshotSequence,
        TState state,
        RowVersionFunc<TState, TResult> onFound,
        TResult missing)
    {
        if (!MayContain(key))
        {
            return missing;
        }

        int block = FindFirstBlockEndingAtOrAfter(key);
        while (block < BlockCount)
        {
            SsTableV2BlockInfo info = GetBlockInfo(block);
            if (info.FirstKey > key)
            {
                return missing;
            }

            var request = new LookupRequest<TState, TResult>(key, snapshotSequence, state, onFound);
            BlockLookupResult<TResult> result;
            using (SsTableBlockLease lease = _source.ReadBlock(info.Offset, info.Length, block, this))
            {
                result = SearchBlock(lease.Span, request);
            }

            if (result.Status == BlockLookupStatus.Found)
            {
                return result.Value;
            }

            if (result.Status == BlockLookupStatus.Absent)
            {
                return missing;
            }

            block++; // every version in this block was newer than the snapshot and the row continues
        }

        return missing;
    }

    /// <summary>Opens a sequential cursor over every entry in key order.</summary>
    public SsTableV2Cursor OpenCursor() => new(this);

    /// <summary>
    /// Borrows the verified bytes of block <paramref name="block"/>: mapped memory (checked on first use), or a
    /// pooled or cached buffer whose bytes were checked when read from disk. Dispose the lease when done.
    /// </summary>
    internal SsTableBlockLease AcquireBlock(int block)
    {
        SsTableV2BlockInfo info = GetBlockInfo(block);
        return _source.ReadBlock(info.Offset, info.Length, block, this);
    }

    public void Dispose()
    {
        if (_ownsSource)
        {
            _source.Dispose();
        }
    }

    private static BlockLookupResult<TResult> SearchBlock<TState, TResult>(ReadOnlySpan<byte> bytes, LookupRequest<TState, TResult> request)
    {
        if (SsTableWideRowBlock.IsWide(bytes))
        {
            return SearchWideBlock(bytes, request);
        }

        // An integer block. Block selection only picks it for a key at most its last key, which is an
        // integer key, so the target is an integer too.
        if (!request.Key.IsInt64)
        {
            return BlockLookupResult<TResult>.Absent;
        }

        return SearchCompactBlock(bytes, request.Key.AsInt64(), request);
    }

    private static BlockLookupResult<TResult> SearchCompactBlock<TState, TResult>(ReadOnlySpan<byte> bytes, long rowId, LookupRequest<TState, TResult> request)
    {
        // Last restart whose row id is below the target: every version of the target starts after it.
        int low = 0;
        int high = SsTableCompactRowBlock.RestartCount(bytes) - 1;
        while (low < high)
        {
            int mid = (int)((uint)(low + high + 1) >> 1);
            if (SsTableCompactRowBlock.RestartRowId(bytes, mid) < rowId)
            {
                low = mid;
            }
            else
            {
                high = mid - 1;
            }
        }

        var entries = default(CompactRowBlockReader);
        entries.Seek(bytes, low);
        while (entries.MoveNext(bytes))
        {
            if (entries.RowId < rowId)
            {
                continue;
            }

            if (entries.RowId != rowId)
            {
                return BlockLookupResult<TResult>.Absent;
            }

            ulong sequence = SsTableV2Format.SequenceOf(entries.Tag);
            if (sequence > request.SnapshotSequence)
            {
                continue; // newer than the reader's snapshot
            }

            return BlockLookupResult<TResult>.Found(request.OnFound(
                SsTableV2Format.TypeOf(entries.Tag),
                sequence,
                bytes.Slice(entries.ValueOffset, entries.ValueLength),
                request.State));
        }

        return BlockLookupResult<TResult>.ContinueNextBlock;
    }

    // A dedicated loop rather than WideRowBlockReader: locals for the key halves and the restart array,
    // and one-byte varints decoded inline. The block was verified once (VerifyWideBlock), so offsets are
    // trusted here the same way the cursor trusts them.
    private static BlockLookupResult<TResult> SearchWideBlock<TState, TResult>(ReadOnlySpan<byte> bytes, LookupRequest<TState, TResult> request)
    {
        ulong targetHigh = request.Key.High;
        ulong targetLow = request.Key.Low;
        int count = BinaryPrimitives.ReadInt32LittleEndian(bytes[4..]);
        int restarts = BinaryPrimitives.ReadInt32LittleEndian(bytes[^sizeof(int)..]);
        int restartsStart = bytes.Length - sizeof(int) - (restarts * sizeof(int));

        // Last restart whose key is below the target: every version of the target starts after it.
        int low = 0;
        int high = restarts - 1;
        while (low < high)
        {
            int mid = (int)((uint)(low + high + 1) >> 1);
            int offset = BinaryPrimitives.ReadInt32LittleEndian(bytes[(restartsStart + (mid * sizeof(int)))..]);
            ulong restartHigh = ReadSmallVarint(bytes, ref offset, restartsStart);
            ulong restartLow = BinaryPrimitives.ReadUInt64LittleEndian(bytes[offset..]);
            if (restartHigh < targetHigh || (restartHigh == targetHigh && restartLow < targetLow))
            {
                low = mid;
            }
            else
            {
                high = mid - 1;
            }
        }

        int position = BinaryPrimitives.ReadInt32LittleEndian(bytes[(restartsStart + (low * sizeof(int)))..]);
        ulong keyHigh = 0;
        ulong keyLow = 0;
        ulong tag = 0;
        for (int index = low * SsTableWideRowBlock.RestartInterval; index < count; index++)
        {
            if (index % SsTableWideRowBlock.RestartInterval == 0)
            {
                keyHigh = ReadSmallVarint(bytes, ref position, restartsStart);
                keyLow = BinaryPrimitives.ReadUInt64LittleEndian(bytes[position..]);
                position += sizeof(ulong);
                tag = ReadSmallVarint(bytes, ref position, restartsStart);
            }
            else
            {
                ulong step = ReadSmallVarint(bytes, ref position, restartsStart);
                switch (step & 3)
                {
                    case 0:
                        keyLow = unchecked(keyLow + (step >> 2));
                        break;
                    case 1:
                        keyHigh = unchecked(keyHigh + (step >> 2));
                        keyLow = BinaryPrimitives.ReadUInt64LittleEndian(bytes[position..]);
                        position += sizeof(ulong);
                        break;
                    default:
                        RowKey full = SsTableV2Format.ReadKey(bytes[position..]);
                        (keyHigh, keyLow) = (full.High, full.Low);
                        position += SsTableWideRowBlock.KeySize;
                        break;
                }

                tag = (ulong)((long)tag + SsTableCompactRowBlock.UnZigZag(ReadSmallVarint(bytes, ref position, restartsStart)));
            }

            int valueLength = (int)ReadSmallVarint(bytes, ref position, restartsStart);
            int valueOffset = position;
            position += valueLength;

            if (keyHigh < targetHigh || (keyHigh == targetHigh && keyLow < targetLow))
            {
                continue;
            }

            if (keyHigh != targetHigh || keyLow != targetLow)
            {
                return BlockLookupResult<TResult>.Absent;
            }

            ulong sequence = SsTableV2Format.SequenceOf(tag);
            if (sequence > request.SnapshotSequence)
            {
                continue; // newer than the reader's snapshot
            }

            return BlockLookupResult<TResult>.Found(request.OnFound(
                SsTableV2Format.TypeOf(tag),
                sequence,
                bytes.Slice(valueOffset, valueLength),
                request.State));
        }

        return BlockLookupResult<TResult>.ContinueNextBlock;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ReadSmallVarint(ReadOnlySpan<byte> bytes, ref int position, int limit)
    {
        byte first = bytes[position];
        if (first < 0x80)
        {
            position++;
            return first;
        }

        return SsTableCompactRowBlock.ReadVarint(bytes, ref position, limit);
    }

    /// <summary>First block whose last key is at or after <paramref name="key"/>.</summary>
    internal int FindFirstBlockEndingAtOrAfter(RowKey key)
    {
        int low = 0;
        int high = BlockCount;
        while (low < high)
        {
            int mid = (int)((uint)(low + high) >> 1);
            if (_blockLastKeys[mid] < key)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        return low;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Mapped bytes are checked once per block (the bit set remembers it); bytes read from disk are checked on
    /// every read, so a block that rots on disk after leaving the block cache is still caught.
    /// </remarks>
    void ISsTableBlockVerifier.VerifyBlock(int block, ReadOnlySpan<byte> bytes, bool physicalRead)
    {
        int word = block >> 5;
        int bit = 1 << (block & 31);
        if (!physicalRead && (Volatile.Read(ref _verifiedBlocks[word]) & bit) != 0)
        {
            return;
        }

        if (bytes.Length < SsTableV2Format.BlockHeaderSize)
        {
            throw new InvalidDataException($"SSTable block {block} is truncated.");
        }

        int count = BinaryPrimitives.ReadInt32LittleEndian(bytes[4..]);
        int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(bytes[12..]);
        bool wide = SsTableWideRowBlock.IsWide(bytes);
        if (count <= 0
            || (!wide && !SsTableCompactRowBlock.IsCompact(bytes))
            || payloadLength < sizeof(int)
            || SsTableV2Format.BlockHeaderSize + payloadLength != bytes.Length)
        {
            throw new InvalidDataException($"SSTable block {block} header is corrupt.");
        }

        uint expected = BinaryPrimitives.ReadUInt32LittleEndian(bytes);
        if (WalCrc32C.HashToUInt32(bytes[4..]) != expected)
        {
            throw new InvalidDataException($"SSTable block {block} failed its CRC32C check.");
        }

        if (wide)
        {
            VerifyWideBlock(block, bytes, count);
        }
        else
        {
            VerifyCompactBlock(block, bytes, count);
        }

        int current;
        do
        {
            current = Volatile.Read(ref _verifiedBlocks[word]);
        }
        while (Interlocked.CompareExchange(ref _verifiedBlocks[word], current | bit, current) != current);
    }

    // Decodes every entry once, so later reads of this block can trust every offset and length.
    private static void VerifyCompactBlock(int block, ReadOnlySpan<byte> bytes, int count)
    {
        VerifyRestartArray(block, bytes, count);

        var entries = default(CompactRowBlockReader);
        try
        {
            entries.Seek(bytes, 0);
            int restart = 0;
            long previous = long.MinValue;
            while (entries.MoveNext(bytes))
            {
                if (entries.Index % SsTableCompactRowBlock.RestartInterval == 0
                    && SsTableCompactRowBlock.RestartOffset(bytes, restart++) != entries.EntryOffset)
                {
                    throw new InvalidDataException($"SSTable block {block} restart {restart - 1} is misplaced.");
                }

                if (entries.RowId < previous)
                {
                    throw new InvalidDataException($"SSTable block {block} entries are out of order.");
                }

                previous = entries.RowId;
            }

            if (!entries.EndsAtRestarts)
            {
                throw new InvalidDataException($"SSTable block {block} entries do not fill the block.");
            }
        }
        catch (Exception ex) when (ex is ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
        {
            throw new InvalidDataException($"SSTable block {block} is corrupt.", ex);
        }
    }

    private static void VerifyWideBlock(int block, ReadOnlySpan<byte> bytes, int count)
    {
        VerifyRestartArray(block, bytes, count);
        var entries = default(WideRowBlockReader);
        try
        {
            entries.Seek(bytes, 0);
            int restart = 0;
            bool hasPrevious = false;
            RowKey previous = default;
            while (entries.MoveNext(bytes))
            {
                if (entries.Index % SsTableWideRowBlock.RestartInterval == 0
                    && SsTableCompactRowBlock.RestartOffset(bytes, restart++) != entries.EntryOffset)
                {
                    throw new InvalidDataException($"SSTable block {block} restart {restart - 1} is misplaced.");
                }

                if (hasPrevious && entries.Key < previous)
                {
                    throw new InvalidDataException($"SSTable block {block} entries are out of order.");
                }

                previous = entries.Key;
                hasPrevious = true;
            }

            if (!entries.EndsAtRestarts)
            {
                throw new InvalidDataException($"SSTable block {block} entries do not fill the block.");
            }
        }
        catch (Exception ex) when (ex is ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
        {
            throw new InvalidDataException($"SSTable block {block} is corrupt.", ex);
        }
    }

    private static void VerifyRestartArray(int block, ReadOnlySpan<byte> bytes, int count)
    {
        int restarts = SsTableCompactRowBlock.RestartCount(bytes);
        long restartsStart = (long)bytes.Length - sizeof(int) - ((long)restarts * sizeof(int));
        if (restarts != (count + SsTableCompactRowBlock.RestartInterval - 1) / SsTableCompactRowBlock.RestartInterval
            || restartsStart < SsTableV2Format.BlockHeaderSize)
        {
            throw new InvalidDataException($"SSTable block {block} restart array is corrupt.");
        }
    }

    private static (long Offset, int Length) ReadHandle(ReadOnlySpan<byte> source) =>
        (BinaryPrimitives.ReadInt64LittleEndian(source), BinaryPrimitives.ReadInt32LittleEndian(source[8..]));

    private static void ValidateHandle(long offset, int length, long dataEnd, string name)
    {
        if (offset < 0 || length <= 0 || offset > dataEnd - length)
        {
            throw new InvalidDataException($"SSTable {name} handle points outside the file.");
        }
    }

    private static void ValidateIndex(ReadOnlySpan<byte> index, SsTableV2Properties properties, long indexOffset, int alignment)
    {
        const int entrySize = SsTableV2Format.IndexEntrySize;
        int count = BinaryPrimitives.ReadInt32LittleEndian(index);
        if (count <= 0
            || count != properties.BlockCount
            || index.Length != SsTableV2Format.IndexHeaderSize + ((long)count * entrySize))
        {
            throw new InvalidDataException("SSTable block index does not match its properties.");
        }

        RowKey previousLast = default;
        long previousEnd = 0;
        for (int i = 0; i < count; i++)
        {
            var info = SsTableV2BlockInfo.ReadFrom(index.Slice(SsTableV2Format.IndexHeaderSize + (i * entrySize)));
            if (info.Offset % alignment != 0
                || info.Offset < previousEnd
                || info.Length <= SsTableV2Format.BlockHeaderSize
                || info.Offset + info.Length > indexOffset
                || info.EntryCount <= 0
                || info.FirstKey > info.LastKey
                || (i > 0 && info.FirstKey < previousLast))
            {
                throw new InvalidDataException($"SSTable block index entry {i} is corrupt.");
            }

            previousLast = info.LastKey;
            previousEnd = info.Offset + info.Length;
        }
    }

    // A struct passed by value through the block callback: a lookup allocates nothing.
    private readonly struct LookupRequest<TState, TResult>(
        RowKey key,
        ulong snapshotSequence,
        TState state,
        RowVersionFunc<TState, TResult> onFound)
    {
        public RowKey Key { get; } = key;
        public ulong SnapshotSequence { get; } = snapshotSequence;
        public TState State { get; } = state;
        public RowVersionFunc<TState, TResult> OnFound { get; } = onFound;
    }

    private enum BlockLookupStatus : byte
    {
        Absent,
        Found,
        ContinueNextBlock,
    }

    private readonly record struct BlockLookupResult<TResult>(BlockLookupStatus Status, TResult Value)
    {
        public static BlockLookupResult<TResult> Absent => new(BlockLookupStatus.Absent, default!);

        public static BlockLookupResult<TResult> ContinueNextBlock => new(BlockLookupStatus.ContinueNextBlock, default!);

        public static BlockLookupResult<TResult> Found(TResult value) => new(BlockLookupStatus.Found, value);
    }
}

/// <summary>Sequential cursor over all entries of a row-keyed SSTable, block by block.</summary>
internal sealed unsafe class SsTableV2Cursor
{
    private SsTableV2Reader _reader;

    // The current block: mapped memory, or a pooled/cached buffer held until the cursor moves past it.
    private SsTableBlockLease _block;
    private int _blockIndex = -1;
    private int _entry;
    private int _entryCount;
    private bool _wide;
    private CompactRowBlockReader _compactEntries;
    private WideRowBlockReader _wideEntries;

    internal SsTableV2Cursor(SsTableV2Reader reader)
    {
        _reader = reader;
    }

    /// <summary>Points this cursor at the start of a file, so one cursor object serves many scans.</summary>
    internal SsTableV2Cursor Reset(SsTableV2Reader reader)
    {
        _reader = reader;
        DropBlock();
        _blockIndex = -1;
        _entry = 0;
        _entryCount = 0;
        _wide = false;
        return this;
    }

    /// <summary>Drops the file this cursor read, so a pooled cursor keeps no reader or block alive.</summary>
    internal void Release()
    {
        _reader = null!;
        DropBlock();
    }

    private void DropBlock()
    {
        SsTableBlockLease block = _block;
        _block = default;
        block.Dispose();
    }

    public RowKey Key { get; private set; }

    public ulong Sequence { get; private set; }

    public LsmValueType Type { get; private set; }

    private int _valueOffset;
    private int _valueLength;

    /// <summary>The current entry's value; valid until the next <see cref="MoveNext"/>.</summary>
    public ReadOnlySpan<byte> Value => BlockSpan.Slice(_valueOffset, _valueLength);

    private ReadOnlySpan<byte> BlockSpan => _block.Span;

    /// <summary>
    /// Moves to the first entry whose key is at or after <paramref name="key"/>; returns false when there
    /// is none. Earlier blocks are skipped through the index, so only one block is read to get there.
    /// </summary>
    public bool SeekTo(RowKey key)
    {
        _blockIndex = _reader.FindFirstBlockEndingAtOrAfter(key) - 1;
        _entry = 0;
        _entryCount = 0;
        while (MoveNext())
        {
            if (Key >= key)
            {
                return true;
            }
        }

        return false;
    }

    public bool MoveNext()
    {
        while (_entry >= _entryCount)
        {
            _blockIndex++;
            DropBlock();
            if (_blockIndex >= _reader.BlockCount)
            {
                return false;
            }

            _block = _reader.AcquireBlock(_blockIndex);
            _entryCount = BinaryPrimitives.ReadInt32LittleEndian(BlockSpan[4..]);
            _entry = 0;
            _wide = SsTableWideRowBlock.IsWide(BlockSpan);
            if (_wide)
            {
                _wideEntries.Seek(BlockSpan, 0);
            }
            else
            {
                _compactEntries.Seek(BlockSpan, 0);
            }
        }

        if (_wide)
        {
            _wideEntries.MoveNext(BlockSpan);
            Key = _wideEntries.Key;
            Sequence = SsTableV2Format.SequenceOf(_wideEntries.Tag);
            Type = SsTableV2Format.TypeOf(_wideEntries.Tag);
            _valueOffset = _wideEntries.ValueOffset;
            _valueLength = _wideEntries.ValueLength;
            _entry++;
            return true;
        }

        _compactEntries.MoveNext(BlockSpan);
        Key = _compactEntries.RowId;
        Sequence = SsTableV2Format.SequenceOf(_compactEntries.Tag);
        Type = SsTableV2Format.TypeOf(_compactEntries.Tag);
        _valueOffset = _compactEntries.ValueOffset;
        _valueLength = _compactEntries.ValueLength;
        _entry++;
        return true;
    }
}
