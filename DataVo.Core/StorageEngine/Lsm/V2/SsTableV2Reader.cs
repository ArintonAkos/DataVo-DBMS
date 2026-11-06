using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using DataVo.Core.Transactions;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>Receives the newest visible version of a row; the span is valid only during the call.</summary>
internal delegate TResult RowVersionFunc<TState, TResult>(LsmValueType type, ulong sequence, ReadOnlySpan<byte> value, TState state);

/// <summary>
/// Reads a row-keyed SSTable (v2, v4 or v6) through a <see cref="SsTableBlockSource"/>. Opening validates the footer, block
/// index, Bloom filter, and properties (O(metadata)); no data block is read until a lookup needs it.
/// </summary>
/// <remarks>
/// A point lookup costs: a key range check, one Bloom probe (one cache line), a binary search over the
/// block index (L1/L2-resident), and a binary search over the block's restart points (or its dense
/// 24-byte entries in v2). Each block's CRC32C is verified the first time any reader touches it.
/// Files of versions 2 and 4 hold integer keys; their keys are exposed as <see cref="RowKey"/>s.
/// </remarks>
internal sealed class SsTableV2Reader : IDisposable
{
    private readonly SsTableBlockSource _source;
    private readonly ReadOnlyMemory<byte> _index;
    private readonly ReadOnlyMemory<byte> _bloomBits;
    private readonly int[] _verifiedBlocks;
    private readonly bool _ownsSource;
    private readonly int _indexEntrySize;
    private readonly bool _wide;

    // v6: the last key of every block, decoded once at open (16 B per block; a 1M-row file has ~2K
    // blocks), so the block search compares keys instead of decoding index records on every step.
    private readonly RowKey[] _blockLastKeys = [];

    private SsTableV2Reader(
        SsTableBlockSource source,
        bool ownsSource,
        ReadOnlyMemory<byte> index,
        ReadOnlyMemory<byte> bloomBits,
        SsTableV2Properties properties,
        int alignment,
        ushort version)
    {
        _source = source;
        Version = version;
        _wide = version == SsTableV2Format.WideVersion;
        _indexEntrySize = SsTableV2Format.IndexEntrySizeOf(version);
        if (_wide)
        {
            _blockLastKeys = new RowKey[properties.BlockCount];
            ReadOnlySpan<byte> records = index.Span;
            for (int block = 0; block < _blockLastKeys.Length; block++)
            {
                _blockLastKeys[block] = SsTableV2Format.ReadKey(records[(SsTableV2Format.IndexHeaderSize + (block * _indexEntrySize) + SsTableWideRowBlock.KeySize)..]);
            }
        }
        _ownsSource = ownsSource;
        _index = index;
        _bloomBits = bloomBits;
        Properties = properties;
        Alignment = alignment;
        _verifiedBlocks = new int[(properties.BlockCount + 31) / 32];
    }

    public SsTableV2Properties Properties { get; }

    /// <summary>The file's format version (2, 4 or 6).</summary>
    public ushort Version { get; }

    public int Alignment { get; }

    public int BlockCount => Properties.BlockCount;

    public bool IsMapped => _source.IsMapped;

    public long FileLength => _source.Length;

    /// <summary>Opens <paramref name="path"/> with the given read mode.</summary>
    public static SsTableV2Reader Open(string path, LsmReadMode mode = LsmReadMode.Auto)
    {
        SsTableBlockSource source = SsTableBlockSource.Open(path, mode);
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

        ReadOnlySpan<byte> footer = source.GetRegion(length - SsTableV2Format.FooterSize, SsTableV2Format.FooterSize).Span;
        if (!SsTableV2Format.TryReadVersion(footer, out ushort version) || !SsTableV2Format.IsRowKeyedVersion(version))
        {
            throw new InvalidDataException("SSTable footer is not a row-keyed (v2, v4 or v6) footer.");
        }

        (long indexOffset, int indexLength) = ReadHandle(footer[0..]);
        (long bloomOffset, int bloomLength) = ReadHandle(footer[12..]);
        (long propertiesOffset, int propertiesLength) = ReadHandle(footer[24..]);
        int alignment = BinaryPrimitives.ReadInt32LittleEndian(footer[36..]);
        long dataEnd = length - SsTableV2Format.FooterSize;

        if (alignment < SsTableV2Format.MinimumAlignment || (alignment & (alignment - 1)) != 0)
        {
            throw new InvalidDataException($"SSTable block alignment {alignment} is invalid.");
        }

        ValidateHandle(indexOffset, indexLength, dataEnd, "index");
        ValidateHandle(bloomOffset, bloomLength, dataEnd, "Bloom filter");
        ValidateHandle(propertiesOffset, propertiesLength, dataEnd, "properties");
        if (propertiesLength != SsTableV2Format.PropertiesSizeOf(version))
        {
            throw new InvalidDataException("SSTable properties block has an unexpected size.");
        }

        SsTableV2Properties properties = SsTableV2Properties.ReadFrom(source.GetRegion(propertiesOffset, propertiesLength).Span, version);

        ReadOnlyMemory<byte> index = source.GetRegion(indexOffset, indexLength);
        ValidateIndex(index.Span, properties, indexOffset, alignment, version);

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

        return new SsTableV2Reader(source, ownsSource, index, bloomBits, properties, alignment, version);
    }

    /// <summary>Block <paramref name="block"/> of the index.</summary>
    public SsTableV2BlockInfo GetBlockInfo(int block) =>
        SsTableV2BlockInfo.ReadFrom(_index.Span.Slice(SsTableV2Format.IndexHeaderSize + (block * _indexEntrySize), _indexEntrySize), Version);

    /// <summary>False when <paramref name="key"/> is certainly not in this file.</summary>
    /// <remarks>
    /// In a v2 or v4 file both bounds are integer keys (high half zero), so a key inside the range is an
    /// integer key too and probes the filter by its integer.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MayContain(RowKey key) =>
        key >= Properties.SmallestKey
        && key <= Properties.LargestKey
        && BlockedBloomFilter.MightContain(_bloomBits.Span, _wide ? SsTableV2Format.WideBloomKey(key) : unchecked((long)(key.Low ^ 0x8000_0000_0000_0000UL)));

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

            var request = new LookupRequest<TState, TResult>(this, key, snapshotSequence, state, onFound, block);
            BlockLookupResult<TResult> result = _source.Read(info.Offset, info.Length, request, static (bytes, req) => req.Reader.SearchBlock(bytes, req));
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

    /// <summary>The bytes of block <paramref name="block"/>, CRC-verified on first use.</summary>
    internal ReadOnlyMemory<byte> GetVerifiedBlock(int block)
    {
        SsTableV2BlockInfo info = GetBlockInfo(block);
        ReadOnlyMemory<byte> bytes = _source.GetRegion(info.Offset, info.Length);
        VerifyOnce(block, bytes.Span);
        return bytes;
    }

    public void Dispose()
    {
        if (_ownsSource)
        {
            _source.Dispose();
        }
    }

    private BlockLookupResult<TResult> SearchBlock<TState, TResult>(ReadOnlySpan<byte> bytes, LookupRequest<TState, TResult> request)
    {
        VerifyOnce(request.Block, bytes);
        if (SsTableWideRowBlock.IsWide(bytes))
        {
            return SearchWideBlock(bytes, request);
        }

        // An integer block (any v2/v4 block, or a v6 block of integer keys). Block selection only picks it
        // for a key at most its last key, which is an integer key, so the target is an integer too.
        if (!request.Key.IsInt64)
        {
            return BlockLookupResult<TResult>.Absent;
        }

        long rowId = request.Key.AsInt64();
        if (SsTableCompactRowBlock.IsCompact(bytes))
        {
            return SearchCompactBlock(bytes, rowId, request);
        }

        int count = BinaryPrimitives.ReadInt32LittleEndian(bytes[4..]);

        // Lower bound: first entry whose row id is >= the target.
        int low = 0;
        int high = count;
        while (low < high)
        {
            int mid = (int)((uint)(low + high) >> 1);
            if (EntryRowId(bytes, mid) < rowId)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        for (int i = low; i < count; i++)
        {
            ReadOnlySpan<byte> entry = bytes.Slice(SsTableV2Format.BlockHeaderSize + (i * SsTableV2Format.EntrySize), SsTableV2Format.EntrySize);
            if (BinaryPrimitives.ReadInt64LittleEndian(entry) != rowId)
            {
                return BlockLookupResult<TResult>.Absent;
            }

            ulong tag = BinaryPrimitives.ReadUInt64LittleEndian(entry[8..]);
            ulong sequence = SsTableV2Format.SequenceOf(tag);
            if (sequence > request.SnapshotSequence)
            {
                continue; // newer than the reader's snapshot
            }

            int valueOffset = BinaryPrimitives.ReadInt32LittleEndian(entry[16..]);
            int valueLength = BinaryPrimitives.ReadInt32LittleEndian(entry[20..]);
            LsmValueType type = SsTableV2Format.TypeOf(tag);
            return BlockLookupResult<TResult>.Found(
                request.OnFound(type, sequence, bytes.Slice(valueOffset, valueLength), request.State));
        }

        // Ran off the end of the block while still on this row id (every version here was newer than
        // the snapshot) or before reaching it: older versions can only be in the next block.
        return BlockLookupResult<TResult>.ContinueNextBlock;
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long EntryRowId(ReadOnlySpan<byte> block, int entry) =>
        BinaryPrimitives.ReadInt64LittleEndian(block[(SsTableV2Format.BlockHeaderSize + (entry * SsTableV2Format.EntrySize))..]);

    /// <summary>First block whose last key is at or after <paramref name="key"/>.</summary>
    private int FindFirstBlockEndingAtOrAfter(RowKey key)
    {
        ReadOnlySpan<byte> index = _index.Span;
        int low = 0;
        int high = BlockCount;
        while (low < high)
        {
            int mid = (int)((uint)(low + high) >> 1);
            int record = SsTableV2Format.IndexHeaderSize + (mid * _indexEntrySize);
            bool before = _wide
                ? _blockLastKeys[mid] < key
                : (RowKey)BinaryPrimitives.ReadInt64LittleEndian(index[(record + 8)..]) < key;
            if (before)
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

    private void VerifyOnce(int block, ReadOnlySpan<byte> bytes)
    {
        int word = block >> 5;
        int bit = 1 << (block & 31);
        if ((Volatile.Read(ref _verifiedBlocks[word]) & bit) != 0)
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
        bool compact = wide || SsTableCompactRowBlock.IsCompact(bytes);
        if (wide && !_wide)
        {
            throw new InvalidDataException($"SSTable block {block} uses the wide encoding, which version {Version} files do not have.");
        }

        if (count <= 0
            || payloadLength < (compact ? sizeof(int) : (long)count * SsTableV2Format.EntrySize)
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
        else if (compact)
        {
            VerifyCompactBlock(block, bytes, count);
        }

        for (int i = 0; !compact && i < count; i++)
        {
            ReadOnlySpan<byte> entry = bytes.Slice(SsTableV2Format.BlockHeaderSize + (i * SsTableV2Format.EntrySize), SsTableV2Format.EntrySize);
            int valueOffset = BinaryPrimitives.ReadInt32LittleEndian(entry[16..]);
            int valueLength = BinaryPrimitives.ReadInt32LittleEndian(entry[20..]);
            if (valueOffset < SsTableV2Format.BlockHeaderSize || valueLength < 0 || valueOffset > bytes.Length - valueLength)
            {
                throw new InvalidDataException($"SSTable block {block} entry {i} points outside the block.");
            }
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

    private static void ValidateIndex(ReadOnlySpan<byte> index, SsTableV2Properties properties, long indexOffset, int alignment, ushort version)
    {
        int entrySize = SsTableV2Format.IndexEntrySizeOf(version);
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
            var info = SsTableV2BlockInfo.ReadFrom(index.Slice(SsTableV2Format.IndexHeaderSize + (i * entrySize)), version);
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
        SsTableV2Reader reader,
        RowKey key,
        ulong snapshotSequence,
        TState state,
        RowVersionFunc<TState, TResult> onFound,
        int block)
    {
        public SsTableV2Reader Reader { get; } = reader;
        public RowKey Key { get; } = key;
        public ulong SnapshotSequence { get; } = snapshotSequence;
        public TState State { get; } = state;
        public RowVersionFunc<TState, TResult> OnFound { get; } = onFound;
        public int Block { get; } = block;
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
internal sealed class SsTableV2Cursor
{
    private readonly SsTableV2Reader _reader;
    private ReadOnlyMemory<byte> _block;
    private int _blockIndex = -1;
    private int _entry;
    private int _entryCount;
    private bool _compact;
    private bool _wide;
    private CompactRowBlockReader _compactEntries;
    private WideRowBlockReader _wideEntries;

    internal SsTableV2Cursor(SsTableV2Reader reader)
    {
        _reader = reader;
    }

    public RowKey Key { get; private set; }

    public ulong Sequence { get; private set; }

    public LsmValueType Type { get; private set; }

    private int _valueOffset;
    private int _valueLength;

    /// <summary>The current entry's value; valid until the next <see cref="MoveNext"/>.</summary>
    public ReadOnlySpan<byte> Value => _block.Span.Slice(_valueOffset, _valueLength);

    public bool MoveNext()
    {
        while (_entry >= _entryCount)
        {
            _blockIndex++;
            if (_blockIndex >= _reader.BlockCount)
            {
                return false;
            }

            _block = _reader.GetVerifiedBlock(_blockIndex);
            _entryCount = BinaryPrimitives.ReadInt32LittleEndian(_block.Span[4..]);
            _entry = 0;
            _wide = SsTableWideRowBlock.IsWide(_block.Span);
            _compact = !_wide && SsTableCompactRowBlock.IsCompact(_block.Span);
            if (_wide)
            {
                _wideEntries.Seek(_block.Span, 0);
            }
            else if (_compact)
            {
                _compactEntries.Seek(_block.Span, 0);
            }
        }

        if (_wide)
        {
            _wideEntries.MoveNext(_block.Span);
            Key = _wideEntries.Key;
            Sequence = SsTableV2Format.SequenceOf(_wideEntries.Tag);
            Type = SsTableV2Format.TypeOf(_wideEntries.Tag);
            _valueOffset = _wideEntries.ValueOffset;
            _valueLength = _wideEntries.ValueLength;
            _entry++;
            return true;
        }

        if (_compact)
        {
            _compactEntries.MoveNext(_block.Span);
            Key = _compactEntries.RowId;
            Sequence = SsTableV2Format.SequenceOf(_compactEntries.Tag);
            Type = SsTableV2Format.TypeOf(_compactEntries.Tag);
            _valueOffset = _compactEntries.ValueOffset;
            _valueLength = _compactEntries.ValueLength;
            _entry++;
            return true;
        }

        ReadOnlySpan<byte> entry = _block.Span.Slice(SsTableV2Format.BlockHeaderSize + (_entry * SsTableV2Format.EntrySize), SsTableV2Format.EntrySize);
        Key = BinaryPrimitives.ReadInt64LittleEndian(entry);
        ulong tag = BinaryPrimitives.ReadUInt64LittleEndian(entry[8..]);
        Sequence = SsTableV2Format.SequenceOf(tag);
        Type = SsTableV2Format.TypeOf(tag);
        _valueOffset = BinaryPrimitives.ReadInt32LittleEndian(entry[16..]);
        _valueLength = BinaryPrimitives.ReadInt32LittleEndian(entry[20..]);
        _entry++;
        return true;
    }
}
