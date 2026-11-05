using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using DataVo.Core.Transactions;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>Receives the newest visible version of a row; the span is valid only during the call.</summary>
internal delegate TResult RowVersionFunc<TState, TResult>(LsmValueType type, ulong sequence, ReadOnlySpan<byte> value, TState state);

/// <summary>
/// Reads a v2 SSTable through a <see cref="SsTableBlockSource"/>. Opening validates the footer, block
/// index, Bloom filter, and properties (O(metadata)); no data block is read until a lookup needs it.
/// </summary>
/// <remarks>
/// A point lookup costs: a row-id range check, one Bloom probe (one cache line), a binary search over the
/// block index (32 B records, L1/L2-resident), and a binary search over the block's dense 24-byte entry
/// array. Each block's CRC32C is verified the first time any reader touches it.
/// </remarks>
internal sealed class SsTableV2Reader : IDisposable
{
    private readonly SsTableBlockSource _source;
    private readonly ReadOnlyMemory<byte> _index;
    private readonly ReadOnlyMemory<byte> _bloomBits;
    private readonly int[] _verifiedBlocks;
    private readonly bool _ownsSource;

    private SsTableV2Reader(
        SsTableBlockSource source,
        bool ownsSource,
        ReadOnlyMemory<byte> index,
        ReadOnlyMemory<byte> bloomBits,
        SsTableV2Properties properties,
        int alignment)
    {
        _source = source;
        _ownsSource = ownsSource;
        _index = index;
        _bloomBits = bloomBits;
        Properties = properties;
        Alignment = alignment;
        _verifiedBlocks = new int[(properties.BlockCount + 31) / 32];
    }

    public SsTableV2Properties Properties { get; }

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
        if (!SsTableV2Format.TryReadVersion(footer, out ushort version) || version != SsTableV2Format.Version)
        {
            throw new InvalidDataException("SSTable footer is not a v2 footer.");
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

        return new SsTableV2Reader(source, ownsSource, index, bloomBits, properties, alignment);
    }

    /// <summary>Block <paramref name="block"/> of the index.</summary>
    public SsTableV2BlockInfo GetBlockInfo(int block) =>
        SsTableV2BlockInfo.ReadFrom(_index.Span.Slice(SsTableV2Format.IndexHeaderSize + (block * SsTableV2Format.IndexEntrySize), SsTableV2Format.IndexEntrySize));

    /// <summary>False when <paramref name="rowId"/> is certainly not in this file.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MayContain(long rowId) =>
        rowId >= Properties.SmallestRowId
        && rowId <= Properties.LargestRowId
        && BlockedBloomFilter.MightContain(_bloomBits.Span, rowId);

    /// <summary>
    /// Finds the newest version of <paramref name="rowId"/> with sequence at most
    /// <paramref name="snapshotSequence"/>. Calls <paramref name="onFound"/> with it (a put or a
    /// deletion) and returns its result; returns <paramref name="missing"/> when the file has none.
    /// </summary>
    public TResult Lookup<TState, TResult>(
        long rowId,
        ulong snapshotSequence,
        TState state,
        RowVersionFunc<TState, TResult> onFound,
        TResult missing)
    {
        if (!MayContain(rowId))
        {
            return missing;
        }

        int block = FindFirstBlockEndingAtOrAfter(rowId);
        while (block < BlockCount)
        {
            SsTableV2BlockInfo info = GetBlockInfo(block);
            if (info.FirstRowId > rowId)
            {
                return missing;
            }

            var request = new LookupRequest<TState, TResult>(this, rowId, snapshotSequence, state, onFound, block);
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
        int count = BinaryPrimitives.ReadInt32LittleEndian(bytes[4..]);

        // Lower bound: first entry whose row id is >= the target.
        int low = 0;
        int high = count;
        while (low < high)
        {
            int mid = (int)((uint)(low + high) >> 1);
            if (EntryRowId(bytes, mid) < request.RowId)
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
            if (BinaryPrimitives.ReadInt64LittleEndian(entry) != request.RowId)
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long EntryRowId(ReadOnlySpan<byte> block, int entry) =>
        BinaryPrimitives.ReadInt64LittleEndian(block[(SsTableV2Format.BlockHeaderSize + (entry * SsTableV2Format.EntrySize))..]);

    /// <summary>First block whose last row id is at or after <paramref name="rowId"/>.</summary>
    private int FindFirstBlockEndingAtOrAfter(long rowId)
    {
        ReadOnlySpan<byte> index = _index.Span;
        int low = 0;
        int high = BlockCount;
        while (low < high)
        {
            int mid = (int)((uint)(low + high) >> 1);
            long last = BinaryPrimitives.ReadInt64LittleEndian(index[(SsTableV2Format.IndexHeaderSize + (mid * SsTableV2Format.IndexEntrySize) + 8)..]);
            if (last < rowId)
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
        if (count <= 0
            || payloadLength < (long)count * SsTableV2Format.EntrySize
            || SsTableV2Format.BlockHeaderSize + payloadLength != bytes.Length)
        {
            throw new InvalidDataException($"SSTable block {block} header is corrupt.");
        }

        uint expected = BinaryPrimitives.ReadUInt32LittleEndian(bytes);
        if (WalCrc32C.HashToUInt32(bytes[4..]) != expected)
        {
            throw new InvalidDataException($"SSTable block {block} failed its CRC32C check.");
        }

        for (int i = 0; i < count; i++)
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
        int count = BinaryPrimitives.ReadInt32LittleEndian(index);
        if (count <= 0
            || count != properties.BlockCount
            || index.Length != SsTableV2Format.IndexHeaderSize + ((long)count * SsTableV2Format.IndexEntrySize))
        {
            throw new InvalidDataException("SSTable block index does not match its properties.");
        }

        long previousLast = long.MinValue;
        long previousEnd = 0;
        for (int i = 0; i < count; i++)
        {
            var info = SsTableV2BlockInfo.ReadFrom(index.Slice(SsTableV2Format.IndexHeaderSize + (i * SsTableV2Format.IndexEntrySize)));
            if (info.Offset % alignment != 0
                || info.Offset < previousEnd
                || info.Length < SsTableV2Format.BlockHeaderSize + SsTableV2Format.EntrySize
                || info.Offset + info.Length > indexOffset
                || info.EntryCount <= 0
                || info.FirstRowId > info.LastRowId
                || info.FirstRowId < previousLast)
            {
                throw new InvalidDataException($"SSTable block index entry {i} is corrupt.");
            }

            previousLast = info.LastRowId;
            previousEnd = info.Offset + info.Length;
        }
    }

    // A struct passed by value through the block callback: a lookup allocates nothing.
    private readonly struct LookupRequest<TState, TResult>(
        SsTableV2Reader reader,
        long rowId,
        ulong snapshotSequence,
        TState state,
        RowVersionFunc<TState, TResult> onFound,
        int block)
    {
        public SsTableV2Reader Reader { get; } = reader;
        public long RowId { get; } = rowId;
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

/// <summary>Sequential cursor over all entries of a v2 SSTable, block by block.</summary>
internal sealed class SsTableV2Cursor
{
    private readonly SsTableV2Reader _reader;
    private ReadOnlyMemory<byte> _block;
    private int _blockIndex = -1;
    private int _entry;
    private int _entryCount;

    internal SsTableV2Cursor(SsTableV2Reader reader)
    {
        _reader = reader;
    }

    public long RowId { get; private set; }

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
        }

        ReadOnlySpan<byte> entry = _block.Span.Slice(SsTableV2Format.BlockHeaderSize + (_entry * SsTableV2Format.EntrySize), SsTableV2Format.EntrySize);
        RowId = BinaryPrimitives.ReadInt64LittleEndian(entry);
        ulong tag = BinaryPrimitives.ReadUInt64LittleEndian(entry[8..]);
        Sequence = SsTableV2Format.SequenceOf(tag);
        Type = SsTableV2Format.TypeOf(tag);
        _valueOffset = BinaryPrimitives.ReadInt32LittleEndian(entry[16..]);
        _valueLength = BinaryPrimitives.ReadInt32LittleEndian(entry[20..]);
        _entry++;
        return true;
    }
}
