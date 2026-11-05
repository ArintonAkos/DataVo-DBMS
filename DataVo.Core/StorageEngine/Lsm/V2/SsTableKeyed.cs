using System.Buffers;
using System.Buffers.Binary;
using DataVo.Core.Transactions;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Layout of keyed SSTables (format version 3): variable-length user keys and values, used by index
/// tables. Same skeleton as v2 (page-aligned blocks, block index, blocked Bloom filter, properties,
/// footer, per-block CRC32C).
/// </summary>
/// <remarks>
/// <code>
/// block  := header(16) | u32 entry offset[count] | entry* | padding to the alignment
/// entry  := u16 user key length | u32 value length | user key | value | u64 tag
/// index  := u32 count | u32 reserved | record(32)[count] | key bytes
/// record := i64 offset | i32 length | i32 entry count | i32 first key offset | i32 first key length
///           | i32 last key offset | i32 last key length            (key offsets from the index start)
/// bloom  := over user key minus its last BloomSuffixLength bytes (index tables: minus the row id)
/// footer := index | bloom | properties handles | u32 alignment | u32 bloom suffix length | u16 version (3) | u32 magic
/// </code>
/// Entries are sorted by user key ascending, then sequence descending.
/// </remarks>
internal static class SsTableKeyedFormat
{
    public const ushort Version = 3;
    public const int BlockHeaderSize = 16;
    public const int EntryHeaderSize = sizeof(ushort) + sizeof(uint);
    public const int IndexHeaderSize = 8;
    public const int IndexRecordSize = 32;
    public const int BloomHeaderSize = 16;
    public const int PropertiesSize = 64;
    public const int FooterSize = (SsTableV2Format.BlockHandleSize * 3) + sizeof(uint) + sizeof(uint) + sizeof(ushort) + sizeof(uint);

    public static bool IsKeyedFile(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        if (stream.Length < FooterSize)
        {
            return false;
        }

        Span<byte> tail = stackalloc byte[sizeof(ushort) + sizeof(uint)];
        stream.Position = stream.Length - tail.Length;
        int read = 0;
        while (read < tail.Length)
        {
            int n = stream.Read(tail[read..]);
            if (n == 0)
            {
                return false;
            }

            read += n;
        }

        return SsTableV2Format.TryReadVersion(tail, out ushort version) && version == Version;
    }

    /// <summary>64-bit FNV-1a of a byte key, mixed by the Bloom filter: keys are arbitrary bytes here.</summary>
    public static long HashKey(ReadOnlySpan<byte> key)
    {
        ulong hash = 14695981039346656037UL;
        foreach (byte b in key)
        {
            hash = (hash ^ b) * 1099511628211UL;
        }

        return unchecked((long)hash);
    }
}

/// <summary>Per-file statistics of a keyed SSTable.</summary>
internal readonly record struct SsTableKeyedProperties(
    long EntryCount,
    long PutCount,
    long TombstoneCount,
    ulong SmallestSequence,
    ulong LargestSequence,
    int BlockCount)
{
    public void WriteTo(Span<byte> destination)
    {
        destination[..SsTableKeyedFormat.PropertiesSize].Clear();
        BinaryPrimitives.WriteInt64LittleEndian(destination[0..], EntryCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[8..], PutCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[16..], TombstoneCount);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[24..], SmallestSequence);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[32..], LargestSequence);
        BinaryPrimitives.WriteInt32LittleEndian(destination[40..], BlockCount);
    }

    public static SsTableKeyedProperties ReadFrom(ReadOnlySpan<byte> source) => new(
        BinaryPrimitives.ReadInt64LittleEndian(source[0..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[8..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[16..]),
        BinaryPrimitives.ReadUInt64LittleEndian(source[24..]),
        BinaryPrimitives.ReadUInt64LittleEndian(source[32..]),
        BinaryPrimitives.ReadInt32LittleEndian(source[40..]));
}

/// <summary>Streams a keyed SSTable block by block (see <see cref="SsTableKeyedFormat"/>).</summary>
internal sealed class SsTableKeyedWriter : IDisposable
{
    private static readonly byte[] Zeros = new byte[SsTableV2Format.MinimumAlignment];

    private readonly Stream _output;
    private readonly int _alignment;
    private readonly int _bloomSuffixLength;
    private readonly List<(long Offset, int Length, int Count, byte[] First, byte[] Last)> _blocks = [];
    private readonly List<long> _bloomKeys = [];

    private byte[] _entries;
    private readonly List<int> _entryOffsets = [];
    private int _entriesLength;
    private byte[]? _blockFirstKey;
    private byte[]? _lastKey;
    private ulong _lastSequence;
    private byte[]? _lastBloomKey;
    private long _position;
    private long _entryCount;
    private long _putCount;
    private long _tombstoneCount;
    private ulong _smallestSequence = ulong.MaxValue;
    private ulong _largestSequence;
    private bool _finished;

    public SsTableKeyedWriter(Stream output, int bloomSuffixLength, int alignment = 0)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(output);
        _output = output;
        _bloomSuffixLength = bloomSuffixLength;
        _alignment = alignment == 0 ? SsTableV2Format.DefaultAlignment : alignment;
        _entries = ArrayPool<byte>.Shared.Rent(_alignment);
    }

    public long EntryCount => _entryCount;

    public void Add(ReadOnlySpan<byte> userKey, ulong sequence, LsmValueType type, ReadOnlySpan<byte> value)
    {
        if (_finished)
        {
            throw new InvalidOperationException("Cannot add entries after Finish.");
        }

        if (userKey.Length > ushort.MaxValue || userKey.Length < _bloomSuffixLength)
        {
            throw new ArgumentException("User key length is out of range.", nameof(userKey));
        }

        if (_lastKey is not null)
        {
            int order = userKey.SequenceCompareTo(_lastKey);
            if (order < 0 || (order == 0 && sequence >= _lastSequence))
            {
                throw new InvalidOperationException("Keyed SSTable entries must be sorted by user key ascending, then sequence descending.");
            }
        }

        int entryLength = SsTableKeyedFormat.EntryHeaderSize + userKey.Length + value.Length + sizeof(ulong);
        int blockWithEntry = SsTableKeyedFormat.BlockHeaderSize + ((_entryOffsets.Count + 1) * sizeof(uint)) + _entriesLength + entryLength;
        if (_entryOffsets.Count > 0 && blockWithEntry > _alignment)
        {
            FlushBlock();
        }

        EnsureCapacity(_entriesLength + entryLength);
        Span<byte> entry = _entries.AsSpan(_entriesLength, entryLength);
        BinaryPrimitives.WriteUInt16LittleEndian(entry, (ushort)userKey.Length);
        BinaryPrimitives.WriteInt32LittleEndian(entry[2..], value.Length);
        userKey.CopyTo(entry[SsTableKeyedFormat.EntryHeaderSize..]);
        value.CopyTo(entry[(SsTableKeyedFormat.EntryHeaderSize + userKey.Length)..]);
        BinaryPrimitives.WriteUInt64LittleEndian(entry[^sizeof(ulong)..], SsTableV2Format.PackTag(sequence, type));
        _entryOffsets.Add(_entriesLength);
        _entriesLength += entryLength;

        _blockFirstKey ??= userKey.ToArray();
        if (_lastKey is null || !userKey.SequenceEqual(_lastKey))
        {
            _lastKey = userKey.ToArray();
            ReadOnlySpan<byte> bloomKey = userKey[..^_bloomSuffixLength];
            if (_lastBloomKey is null || !bloomKey.SequenceEqual(_lastBloomKey))
            {
                _lastBloomKey = bloomKey.ToArray();
                _bloomKeys.Add(SsTableKeyedFormat.HashKey(bloomKey));
            }
        }

        _lastSequence = sequence;
        _entryCount++;
        _smallestSequence = Math.Min(_smallestSequence, sequence);
        _largestSequence = Math.Max(_largestSequence, sequence);
        if (type == LsmValueType.Put)
        {
            _putCount++;
        }
        else
        {
            _tombstoneCount++;
        }
    }

    public long Finish()
    {
        if (_finished)
        {
            throw new InvalidOperationException("Finish can only be called once.");
        }

        if (_entryCount == 0)
        {
            throw new InvalidOperationException("Cannot finish an empty SSTable.");
        }

        _finished = true;
        if (_entryOffsets.Count > 0)
        {
            FlushBlock();
        }

        // Block index: fixed records, then the first/last keys they point at.
        PadTo(8);
        long indexOffset = _position;
        int keysLength = 0;
        foreach (var block in _blocks)
        {
            keysLength += block.First.Length + block.Last.Length;
        }

        int recordsLength = SsTableKeyedFormat.IndexHeaderSize + (_blocks.Count * SsTableKeyedFormat.IndexRecordSize);
        int indexLength = recordsLength + keysLength;
        byte[] index = ArrayPool<byte>.Shared.Rent(indexLength);
        try
        {
            Span<byte> span = index.AsSpan(0, indexLength);
            span.Clear();
            BinaryPrimitives.WriteInt32LittleEndian(span, _blocks.Count);
            int keyOffset = recordsLength;
            for (int i = 0; i < _blocks.Count; i++)
            {
                var block = _blocks[i];
                Span<byte> record = span.Slice(SsTableKeyedFormat.IndexHeaderSize + (i * SsTableKeyedFormat.IndexRecordSize), SsTableKeyedFormat.IndexRecordSize);
                BinaryPrimitives.WriteInt64LittleEndian(record, block.Offset);
                BinaryPrimitives.WriteInt32LittleEndian(record[8..], block.Length);
                BinaryPrimitives.WriteInt32LittleEndian(record[12..], block.Count);
                BinaryPrimitives.WriteInt32LittleEndian(record[16..], keyOffset);
                BinaryPrimitives.WriteInt32LittleEndian(record[20..], block.First.Length);
                block.First.CopyTo(span[keyOffset..]);
                keyOffset += block.First.Length;
                BinaryPrimitives.WriteInt32LittleEndian(record[24..], keyOffset);
                BinaryPrimitives.WriteInt32LittleEndian(record[28..], block.Last.Length);
                block.Last.CopyTo(span[keyOffset..]);
                keyOffset += block.Last.Length;
            }

            Write(span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(index);
        }

        // Bloom filter over the key prefixes.
        int bloomBlocks = BlockedBloomFilter.ComputeBlockCount(_bloomKeys.Count, SsTableV2Format.BloomBitsPerKey);
        PadTo(BlockedBloomFilter.BlockBytes);
        PadBytes(BlockedBloomFilter.BlockBytes - SsTableKeyedFormat.BloomHeaderSize);
        long bloomOffset = _position;
        int bloomLength = SsTableKeyedFormat.BloomHeaderSize + (bloomBlocks * BlockedBloomFilter.BlockBytes);
        byte[] bloom = ArrayPool<byte>.Shared.Rent(bloomLength);
        try
        {
            Span<byte> span = bloom.AsSpan(0, bloomLength);
            span.Clear();
            BinaryPrimitives.WriteInt32LittleEndian(span, bloomBlocks);
            BinaryPrimitives.WriteInt32LittleEndian(span[4..], BlockedBloomFilter.ProbeCount);
            Span<byte> bits = span[SsTableKeyedFormat.BloomHeaderSize..];
            foreach (long key in _bloomKeys)
            {
                BlockedBloomFilter.Add(bits, key);
            }

            Write(span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bloom);
        }

        PadTo(8);
        long propertiesOffset = _position;
        Span<byte> properties = stackalloc byte[SsTableKeyedFormat.PropertiesSize];
        new SsTableKeyedProperties(_entryCount, _putCount, _tombstoneCount, _smallestSequence, _largestSequence, _blocks.Count).WriteTo(properties);
        Write(properties);

        Span<byte> footer = stackalloc byte[SsTableKeyedFormat.FooterSize];
        WriteHandle(footer[0..], indexOffset, indexLength);
        WriteHandle(footer[12..], bloomOffset, bloomLength);
        WriteHandle(footer[24..], propertiesOffset, SsTableKeyedFormat.PropertiesSize);
        BinaryPrimitives.WriteInt32LittleEndian(footer[36..], _alignment);
        BinaryPrimitives.WriteInt32LittleEndian(footer[40..], _bloomSuffixLength);
        BinaryPrimitives.WriteUInt16LittleEndian(footer[44..], SsTableKeyedFormat.Version);
        BinaryPrimitives.WriteUInt32LittleEndian(footer[46..], SsTableV2Format.Magic);
        Write(footer);
        _output.Flush();
        return _position;
    }

    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(_entries);
        _entries = [];
    }

    private void FlushBlock()
    {
        int count = _entryOffsets.Count;
        int offsetsLength = count * sizeof(uint);
        int dataStart = SsTableKeyedFormat.BlockHeaderSize + offsetsLength;
        int payloadLength = offsetsLength + _entriesLength;
        int blockLength = SsTableKeyedFormat.BlockHeaderSize + payloadLength;
        byte[] block = ArrayPool<byte>.Shared.Rent(blockLength);
        try
        {
            Span<byte> span = block.AsSpan(0, blockLength);
            BinaryPrimitives.WriteInt32LittleEndian(span[4..], count);
            BinaryPrimitives.WriteInt32LittleEndian(span[8..], 0);
            BinaryPrimitives.WriteInt32LittleEndian(span[12..], payloadLength);
            for (int i = 0; i < count; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(span[(SsTableKeyedFormat.BlockHeaderSize + (i * sizeof(uint)))..], dataStart + _entryOffsets[i]);
            }

            _entries.AsSpan(0, _entriesLength).CopyTo(span[dataStart..]);
            BinaryPrimitives.WriteUInt32LittleEndian(span, WalCrc32C.HashToUInt32(span[4..]));

            long offset = _position;
            Write(span);
            PadTo(_alignment);

            ReadOnlySpan<byte> lastEntry = _entries.AsSpan(_entryOffsets[^1]);
            int lastKeyLength = BinaryPrimitives.ReadUInt16LittleEndian(lastEntry);
            byte[] lastKey = lastEntry.Slice(SsTableKeyedFormat.EntryHeaderSize, lastKeyLength).ToArray();
            _blocks.Add((offset, blockLength, count, _blockFirstKey!, lastKey));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(block);
        }

        _entryOffsets.Clear();
        _entriesLength = 0;
        _blockFirstKey = null;
    }

    private void EnsureCapacity(int required)
    {
        if (_entries.Length >= required)
        {
            return;
        }

        byte[] grown = ArrayPool<byte>.Shared.Rent(Math.Max(required, _entries.Length * 2));
        _entries.AsSpan(0, _entriesLength).CopyTo(grown);
        ArrayPool<byte>.Shared.Return(_entries);
        _entries = grown;
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
        BinaryPrimitives.WriteInt64LittleEndian(destination, offset);
        BinaryPrimitives.WriteInt32LittleEndian(destination[8..], length);
    }
}

/// <summary>Visits one entry; spans are valid only during the call.</summary>
internal delegate void KeyedEntryAction<TState>(ReadOnlySpan<byte> userKey, ulong sequence, LsmValueType type, ReadOnlySpan<byte> value, TState state);

/// <summary>Reads a keyed SSTable; opening validates only metadata (O(blocks)).</summary>
internal sealed class SsTableKeyedReader : IDisposable
{
    private readonly SsTableBlockSource _source;
    private readonly ReadOnlyMemory<byte> _index;
    private readonly ReadOnlyMemory<byte> _bloomBits;
    private readonly int[] _verifiedBlocks;

    private SsTableKeyedReader(SsTableBlockSource source, ReadOnlyMemory<byte> index, ReadOnlyMemory<byte> bloomBits, SsTableKeyedProperties properties, int bloomSuffixLength)
    {
        _source = source;
        _index = index;
        _bloomBits = bloomBits;
        Properties = properties;
        BloomSuffixLength = bloomSuffixLength;
        _verifiedBlocks = new int[(properties.BlockCount + 31) / 32];
    }

    public SsTableKeyedProperties Properties { get; }

    public int BloomSuffixLength { get; }

    public int BlockCount => Properties.BlockCount;

    public static SsTableKeyedReader Open(string path, LsmReadMode mode = LsmReadMode.Auto)
    {
        SsTableBlockSource source = SsTableBlockSource.Open(path, mode);
        try
        {
            long length = source.Length;
            if (length < SsTableKeyedFormat.FooterSize)
            {
                throw new InvalidDataException("Keyed SSTable is shorter than its footer.");
            }

            ReadOnlySpan<byte> footer = source.GetRegion(length - SsTableKeyedFormat.FooterSize, SsTableKeyedFormat.FooterSize).Span;
            if (!SsTableV2Format.TryReadVersion(footer, out ushort version) || version != SsTableKeyedFormat.Version)
            {
                throw new InvalidDataException("SSTable footer is not a keyed (v3) footer.");
            }

            (long indexOffset, int indexLength) = ReadHandle(footer[0..]);
            (long bloomOffset, int bloomLength) = ReadHandle(footer[12..]);
            (long propertiesOffset, int propertiesLength) = ReadHandle(footer[24..]);
            int alignment = BinaryPrimitives.ReadInt32LittleEndian(footer[36..]);
            int bloomSuffix = BinaryPrimitives.ReadInt32LittleEndian(footer[40..]);
            long dataEnd = length - SsTableKeyedFormat.FooterSize;
            if (alignment < SsTableV2Format.MinimumAlignment || (alignment & (alignment - 1)) != 0 || bloomSuffix < 0
                || indexOffset < 0 || indexLength < SsTableKeyedFormat.IndexHeaderSize || indexOffset > dataEnd - indexLength
                || bloomOffset < 0 || bloomLength < SsTableKeyedFormat.BloomHeaderSize || bloomOffset > dataEnd - bloomLength
                || propertiesLength != SsTableKeyedFormat.PropertiesSize || propertiesOffset < 0 || propertiesOffset > dataEnd - propertiesLength)
            {
                throw new InvalidDataException("Keyed SSTable footer is corrupt.");
            }

            var properties = SsTableKeyedProperties.ReadFrom(source.GetRegion(propertiesOffset, propertiesLength).Span);
            ReadOnlyMemory<byte> index = source.GetRegion(indexOffset, indexLength);
            ReadOnlySpan<byte> indexSpan = index.Span;
            int count = BinaryPrimitives.ReadInt32LittleEndian(indexSpan);
            if (count <= 0 || count != properties.BlockCount
                || SsTableKeyedFormat.IndexHeaderSize + ((long)count * SsTableKeyedFormat.IndexRecordSize) > indexLength)
            {
                throw new InvalidDataException("Keyed SSTable block index does not match its properties.");
            }

            for (int i = 0; i < count; i++)
            {
                ReadOnlySpan<byte> record = indexSpan.Slice(SsTableKeyedFormat.IndexHeaderSize + (i * SsTableKeyedFormat.IndexRecordSize), SsTableKeyedFormat.IndexRecordSize);
                long offset = BinaryPrimitives.ReadInt64LittleEndian(record);
                int blockLength = BinaryPrimitives.ReadInt32LittleEndian(record[8..]);
                if (offset % alignment != 0 || offset < 0 || blockLength < SsTableKeyedFormat.BlockHeaderSize || offset + blockLength > indexOffset
                    || !KeyInRange(record[16..], indexLength) || !KeyInRange(record[24..], indexLength))
                {
                    throw new InvalidDataException($"Keyed SSTable block index entry {i} is corrupt.");
                }
            }

            ReadOnlySpan<byte> bloomHeader = source.GetRegion(bloomOffset, SsTableKeyedFormat.BloomHeaderSize).Span;
            int bloomBlocks = BinaryPrimitives.ReadInt32LittleEndian(bloomHeader);
            if (bloomBlocks <= 0 || SsTableKeyedFormat.BloomHeaderSize + ((long)bloomBlocks * BlockedBloomFilter.BlockBytes) != bloomLength)
            {
                throw new InvalidDataException("Keyed SSTable Bloom filter header is corrupt.");
            }

            ReadOnlyMemory<byte> bloomBits = source.GetRegion(bloomOffset + SsTableKeyedFormat.BloomHeaderSize, bloomBlocks * BlockedBloomFilter.BlockBytes);
            return new SsTableKeyedReader(source, index, bloomBits, properties, bloomSuffix);
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Visits the newest version (by sequence, file-local) of every user key that starts with
    /// <paramref name="bloomKey"/> and is exactly <c>bloomKey.Length + BloomSuffixLength</c> long.
    /// </summary>
    public void ForEachWithPrefix<TState>(ReadOnlySpan<byte> bloomKey, TState state, KeyedEntryAction<TState> visit)
    {
        if (!BlockedBloomFilter.MightContain(_bloomBits.Span, SsTableKeyedFormat.HashKey(bloomKey)))
        {
            return;
        }

        int block = FindFirstBlockEndingAtOrAfter(bloomKey);

        // Versions of one user key are adjacent (sequence descending): only the first is visited. Within a
        // block the previous entry is compared in place; the last key of a block is kept for the next one.
        Span<byte> carried = stackalloc byte[256];
        byte[]? carriedHeap = null;
        int carriedLength = -1;
        for (; block < BlockCount; block++)
        {
            ReadOnlySpan<byte> firstKey = BlockKey(block, first: true);
            if (ComparePrefix(firstKey, bloomKey) > 0)
            {
                return; // the block starts past every key with this prefix
            }

            ReadOnlySpan<byte> bytes = GetVerifiedBlock(block).Span;
            int count = BinaryPrimitives.ReadInt32LittleEndian(bytes[4..]);
            int start = LowerBound(bytes, count, bloomKey);
            ReadOnlySpan<byte> previous = carriedLength < 0
                ? ReadOnlySpan<byte>.Empty
                : (carriedHeap is null ? carried[..carriedLength] : carriedHeap.AsSpan(0, carriedLength));
            bool hasPrevious = carriedLength >= 0;
            for (int i = start; i < count; i++)
            {
                ReadEntry(bytes, i, out ReadOnlySpan<byte> userKey, out ReadOnlySpan<byte> value, out ulong tag);
                int order = ComparePrefix(userKey, bloomKey);
                if (order > 0)
                {
                    return;
                }

                if (order < 0 || userKey.Length != bloomKey.Length + BloomSuffixLength)
                {
                    continue;
                }

                if (hasPrevious && userKey.SequenceEqual(previous))
                {
                    continue;
                }

                previous = userKey;
                hasPrevious = true;
                visit(userKey, SsTableV2Format.SequenceOf(tag), SsTableV2Format.TypeOf(tag), value, state);
            }

            if (hasPrevious)
            {
                if (previous.Length <= carried.Length)
                {
                    previous.CopyTo(carried);
                    carriedHeap = null;
                }
                else
                {
                    carriedHeap = previous.ToArray();
                }

                carriedLength = previous.Length;
            }
        }
    }

    /// <summary>Visits every entry (all versions) in order.</summary>
    public void ForEach<TState>(TState state, KeyedEntryAction<TState> visit)
    {
        for (int block = 0; block < BlockCount; block++)
        {
            ReadOnlySpan<byte> bytes = GetVerifiedBlock(block).Span;
            int count = BinaryPrimitives.ReadInt32LittleEndian(bytes[4..]);
            for (int i = 0; i < count; i++)
            {
                ReadEntry(bytes, i, out ReadOnlySpan<byte> userKey, out ReadOnlySpan<byte> value, out ulong tag);
                visit(userKey, SsTableV2Format.SequenceOf(tag), SsTableV2Format.TypeOf(tag), value, state);
            }
        }
    }

    public void Dispose() => _source.Dispose();

    private ReadOnlyMemory<byte> GetVerifiedBlock(int block)
    {
        ReadOnlySpan<byte> record = Record(block);
        long offset = BinaryPrimitives.ReadInt64LittleEndian(record);
        int length = BinaryPrimitives.ReadInt32LittleEndian(record[8..]);
        ReadOnlyMemory<byte> bytes = _source.GetRegion(offset, length);
        VerifyOnce(block, bytes.Span);
        return bytes;
    }

    private ReadOnlySpan<byte> Record(int block) =>
        _index.Span.Slice(SsTableKeyedFormat.IndexHeaderSize + (block * SsTableKeyedFormat.IndexRecordSize), SsTableKeyedFormat.IndexRecordSize);

    private ReadOnlySpan<byte> BlockKey(int block, bool first)
    {
        ReadOnlySpan<byte> record = Record(block);
        int offset = BinaryPrimitives.ReadInt32LittleEndian(record[(first ? 16 : 24)..]);
        int length = BinaryPrimitives.ReadInt32LittleEndian(record[(first ? 20 : 28)..]);
        return _index.Span.Slice(offset, length);
    }

    private int FindFirstBlockEndingAtOrAfter(ReadOnlySpan<byte> prefix)
    {
        int low = 0;
        int high = BlockCount;
        while (low < high)
        {
            int mid = (int)((uint)(low + high) >> 1);
            if (ComparePrefix(BlockKey(mid, first: false), prefix) < 0)
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

    private static int LowerBound(ReadOnlySpan<byte> block, int count, ReadOnlySpan<byte> prefix)
    {
        int low = 0;
        int high = count;
        while (low < high)
        {
            int mid = (int)((uint)(low + high) >> 1);
            ReadEntry(block, mid, out ReadOnlySpan<byte> userKey, out _, out _);
            if (ComparePrefix(userKey, prefix) < 0)
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

    /// <summary>Orders <paramref name="key"/> against all keys starting with <paramref name="prefix"/> (0 = starts with it).</summary>
    private static int ComparePrefix(ReadOnlySpan<byte> key, ReadOnlySpan<byte> prefix)
    {
        int length = Math.Min(key.Length, prefix.Length);
        int order = key[..length].SequenceCompareTo(prefix[..length]);
        if (order != 0)
        {
            return order;
        }

        return key.Length < prefix.Length ? -1 : 0;
    }

    private static void ReadEntry(ReadOnlySpan<byte> block, int index, out ReadOnlySpan<byte> userKey, out ReadOnlySpan<byte> value, out ulong tag)
    {
        int offset = BinaryPrimitives.ReadInt32LittleEndian(block[(SsTableKeyedFormat.BlockHeaderSize + (index * sizeof(uint)))..]);
        ReadOnlySpan<byte> entry = block[offset..];
        int keyLength = BinaryPrimitives.ReadUInt16LittleEndian(entry);
        int valueLength = BinaryPrimitives.ReadInt32LittleEndian(entry[2..]);
        userKey = entry.Slice(SsTableKeyedFormat.EntryHeaderSize, keyLength);
        value = entry.Slice(SsTableKeyedFormat.EntryHeaderSize + keyLength, valueLength);
        tag = BinaryPrimitives.ReadUInt64LittleEndian(entry[(SsTableKeyedFormat.EntryHeaderSize + keyLength + valueLength)..]);
    }

    private void VerifyOnce(int block, ReadOnlySpan<byte> bytes)
    {
        int word = block >> 5;
        int bit = 1 << (block & 31);
        if ((Volatile.Read(ref _verifiedBlocks[word]) & bit) != 0)
        {
            return;
        }

        int count = bytes.Length >= SsTableKeyedFormat.BlockHeaderSize ? BinaryPrimitives.ReadInt32LittleEndian(bytes[4..]) : -1;
        int payload = bytes.Length >= SsTableKeyedFormat.BlockHeaderSize ? BinaryPrimitives.ReadInt32LittleEndian(bytes[12..]) : -1;
        if (count <= 0 || payload < (long)count * sizeof(uint) || SsTableKeyedFormat.BlockHeaderSize + payload != bytes.Length)
        {
            throw new InvalidDataException($"Keyed SSTable block {block} header is corrupt.");
        }

        if (WalCrc32C.HashToUInt32(bytes[4..]) != BinaryPrimitives.ReadUInt32LittleEndian(bytes))
        {
            throw new InvalidDataException($"Keyed SSTable block {block} failed its CRC32C check.");
        }

        int dataStart = SsTableKeyedFormat.BlockHeaderSize + (count * sizeof(uint));
        for (int i = 0; i < count; i++)
        {
            int offset = BinaryPrimitives.ReadInt32LittleEndian(bytes[(SsTableKeyedFormat.BlockHeaderSize + (i * sizeof(uint)))..]);
            if (offset < dataStart || offset > bytes.Length - SsTableKeyedFormat.EntryHeaderSize)
            {
                throw new InvalidDataException($"Keyed SSTable block {block} entry {i} points outside the block.");
            }

            ReadOnlySpan<byte> entry = bytes[offset..];
            long end = (long)SsTableKeyedFormat.EntryHeaderSize + BinaryPrimitives.ReadUInt16LittleEndian(entry)
                + BinaryPrimitives.ReadInt32LittleEndian(entry[2..]) + sizeof(ulong);
            if (BinaryPrimitives.ReadInt32LittleEndian(entry[2..]) < 0 || end > entry.Length)
            {
                throw new InvalidDataException($"Keyed SSTable block {block} entry {i} is truncated.");
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

    private static bool KeyInRange(ReadOnlySpan<byte> offsetAndLength, int indexLength)
    {
        int offset = BinaryPrimitives.ReadInt32LittleEndian(offsetAndLength);
        int length = BinaryPrimitives.ReadInt32LittleEndian(offsetAndLength[4..]);
        return offset >= 0 && length >= 0 && offset <= indexLength - length;
    }
}
