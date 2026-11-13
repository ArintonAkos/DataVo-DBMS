using System.Buffers.Binary;
using System.Runtime.InteropServices;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Memory.Flat;
using DataVo.Core.Transactions;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// The columnar segment of a version 8 SSTable (<see cref="SsTableColumnarFormat"/>), opened for reading.
/// Opening checks and decodes the directory only; chunk groups stay in the file until a
/// <see cref="SsTableColumnarChunkReader"/> loads one.
/// </summary>
internal sealed class SsTableColumnarSegment
{
    private readonly SsTableBlockSource _source;
    private readonly SsTableColumnarHeader _header;
    private readonly SsTableColumnarChunk[] _chunks;
    private readonly SsTableColumnarColumn[] _columns;
    private readonly byte[] _columnFlags; // per slot, the flags of every chunk joined
    private readonly int[] _verified;
    private ReadOnlyMemory<byte> _tombstones;
    private int _tombstonesLoaded;

    private SsTableColumnarSegment(SsTableBlockSource source, SsTableColumnarHeader header, ColumnarSpec spec, SsTableColumnarChunk[] chunks, SsTableColumnarColumn[] columns)
    {
        _source = source;
        _header = header;
        Spec = spec;
        _chunks = chunks;
        _columns = columns;
        _verified = new int[(chunks.Length + 31) / 32];
        _columnFlags = new byte[header.ColumnCount];
        for (int i = 0; i < columns.Length; i++)
        {
            _columnFlags[i % header.ColumnCount] |= columns[i].Flags;
        }
    }

    /// <summary>The columns the segment holds.</summary>
    public ColumnarSpec Spec { get; }

    /// <summary>False when the file holds a row the segment could not represent; scans must then read rows.</summary>
    public bool Usable => (_header.Flags & SsTableColumnarFormat.UnusableFlag) == 0;

    /// <summary>The number of rows in the segment: the file's row keys whose newest version is a put.</summary>
    public long RowCount => _header.RowCount;

    /// <summary>The number of chunk groups.</summary>
    public int ChunkCount => _chunks.Length;

    /// <summary>The number of keys the file deletes.</summary>
    public long TombstoneCount => _header.TombstoneCount;

    /// <summary>The bytes of one tombstone key: 8 (integer keys) or 16.</summary>
    public int TombstoneKeyWidth => _header.TombstoneKeyWidth;

    /// <summary>Whether blocks are served from mapped memory.</summary>
    public bool IsMapped => _source.IsMapped;

    /// <summary>One chunk group's record.</summary>
    public ref readonly SsTableColumnarChunk Chunk(int chunk) => ref _chunks[chunk];

    /// <summary>One column of one chunk group.</summary>
    public ref readonly SsTableColumnarColumn Column(int chunk, int slot) => ref _columns[(chunk * _header.ColumnCount) + slot];

    /// <summary>Whether any chunk holds a NULL in a column.</summary>
    public bool AnyNull(int slot) => (_columnFlags[slot] & SsTableColumnarFormat.AnyNull) != 0;

    /// <summary>Whether any chunk holds a NaN in a column.</summary>
    public bool AnyNaN(int slot) => (_columnFlags[slot] & SsTableColumnarFormat.AnyNaN) != 0;

    /// <summary>The first chunk whose last key is at or after <paramref name="key"/>, or <see cref="ChunkCount"/>.</summary>
    public int FindChunk(RowKey key)
    {
        int low = 0;
        int high = _chunks.Length;
        while (low < high)
        {
            int mid = (low + high) >>> 1;
            if (_chunks[mid].LastKey < key)
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

    /// <summary>
    /// The keys the file deletes, ascending, each <see cref="TombstoneKeyWidth"/> bytes: the low half of an
    /// integer key, or both halves high first. Checked against their CRC the first time they are read.
    /// </summary>
    public ReadOnlySpan<byte> Tombstones()
    {
        if (Volatile.Read(ref _tombstonesLoaded) == 0)
        {
            lock (_verified)
            {
                if (_tombstonesLoaded == 0)
                {
                    ReadOnlyMemory<byte> bytes = _source.GetRegion(_header.TombstoneOffset, checked((int)(_header.TombstoneCount * _header.TombstoneKeyWidth)));
                    if (WalCrc32C.HashToUInt32(bytes.Span) != _header.TombstoneCrc)
                    {
                        throw new InvalidDataException("SSTable columnar tombstone keys failed their checksum.");
                    }

                    _tombstones = bytes;
                    Volatile.Write(ref _tombstonesLoaded, 1);
                }
            }
        }

        return _tombstones.Span;
    }

    /// <summary>Tombstone key <paramref name="index"/> of <see cref="Tombstones"/>.</summary>
    public static RowKey TombstoneAt(ReadOnlySpan<byte> tombstones, int width, int index) =>
        width == 8
            ? new RowKey(0, BinaryPrimitives.ReadUInt64LittleEndian(tombstones[(index * 8)..]))
            : SsTableV2Format.ReadKey(tombstones[(index * 16)..]);

    /// <summary>
    /// Opens the segment whose directory is at [<paramref name="directoryOffset"/>, +<paramref name="directoryLength"/>).
    /// </summary>
    /// <exception cref="InvalidDataException">The directory is corrupt or points outside the segment.</exception>
    public static SsTableColumnarSegment Open(SsTableBlockSource source, long directoryOffset, int directoryLength)
    {
        if (directoryLength < SsTableColumnarFormat.HeaderSize + sizeof(uint))
        {
            throw new InvalidDataException("SSTable columnar directory is truncated.");
        }

        ReadOnlySpan<byte> directory = source.GetRegion(directoryOffset, directoryLength).Span;
        if (WalCrc32C.HashToUInt32(directory[..^sizeof(uint)]) != BinaryPrimitives.ReadUInt32LittleEndian(directory[^sizeof(uint)..]))
        {
            throw new InvalidDataException("SSTable columnar directory failed its checksum.");
        }

        SsTableColumnarHeader header = SsTableColumnarHeader.ReadFrom(directory);
        if (header.ColumnCount < 0 || header.ChunkCount < 0 || header.SpecLength < 0
            || header.TombstoneKeyWidth is not (8 or 16)
            || header.TombstoneCount < 0
            || header.TombstoneOffset < 0
            || header.TombstoneOffset + (header.TombstoneCount * header.TombstoneKeyWidth) > directoryOffset
            || SsTableColumnarFormat.DirectoryLength(header.SpecLength, header.ChunkCount, header.ColumnCount) != directoryLength)
        {
            throw new InvalidDataException("SSTable columnar directory header is corrupt.");
        }

        ColumnarSpec spec = ColumnarSpec.ReadFrom(directory.Slice(SsTableColumnarFormat.HeaderSize, header.SpecLength));
        if (spec.Hash != header.SpecHash || spec.ColumnCount != header.ColumnCount)
        {
            throw new InvalidDataException("SSTable columnar directory does not match its spec.");
        }

        ReadOnlySpan<byte> records = directory[(SsTableColumnarFormat.HeaderSize + SsTableColumnarFormat.PaddedSpecLength(header.SpecLength))..];
        var chunks = new SsTableColumnarChunk[header.ChunkCount];
        var columns = new SsTableColumnarColumn[header.ChunkCount * header.ColumnCount];
        long rows = 0;
        for (int i = 0; i < chunks.Length; i++)
        {
            chunks[i] = SsTableColumnarChunk.ReadFrom(records[(i * SsTableColumnarFormat.ChunkRecordSize)..]);
            ValidateChunk(chunks, i, header.TombstoneOffset);
            rows += chunks[i].Rows;
        }

        ReadOnlySpan<byte> columnRecords = records[(chunks.Length * SsTableColumnarFormat.ChunkRecordSize)..];
        for (int i = 0; i < columns.Length; i++)
        {
            columns[i] = SsTableColumnarColumn.ReadFrom(columnRecords[(i * SsTableColumnarFormat.ColumnRecordSize)..]);
            ValidateColumn(in columns[i], in chunks[i / header.ColumnCount], spec.KindOf(i % header.ColumnCount), i);
        }

        if (rows != header.RowCount)
        {
            throw new InvalidDataException("SSTable columnar directory row count does not match its chunks.");
        }

        return new SsTableColumnarSegment(source, header, spec, chunks, columns);
    }

    private static void ValidateChunk(SsTableColumnarChunk[] chunks, int i, long end)
    {
        ref readonly SsTableColumnarChunk chunk = ref chunks[i];
        if (chunk.Rows is < 1 or > SsTableColumnarFormat.ChunkRows
            || chunk.KeyWidth is not (8 or 16)
            || chunk.Offset < 0
            || chunk.Offset % SsTableColumnarFormat.Alignment != 0
            || chunk.Length < chunk.Rows * chunk.KeyWidth
            || chunk.Offset + chunk.Length > end
            || chunk.FirstKey > chunk.LastKey
            || (i > 0 && (chunk.FirstKey <= chunks[i - 1].LastKey || chunk.Offset < chunks[i - 1].Offset + chunks[i - 1].Length)))
        {
            throw new InvalidDataException($"SSTable columnar chunk {i} is corrupt.");
        }
    }

    private static void ValidateColumn(in SsTableColumnarColumn column, in SsTableColumnarChunk chunk, StorageKind kind, int i)
    {
        int words = Bitmask.WordsFor(chunk.Rows);
        int maskLength = words * sizeof(ulong);
        int valuesLength = kind == StorageKind.Bit ? maskLength : words * 64 * ColumnarCells.WidthOf(kind);
        long end = chunk.Offset + chunk.Length;
        bool hasNulls = column.NullsOffset != 0;
        if (column.ValuesOffset < chunk.Offset
            || column.ValuesOffset % SsTableColumnarFormat.Alignment != 0
            || column.ValuesOffset + valuesLength > end
            || hasNulls != ((column.Flags & SsTableColumnarFormat.AnyNull) != 0)
            || (hasNulls && (column.NullsOffset < chunk.Offset || column.NullsOffset % SsTableColumnarFormat.Alignment != 0 || column.NullsOffset + maskLength > end)))
        {
            throw new InvalidDataException($"SSTable columnar column record {i} is corrupt.");
        }
    }

    /// <summary>
    /// The address of a chunk group in mapped memory, its CRC checked the first time; false when the file is
    /// read positionally and the group must be copied with <see cref="CopyChunk"/>.
    /// </summary>
    internal unsafe bool TryMapChunk(int chunk, out IntPtr address)
    {
        ref readonly SsTableColumnarChunk info = ref _chunks[chunk];
        if (!_source.TryGetRegionAddress(info.Offset, info.Length, out address))
        {
            return false;
        }

        VerifyOnce(chunk, new ReadOnlySpan<byte>((void*)address, info.Length));
        return true;
    }

    /// <summary>Copies a chunk group into <paramref name="buffer"/>, its CRC checked the first time.</summary>
    internal void CopyChunk(int chunk, byte[] buffer)
    {
        ref readonly SsTableColumnarChunk info = ref _chunks[chunk];
        _source.Read(info.Offset, info.Length, buffer, static (bytes, target) =>
        {
            bytes.CopyTo(target);
            return true;
        });
        VerifyOnce(chunk, buffer.AsSpan(0, info.Length));
    }

    private void VerifyOnce(int chunk, ReadOnlySpan<byte> bytes)
    {
        int bit = 1 << (chunk & 31);
        if ((Volatile.Read(ref _verified[chunk >> 5]) & bit) != 0)
        {
            return;
        }

        if (WalCrc32C.HashToUInt32(bytes) != _chunks[chunk].Crc)
        {
            throw new InvalidDataException($"SSTable columnar chunk {chunk} failed its checksum.");
        }

        int current;
        do
        {
            current = Volatile.Read(ref _verified[chunk >> 5]);
        }
        while (Interlocked.CompareExchange(ref _verified[chunk >> 5], current | bit, current) != current);
    }
}

/// <summary>
/// Reads one chunk group of a columnar segment at a time: its keys and, per column, its values and NULL mask,
/// as spans over the mapped file or over this reader's own buffer when the file is read positionally.
/// </summary>
/// <remarks>
/// Not safe for concurrent use; a scan keeps one per thread. Spans are valid until the next <see cref="Load"/>
/// and while the file stays open.
/// </remarks>
internal sealed unsafe class SsTableColumnarChunkReader
{
    private byte[] _buffer = [];
    private byte* _address;
    private SsTableColumnarSegment? _segment;
    private int _chunk;
    private long _offset;
    private int _keyWidth;

    /// <summary>The number of rows in the loaded group.</summary>
    public int Rows { get; private set; }

    /// <summary>The segment the loaded group belongs to.</summary>
    public SsTableColumnarSegment Segment => _segment!;

    /// <summary>Makes chunk group <paramref name="chunk"/> of <paramref name="segment"/> the loaded one.</summary>
    public void Load(SsTableColumnarSegment segment, int chunk)
    {
        if (ReferenceEquals(_segment, segment) && _chunk == chunk)
        {
            return;
        }

        ref readonly SsTableColumnarChunk info = ref segment.Chunk(chunk);
        if (segment.TryMapChunk(chunk, out IntPtr address))
        {
            _address = (byte*)address;
        }
        else
        {
            if (_buffer.Length < info.Length)
            {
                _buffer = new byte[info.Length];
            }

            segment.CopyChunk(chunk, _buffer);
            _address = null;
        }

        _segment = segment;
        _chunk = chunk;
        _offset = info.Offset;
        _keyWidth = info.KeyWidth;
        Rows = info.Rows;
    }

    /// <summary>Forgets the loaded group, so no span into a file outlives the scan that read it.</summary>
    public void Release()
    {
        _segment = null;
        _address = null;
        Rows = 0;
    }

    /// <summary>The key of row <paramref name="row"/> of the loaded group.</summary>
    public RowKey KeyAt(int row)
    {
        ReadOnlySpan<byte> keys = Bytes(_offset, Rows * _keyWidth);
        return SsTableColumnarSegment.TombstoneAt(keys, _keyWidth, row);
    }

    /// <summary>The row of the loaded group that holds <paramref name="key"/>, or -1.</summary>
    public int Find(RowKey key)
    {
        int low = 0;
        int high = Rows - 1;
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            RowKey at = KeyAt(mid);
            if (at == key)
            {
                return mid;
            }

            if (at < key)
            {
                low = mid + 1;
            }
            else
            {
                high = mid - 1;
            }
        }

        return -1;
    }

    /// <summary>A column's values, a whole number of 64-value runs. <typeparamref name="T"/> must be the column's value type.</summary>
    public ReadOnlySpan<T> Values<T>(int slot)
        where T : unmanaged
    {
        int count = Bitmask.WordsFor(Rows) * 64;
        return MemoryMarshal.Cast<byte, T>(Bytes(_segment!.Column(_chunk, slot).ValuesOffset, count * sizeof(T)));
    }

    /// <summary>A BIT column's values, a bit per row.</summary>
    public ReadOnlySpan<ulong> Bits(int slot) =>
        MemoryMarshal.Cast<byte, ulong>(Bytes(_segment!.Column(_chunk, slot).ValuesOffset, Bitmask.WordsFor(Rows) * sizeof(ulong)));

    /// <summary>Whether the loaded group holds a NULL in a column; only then does <see cref="Nulls"/> have a mask.</summary>
    public bool HasNulls(int slot) => _segment!.Column(_chunk, slot).NullsOffset != 0;

    /// <summary>A column's NULL mask, a bit per row.</summary>
    public ReadOnlySpan<ulong> Nulls(int slot) =>
        MemoryMarshal.Cast<byte, ulong>(Bytes(_segment!.Column(_chunk, slot).NullsOffset, Bitmask.WordsFor(Rows) * sizeof(ulong)));

    private ReadOnlySpan<byte> Bytes(long fileOffset, int length)
    {
        int at = checked((int)(fileOffset - _offset));
        return _address != null ? new ReadOnlySpan<byte>(_address + at, length) : _buffer.AsSpan(at, length);
    }
}

/// <summary>The chunk group a <see cref="SsTableColumnarChunkReader"/> has loaded, as a columnar scan reads it.</summary>
internal readonly struct SegmentChunks(SsTableColumnarChunkReader reader) : IColumnarChunks
{
    /// <inheritdoc />
    public bool AnyNull(int slot) => reader.HasNulls(slot);

    /// <inheritdoc />
    public ReadOnlySpan<T> Values<T>(int slot)
        where T : unmanaged => reader.Values<T>(slot);

    /// <inheritdoc />
    public ReadOnlySpan<ulong> Bits(int slot) => reader.Bits(slot);

    /// <inheritdoc />
    public ReadOnlySpan<ulong> Nulls(int slot) => reader.Nulls(slot);
}
