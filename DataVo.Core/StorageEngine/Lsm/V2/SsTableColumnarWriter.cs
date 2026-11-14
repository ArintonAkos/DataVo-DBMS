using System.Buffers;
using System.Buffers.Binary;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Memory.Flat;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Transactions;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Builds the columnar segment of a version 8 SSTable (<see cref="SsTableColumnarFormat"/>) while the file's
/// rows are written: it is given the newest version of every row key, in key order, and keeps the indexed
/// columns of the puts and the keys of the deletions.
/// </summary>
/// <remarks>
/// Finished chunk groups and deleted keys are held until <see cref="WriteTo"/> writes them after the file's last
/// data block: in pooled memory, or with <see cref="SsTableSpillOptions"/> in temporary files once they outgrow
/// its limit, so a compaction of any size keeps only one group in memory.
/// </remarks>
internal sealed class SsTableColumnarWriter : IDisposable
{
    private const int Rows = SsTableColumnarFormat.ChunkRows;

    private readonly ColumnarSpec _spec;
    private readonly int[] _located;
    private readonly ulong[] _keyHigh = new ulong[Rows];
    private readonly ulong[] _keyLow = new ulong[Rows];
    private readonly ColumnBuffer[] _columns;
    private readonly SsTableSpillBuffer _groups;
    private readonly List<SsTableColumnarChunk> _chunks = [];
    private readonly List<SsTableColumnarColumn> _chunkColumns = [];
    private readonly SsTableSpillBuffer _tombstones; // 16 B per deleted key: high, then low, little-endian
    private long _tombstoneCount;
    private RowKey _lastTombstone;
    private int _rows;
    private long _rowCount;
    private long _length; // bytes of the finished groups
    private bool _unusable;

    /// <summary>One column of the group being filled.</summary>
    private sealed class ColumnBuffer(StorageKind kind)
    {
        public readonly StorageKind Kind = kind;
        public readonly byte[]? Values = kind == StorageKind.Bit ? null : new byte[Rows * ColumnarCells.WidthOf(kind)];
        public readonly ulong[]? Bits = kind == StorageKind.Bit ? new ulong[Bitmask.WordsPerChunk] : null;
        public readonly ulong[] Nulls = new ulong[Bitmask.WordsPerChunk];
        public int NullCount;
        public int Ordered; // values that are neither NULL nor NaN
        public bool AnyNaN;
        public long Min;
        public long Max;

        public void Reset()
        {
            Values?.AsSpan().Clear();
            Bits?.AsSpan().Clear();
            Nulls.AsSpan().Clear();
            NullCount = 0;
            Ordered = 0;
            AnyNaN = false;
            Min = 0;
            Max = 0;
        }
    }

    /// <summary>Creates a writer for the columns <paramref name="spec"/> names.</summary>
    /// <param name="spec">The columns to keep.</param>
    /// <param name="spill">Where to keep groups and deleted keys that outgrow memory; null keeps them in memory.</param>
    public SsTableColumnarWriter(ColumnarSpec spec, SsTableSpillOptions? spill = null)
    {
        _spec = spec;
        _groups = new SsTableSpillBuffer(spill);
        _tombstones = new SsTableSpillBuffer(spill);
        _located = new int[spec.TableKinds.Length];
        _columns = new ColumnBuffer[spec.ColumnCount];
        for (int slot = 0; slot < _columns.Length; slot++)
        {
            _columns[slot] = new ColumnBuffer(spec.KindOf(slot));
        }
    }

    /// <summary>
    /// Takes the newest version of a row key. Keys must arrive ascending, each once.
    /// </summary>
    /// <param name="key">The row's key.</param>
    /// <param name="type">Whether the version is a put or a deletion.</param>
    /// <param name="value">The stored row of a put.</param>
    public void Add(RowKey key, LsmValueType type, ReadOnlySpan<byte> value)
    {
        if (type == LsmValueType.Deletion)
        {
            Span<byte> bytes = stackalloc byte[16];
            BinaryPrimitives.WriteUInt64LittleEndian(bytes, key.High);
            BinaryPrimitives.WriteUInt64LittleEndian(bytes[8..], key.Low);
            _tombstones.Write(bytes);
            _tombstoneCount++;
            _lastTombstone = key;
            return;
        }

        int at = _rows;
        _keyHigh[at] = key.High;
        _keyLow[at] = key.Low;
        bool located = RowSerializer.TryLocateCells(value, _spec.TableKinds, _located);
        _unusable |= !located;
        for (int slot = 0; slot < _columns.Length; slot++)
        {
            AddCell(_columns[slot], _spec.Ordinals[slot], at, key, value, located);
        }

        _rowCount++;
        if (++_rows == Rows)
        {
            CloseGroup();
        }
    }

    private void AddCell(ColumnBuffer column, int ordinal, int at, RowKey key, ReadOnlySpan<byte> value, bool located)
    {
        long bits;
        if (ordinal == _spec.KeyOrdinal)
        {
            // A clustered table's key column is the row key itself. Only integer keys can be indexed.
            if (!ColumnarCells.TryKeyBits(column.Kind, key, out bits))
            {
                _unusable = true;
                SetNull(column, at);
                return;
            }
        }
        else
        {
            int flag = located ? _located[ordinal] : RowSerializer.AbsentCell;
            if (flag == RowSerializer.AbsentCell || value[flag] != 0)
            {
                SetNull(column, at);
                return;
            }

            bits = ColumnarCells.ReadBits(column.Kind, value, flag + 1);
        }

        if (column.Kind == StorageKind.Bit)
        {
            if (bits != 0)
            {
                Bitmask.Set(column.Bits!, at);
            }
        }
        else
        {
            ColumnarCells.Store(column.Kind, column.Values!, at, bits);
        }

        Track(column, bits);
    }

    private static void SetNull(ColumnBuffer column, int at)
    {
        Bitmask.Set(column.Nulls, at);
        column.NullCount++;
    }

    // Keeps the group's smallest and largest value. A NaN has no place in an order and is only flagged.
    private static void Track(ColumnBuffer column, long bits)
    {
        if (ColumnarCells.IsNaN(column.Kind, bits))
        {
            column.AnyNaN = true;
            return;
        }

        long zone = ColumnarCells.ZoneBits(column.Kind, bits);
        if (column.Ordered++ == 0)
        {
            column.Min = zone;
            column.Max = zone;
        }
        else if (ColumnarCells.CompareZone(column.Kind, zone, column.Min) < 0)
        {
            column.Min = zone;
        }
        else if (ColumnarCells.CompareZone(column.Kind, zone, column.Max) > 0)
        {
            column.Max = zone;
        }
    }

    /// <summary>
    /// Writes the segment at <paramref name="position"/> of <paramref name="output"/>, which must be a
    /// multiple of <see cref="SsTableColumnarFormat.Alignment"/>.
    /// </summary>
    /// <returns>Where the directory is, and the position after it.</returns>
    public (long DirectoryOffset, int DirectoryLength, long End) WriteTo(Stream output, long position)
    {
        if (position % SsTableColumnarFormat.Alignment != 0)
        {
            throw new ArgumentException("A columnar segment must start at a multiple of its alignment.", nameof(position));
        }

        if (_rows > 0)
        {
            CloseGroup();
        }

        long start = position;
        _groups.CopyTo(output);
        position += _groups.Length;

        long tombstoneOffset = position;
        ushort tombstoneWidth = WriteTombstones(output, ref position, out uint tombstoneCrc);

        int specLength = _spec.EncodedLength;
        int directoryLength = checked((int)SsTableColumnarFormat.DirectoryLength(specLength, _chunks.Count, _columns.Length));
        byte[] directory = ArrayPool<byte>.Shared.Rent(directoryLength);
        try
        {
            Span<byte> span = directory.AsSpan(0, directoryLength);
            span.Clear();
            new SsTableColumnarHeader(
                tombstoneWidth,
                _unusable ? SsTableColumnarFormat.UnusableFlag : 0,
                _spec.Hash,
                _columns.Length,
                _chunks.Count,
                _rowCount,
                tombstoneOffset,
                _tombstoneCount,
                specLength,
                tombstoneCrc).WriteTo(span);
            _spec.WriteTo(span.Slice(SsTableColumnarFormat.HeaderSize, specLength));
            WriteRecords(span[(SsTableColumnarFormat.HeaderSize + SsTableColumnarFormat.PaddedSpecLength(specLength))..], start);
            BinaryPrimitives.WriteUInt32LittleEndian(span[^sizeof(uint)..], WalCrc32C.HashToUInt32(span[..^sizeof(uint)]));
            output.Write(span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(directory);
        }

        return (position, directoryLength, position + directoryLength);
    }

    /// <summary>Returns the pooled buffers and deletes any temporary file.</summary>
    public void Dispose()
    {
        _groups.Dispose();
        _tombstones.Dispose();
    }

    // Chunk and column records were collected with offsets from the segment's start; the file wants positions.
    private void WriteRecords(Span<byte> records, long start)
    {
        int at = 0;
        foreach (SsTableColumnarChunk chunk in _chunks)
        {
            (chunk with { Offset = chunk.Offset + start }).WriteTo(records[at..]);
            at += SsTableColumnarFormat.ChunkRecordSize;
        }

        foreach (SsTableColumnarColumn column in _chunkColumns)
        {
            (column with
            {
                ValuesOffset = column.ValuesOffset + start,
                NullsOffset = column.NullsOffset == 0 ? 0 : column.NullsOffset + start,
            }).WriteTo(records[at..]);
            at += SsTableColumnarFormat.ColumnRecordSize;
        }
    }

    // Writes the deleted keys, padded so the directory starts 8-byte aligned, and returns their width.
    private ushort WriteTombstones(Stream output, ref long position, out uint crc)
    {
        bool wide = _tombstoneCount > 0 && !_lastTombstone.IsInt64; // keys ascend: integer keys come first
        int width = wide ? 16 : 8;
        long length = _tombstoneCount * width;
        long padded = (length + 7) & ~7L;
        const int Batch = 4096;
        byte[] stored = ArrayPool<byte>.Shared.Rent(Batch * 16);
        byte[] written = ArrayPool<byte>.Shared.Rent(Batch * 16);
        try
        {
            // The CRC covers the keys only, not the padding; it is computed over the keys as they are written.
            uint state = WalCrc32C.Seed;
            _tombstones.Rewind();
            int read;
            while ((read = _tombstones.Read(stored.AsSpan(0, Batch * 16))) > 0)
            {
                int keys = read / 16;
                Span<byte> span = written.AsSpan(0, keys * width);
                for (int i = 0; i < keys; i++)
                {
                    var key = new RowKey(BinaryPrimitives.ReadUInt64LittleEndian(stored.AsSpan(i * 16)), BinaryPrimitives.ReadUInt64LittleEndian(stored.AsSpan((i * 16) + 8)));
                    WriteKey(span[(i * width)..], key, wide);
                }

                state = WalCrc32C.Append(state, span);
                output.Write(span);
            }

            crc = ~state;
            Span<byte> zeros = stackalloc byte[8];
            zeros.Clear();
            output.Write(zeros[..(int)(padded - length)]);
            position += padded;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(stored);
            ArrayPool<byte>.Shared.Return(written);
        }

        return (ushort)width;
    }

    private static void WriteKey(Span<byte> destination, RowKey key, bool wide)
    {
        if (wide)
        {
            SsTableV2Format.WriteKey(destination, key);
        }
        else
        {
            BinaryPrimitives.WriteUInt64LittleEndian(destination, key.Low);
        }
    }

    // Lays the filled group out in one pooled buffer and records where its parts are.
    private void CloseGroup()
    {
        int rows = _rows;
        int words = Bitmask.WordsFor(rows);
        bool wide = _keyHigh[rows - 1] != 0; // keys ascend: a wide key, if any, is last
        int keyWidth = wide ? 16 : 8;
        int maskLength = SsTableColumnarFormat.AlignUp(words * sizeof(ulong));

        int length = SsTableColumnarFormat.AlignUp(rows * keyWidth);
        foreach (ColumnBuffer column in _columns)
        {
            length += column.Values is null ? maskLength : SsTableColumnarFormat.AlignUp(words * 64 * ColumnarCells.WidthOf(column.Kind));
            length += column.NullCount > 0 ? maskLength : 0;
        }

        byte[] buffer = ArrayPool<byte>.Shared.Rent(length);
        Span<byte> group = buffer.AsSpan(0, length);
        group.Clear();
        for (int i = 0; i < rows; i++)
        {
            WriteKey(group[(i * keyWidth)..], new RowKey(_keyHigh[i], _keyLow[i]), wide);
        }

        int at = SsTableColumnarFormat.AlignUp(rows * keyWidth);
        foreach (ColumnBuffer column in _columns)
        {
            long valuesOffset = _length + at;
            if (column.Values is null)
            {
                WriteWords(group[at..], column.Bits.AsSpan(0, words));
                at += maskLength;
            }
            else
            {
                int valueBytes = rows * ColumnarCells.WidthOf(column.Kind);
                column.Values.AsSpan(0, valueBytes).CopyTo(group[at..]);
                at += SsTableColumnarFormat.AlignUp(words * 64 * ColumnarCells.WidthOf(column.Kind));
            }

            long nullsOffset = 0;
            if (column.NullCount > 0)
            {
                nullsOffset = _length + at;
                WriteWords(group[at..], column.Nulls.AsSpan(0, words));
                at += maskLength;
            }

            byte flags = (byte)((column.NullCount > 0 ? SsTableColumnarFormat.AnyNull : 0)
                | (column.NullCount == rows ? SsTableColumnarFormat.AllNull : 0)
                | (column.AnyNaN ? SsTableColumnarFormat.AnyNaN : 0));
            _chunkColumns.Add(new SsTableColumnarColumn(valuesOffset, nullsOffset, column.Min, column.Max, flags));
            column.Reset();
        }

        _chunks.Add(new SsTableColumnarChunk(
            new RowKey(_keyHigh[0], _keyLow[0]),
            new RowKey(_keyHigh[rows - 1], _keyLow[rows - 1]),
            rows,
            length,
            _length,
            WalCrc32C.HashToUInt32(group),
            keyWidth));
        _groups.Write(group);
        ArrayPool<byte>.Shared.Return(buffer);
        _length += length;
        _rows = 0;
    }

    private static void WriteWords(Span<byte> destination, ReadOnlySpan<ulong> words)
    {
        for (int i = 0; i < words.Length; i++)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(destination[(i * sizeof(ulong))..], words[i]);
        }
    }
}
