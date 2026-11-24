using System.Runtime.CompilerServices;
using DataVo.Core.Memory.Flat;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core.Indexing.Columnar;

/// <summary>
/// Some columns of a table a second time, each as a dense run of values of one type, for scans that read a
/// few columns of every row. Position <c>n</c> holds the same row in every column, with a bit saying whether
/// that row is live and a bit per column saying whether its value is NULL.
/// </summary>
/// <remarks>
/// <para>
/// The storage engine that owns the rows writes the index, one writer at a time: it appends a position for
/// each row it stores and clears the live bit of a row it removes. Rows are never moved or changed in place.
/// </para>
/// <para>
/// Readers take no lock. A position is visible once <see cref="Count"/> covers it, and by then all of its
/// values are in place. Removing a row changes <see cref="Version"/>; a reader that needs a consistent view
/// reads the version first and again when it is done, and starts over if it moved.
/// </para>
/// <para>
/// An LSM table keeps one as its <em>tail</em>: only the rows published since the last flush, the rest being in
/// the columnar segments of its SSTables. A tail also remembers the key of every position and every key that
/// was deleted, because a row it adds or removes may replace one that lives in a file; a scan of the table
/// leaves such rows of the files out.
/// </para>
/// </remarks>
internal sealed class ColumnarIndex
{
    private readonly StorageKind[] _tableKinds;
    private readonly Column[] _columns;
    private readonly int[] _slotOfOrdinal;
    private readonly int[] _located;
    private readonly BitmaskDirectory _live = new();
    private int _count;
    private long _version;
    private long _scans;
    private volatile bool _usable = true;

    // Set for an index over rows that are found by key, not by position: a table of the LSM engine.
    private readonly RowKeyPositionMap? _keys;
    private readonly int _keyOrdinal;
    private RowKey _lastKey;
    private bool _anyKey;
    private int _dead;
    private volatile bool _inKeyOrder = true;
    private volatile bool _rebuildRequested;

    // Set for a tail: the key of every position, and the keys deleted since the tail was made, in the order
    // they came. Appended to by the writer; a reader looks only below the counts it read.
    private readonly bool _tail;
    private readonly ChunkDirectory? _keyHigh;
    private readonly ChunkDirectory? _keyLow;
    private readonly ChunkDirectory? _deletedHigh;
    private readonly ChunkDirectory? _deletedLow;
    private int _deleted;
    private RowKey _smallestKey;
    private RowKey _largestKey;
    private bool _anyTouched;

    /// <summary>One indexed column.</summary>
    private sealed class Column(int ordinal, StorageKind kind)
    {
        public readonly int Ordinal = ordinal;
        public readonly StorageKind Kind = kind;
        public readonly ChunkDirectory? Values = kind == StorageKind.Bit ? null : new ChunkDirectory(WidthOf(kind));
        public readonly BitmaskDirectory? Bits = kind == StorageKind.Bit ? new BitmaskDirectory() : null;
        public readonly BitmaskDirectory Nulls = new();
        public volatile bool AnyNull;
        public volatile bool AnyNaN;
    }

    /// <summary>Creates an empty index.</summary>
    /// <param name="tableKinds">The stored type of every column of the table, in table order.</param>
    /// <param name="ordinals">The table positions of the columns to index, each of a type <see cref="Supports"/> accepts.</param>
    /// <param name="schemaVersion">The table's schema version these positions belong to.</param>
    /// <param name="keyed">
    /// Whether rows are identified by key: they are then added with <see cref="Upsert"/> and removed with
    /// <see cref="Delete"/>, and the index keeps a map from key to position.
    /// </param>
    /// <param name="keyOrdinal">
    /// For a keyed index over a clustered table, the table position of the key column, whose value is the
    /// row's key and not in the row; otherwise -1.
    /// </param>
    /// <param name="tail">
    /// Whether this keyed index is the tail of a table whose other rows are in SSTable segments: it then keeps
    /// the key of every position and of every delete, and never claims to be in key order.
    /// </param>
    public ColumnarIndex(StorageKind[] tableKinds, ReadOnlySpan<int> ordinals, int schemaVersion, bool keyed = false, int keyOrdinal = -1, bool tail = false)
    {
        if (tail && !keyed)
        {
            throw new ArgumentException("Only an index whose rows are found by key can be a tail.", nameof(tail));
        }

        if (tail)
        {
            _tail = true;
            _keyHigh = new ChunkDirectory(sizeof(ulong));
            _keyLow = new ChunkDirectory(sizeof(ulong));
            _deletedHigh = new ChunkDirectory(sizeof(ulong));
            _deletedLow = new ChunkDirectory(sizeof(ulong));
        }

        _keys = keyed ? new RowKeyPositionMap() : null;
        _keyOrdinal = keyOrdinal;
        _tableKinds = tableKinds;
        _located = new int[tableKinds.Length];
        _slotOfOrdinal = new int[tableKinds.Length];
        Array.Fill(_slotOfOrdinal, -1);
        _columns = new Column[ordinals.Length];
        for (int slot = 0; slot < ordinals.Length; slot++)
        {
            int ordinal = ordinals[slot];
            if (!Supports(tableKinds[ordinal]))
            {
                throw new ArgumentException($"A column of type {tableKinds[ordinal]} cannot be in a columnar index.", nameof(ordinals));
            }

            _columns[slot] = new Column(ordinal, tableKinds[ordinal]);
            _slotOfOrdinal[ordinal] = slot;
        }

        SchemaVersion = schemaVersion;
    }

    /// <summary>Whether values of this stored type can be indexed: the fixed-width numeric types, BIT, DATE and DATETIME.</summary>
    /// <remarks>A DATETIME is kept as its UTC ticks and scanned with the 64-bit integer kernels.</remarks>
    public static bool Supports(StorageKind kind) =>
        kind is StorageKind.Int or StorageKind.BigInt or StorageKind.Float or StorageKind.Double or StorageKind.Bit or StorageKind.Date or StorageKind.DateTime;

    // A DATE is kept as its day number.
    private static int WidthOf(StorageKind kind) => ColumnarCells.WidthOf(kind);

    /// <summary>The table schema version the index was built for.</summary>
    public int SchemaVersion { get; }

    /// <summary>The number of positions readers may look at.</summary>
    public int Count => Volatile.Read(ref _count);

    /// <summary>Changes whenever a row is removed; odd while a removal is in progress.</summary>
    public long Version => Volatile.Read(ref _version);

    /// <summary>False once a row arrived that the index could not represent; scans must then read rows.</summary>
    public bool Usable => _usable;

    /// <summary>How many scans this index has answered.</summary>
    public long Scans => Interlocked.Read(ref _scans);

    /// <summary>Records one answered scan.</summary>
    public void CountScan() => Interlocked.Increment(ref _scans);

    /// <summary>
    /// Whether positions are in key order, so a scan visits rows in the order a scan of the table does. True for
    /// an index that is not keyed. A keyed index loses it at the first update or out-of-order insert.
    /// </summary>
    public bool InKeyOrder => !_tail && _inKeyOrder;

    /// <summary>Whether this is the tail of an LSM table: the rows published since its last flush.</summary>
    public bool IsTail => _tail;

    private static long s_identities;

    /// <summary>A number no other index has, so a cache can tell indexes apart without holding on to one.</summary>
    public long Identity { get; } = Interlocked.Increment(ref s_identities);

    /// <summary>The key of the row at a position of a tail, live or not.</summary>
    public RowKey KeyAt(int position) => new(
        _keyHigh!.ChunkOf(position).As<ulong>()[position & FlatChunk.SlotMask],
        _keyLow!.ChunkOf(position).As<ulong>()[position & FlatChunk.SlotMask]);

    /// <summary>How many deletes a tail has seen. Each left its key, whether or not the tail held the row.</summary>
    public int DeletedCount => Volatile.Read(ref _deleted);

    /// <summary>The key of the tail's delete number <paramref name="index"/>, in the order they came.</summary>
    public RowKey DeletedKeyAt(int index) => new(
        _deletedHigh!.ChunkOf(index).As<ulong>()[index & FlatChunk.SlotMask],
        _deletedLow!.ChunkOf(index).As<ulong>()[index & FlatChunk.SlotMask]);

    /// <summary>
    /// The smallest and largest key a tail has added or deleted; false when it has done neither. Read where
    /// the writer cannot run (the engine's publish lock), as the two keys are not written together.
    /// </summary>
    public bool TryGetKeyRange(out RowKey smallest, out RowKey largest)
    {
        smallest = _smallestKey;
        largest = _largestKey;
        return _anyTouched;
    }

    /// <summary>Whether <paramref name="other"/> indexes the same columns in the same slots for the same schema version.</summary>
    public bool SameColumns(ColumnarIndex other)
    {
        if (ReferenceEquals(this, other))
        {
            return true;
        }

        if (SchemaVersion != other.SchemaVersion || _columns.Length != other._columns.Length)
        {
            return false;
        }

        for (int slot = 0; slot < _columns.Length; slot++)
        {
            if (_columns[slot].Ordinal != other._columns[slot].Ordinal)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Asks for the index to be built again by the next query that looks for it.</summary>
    public void RequestRebuild() => _rebuildRequested = true;

    /// <summary>
    /// Records that a FLOAT or DOUBLE column holds a NaN somewhere outside this index: in a file whose rows
    /// the index stands in front of.
    /// </summary>
    public void NoteNaN(int slot) => _columns[slot].AnyNaN = true;

    /// <summary>An empty tail over the same columns, which already knows the NaNs this one knows of.</summary>
    public ColumnarIndex NewTail()
    {
        var ordinals = new int[_columns.Length];
        for (int slot = 0; slot < ordinals.Length; slot++)
        {
            ordinals[slot] = _columns[slot].Ordinal;
        }

        var tail = new ColumnarIndex(_tableKinds, ordinals, SchemaVersion, keyed: true, _keyOrdinal, tail: true);
        for (int slot = 0; slot < ordinals.Length; slot++)
        {
            tail._columns[slot].AnyNaN = _columns[slot].AnyNaN;
        }

        return tail;
    }

    /// <summary>How many positions hold a row that was since deleted or replaced.</summary>
    public int Dead => Volatile.Read(ref _dead);

    /// <summary>Whether dead positions outnumber live ones by enough that building the index again is worth it.</summary>
    public bool WantsRebuild => _rebuildRequested || (Dead > FlatChunk.Slots && Dead > Count / 2);

    /// <summary>The number of indexed columns.</summary>
    public int ColumnCount => _columns.Length;

    /// <summary>The slot of the column at a table position, or -1 when it is not indexed.</summary>
    public int SlotOf(int ordinal) => (uint)ordinal < (uint)_slotOfOrdinal.Length ? _slotOfOrdinal[ordinal] : -1;

    /// <summary>The stored type of an indexed column.</summary>
    public StorageKind KindOf(int slot) => _columns[slot].Kind;

    /// <summary>Whether an indexed column holds a NULL at any position.</summary>
    public bool AnyNull(int slot) => _columns[slot].AnyNull;

    /// <summary>Whether a FLOAT or DOUBLE column holds a NaN at any position.</summary>
    public bool AnyNaN(int slot) => _columns[slot].AnyNaN;

    /// <summary>One chunk of a column's values. <typeparamref name="T"/> must be the column's value type.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<T> Values<T>(int slot, int chunk)
        where T : unmanaged => _columns[slot].Values!.Chunk(chunk).As<T>();

    /// <summary>One chunk of a BIT column's values, a bit per position.</summary>
    public ReadOnlySpan<ulong> Bits(int slot, int chunk) => _columns[slot].Bits!.Words(chunk);

    /// <summary>One chunk of a column's NULL mask.</summary>
    public ReadOnlySpan<ulong> Nulls(int slot, int chunk) => _columns[slot].Nulls.Words(chunk);

    /// <summary>One chunk of the live mask.</summary>
    public ReadOnlySpan<ulong> Live(int chunk) => _live.Words(chunk);

    /// <summary>
    /// Appends a position for one row, given in either stored form. Called by the writer, in row order.
    /// </summary>
    /// <param name="stored">The row's serialized bytes, when it has no typed form.</param>
    /// <param name="typed">The row's cells, when it is typed.</param>
    public void Append(ReadOnlySpan<byte> stored, ReadOnlySpan<CellValue> typed) => AppendRow(stored, typed, default);

    /// <summary>
    /// Adds the row with a key, or replaces the row that has it. Called by the writer of a keyed index.
    /// </summary>
    /// <param name="key">The row's key.</param>
    /// <param name="stored">The row's serialized bytes.</param>
    public void Upsert(RowKey key, ReadOnlySpan<byte> stored)
    {
        if (_keys!.TryGet(key, out int old))
        {
            // The new row and the end of the old one become visible together: a scan that overlaps the change
            // sees the version move and starts over, so it never counts the row twice or not at all.
            Volatile.Write(ref _version, _version + 1);
            int at = AppendRow(stored, null, key);
            _live.Clear(old);
            Volatile.Write(ref _version, _version + 1);
            _keys.Set(key, at);
            Volatile.Write(ref _dead, _dead + 1);
            _inKeyOrder = false;
            return;
        }

        if (_anyKey && key.CompareTo(_lastKey) <= 0)
        {
            _inKeyOrder = false;
        }
        else
        {
            _lastKey = key;
            _anyKey = true;
        }

        _keys.Set(key, AppendRow(stored, null, key));
    }

    /// <summary>
    /// Removes the row with a key, if the index holds one. Called by the writer of a keyed index. A tail notes
    /// the key either way: the row may be one of a file.
    /// </summary>
    public void Delete(RowKey key)
    {
        if (_keys!.Remove(key, out int at))
        {
            Remove(at);
            Volatile.Write(ref _dead, _dead + 1);
        }

        if (_tail)
        {
            int index = _deleted;
            _deletedHigh!.EnsureSlots(index + 1);
            _deletedLow!.EnsureSlots(index + 1);
            _deletedHigh.ChunkOf(index).As<ulong>()[index & FlatChunk.SlotMask] = key.High;
            _deletedLow.ChunkOf(index).As<ulong>()[index & FlatChunk.SlotMask] = key.Low;
            NoteKey(key);
            Volatile.Write(ref _deleted, index + 1);
        }
    }

    // Widens the range of keys a tail has touched.
    private void NoteKey(RowKey key)
    {
        if (!_anyTouched)
        {
            _smallestKey = key;
            _largestKey = key;
            _anyTouched = true;
        }
        else if (key < _smallestKey)
        {
            _smallestKey = key;
        }
        else if (key > _largestKey)
        {
            _largestKey = key;
        }
    }

    private int AppendRow(ReadOnlySpan<byte> stored, ReadOnlySpan<CellValue> typed, RowKey key)
    {
        int at = Reserve();
        if (_tail)
        {
            _keyHigh!.ChunkOf(at).As<ulong>()[at & FlatChunk.SlotMask] = key.High;
            _keyLow!.ChunkOf(at).As<ulong>()[at & FlatChunk.SlotMask] = key.Low;
            NoteKey(key);
        }

        if (!typed.IsEmpty)
        {
            ReadOnlySpan<CellValue> cells = typed;
            foreach (Column column in _columns)
            {
                if (column.Ordinal >= cells.Length || cells[column.Ordinal].IsNull)
                {
                    SetNull(column, at);
                }
                else
                {
                    WriteCell(column, at, in cells[column.Ordinal]);
                }
            }
        }
        else if (RowSerializer.TryLocateCells(stored, _tableKinds, _located))
        {
            foreach (Column column in _columns)
            {
                if (column.Ordinal == _keyOrdinal)
                {
                    WriteKey(column, at, key);
                    continue;
                }

                int flag = _located[column.Ordinal];
                if (flag == RowSerializer.AbsentCell || stored[flag] != 0)
                {
                    SetNull(column, at);
                }
                else
                {
                    WriteBytes(column, at, stored, flag + 1);
                }
            }
        }
        else
        {
            _usable = false;
        }

        _live.Set(at);
        Volatile.Write(ref _count, at + 1);
        return at;
    }

    // A clustered table's key column is the row key itself. Only integer keys can be indexed.
    private void WriteKey(Column column, int at, RowKey key)
    {
        if (ColumnarCells.TryKeyBits(column.Kind, key, out long bits))
        {
            StoreBits(column, at, bits);
        }
        else
        {
            _usable = false;
            SetNull(column, at);
        }
    }

    /// <summary>Appends a position that holds no row, so later positions keep matching their rows. Called by the writer.</summary>
    public void AppendEmpty() => Volatile.Write(ref _count, Reserve() + 1);

    /// <summary>Marks the row at a position removed. Called by the writer.</summary>
    public void Remove(int position)
    {
        if ((uint)position >= (uint)_count || !_live.Test(position))
        {
            return;
        }

        Volatile.Write(ref _version, _version + 1);
        _live.Clear(position);
        Volatile.Write(ref _version, _version + 1);
    }

    private int Reserve()
    {
        int at = _count;
        _live.EnsureSlots(at + 1);
        _keyHigh?.EnsureSlots(at + 1);
        _keyLow?.EnsureSlots(at + 1);
        foreach (Column column in _columns)
        {
            column.Nulls.EnsureSlots(at + 1);
            column.Values?.EnsureSlots(at + 1);
            column.Bits?.EnsureSlots(at + 1);
        }

        return at;
    }

    private static void SetNull(Column column, int at)
    {
        column.AnyNull = true; // before the position is published
        column.Nulls.Set(at);
    }

    private void WriteCell(Column column, int at, in CellValue cell)
    {
        int inChunk = at & FlatChunk.SlotMask;
        switch (column.Kind)
        {
            case StorageKind.Int when cell.Type == CellType.Int32:
                column.Values!.ChunkOf(at).As<int>()[inChunk] = cell.AsInt32();
                break;
            case StorageKind.BigInt when cell.Type == CellType.Int64:
                column.Values!.ChunkOf(at).As<long>()[inChunk] = cell.AsInt64();
                break;
            case StorageKind.Double when cell.Type == CellType.Double:
                column.AnyNaN |= double.IsNaN(cell.AsDouble());
                column.Values!.ChunkOf(at).As<double>()[inChunk] = cell.AsDouble();
                break;
            case StorageKind.Float when cell.Type == CellType.Double:
                column.AnyNaN |= double.IsNaN(cell.AsDouble());
                column.Values!.ChunkOf(at).As<float>()[inChunk] = (float)cell.AsDouble(); // a FLOAT is widened in its cell
                break;
            case StorageKind.DateTime when cell.Type == CellType.DateTime:
                column.Values!.ChunkOf(at).As<long>()[inChunk] = cell.AsUtcTicks();
                break;
            case StorageKind.Date when cell.Type == CellType.Date:
                column.Values!.ChunkOf(at).As<int>()[inChunk] = cell.AsDate().DayNumber;
                break;
            case StorageKind.Bit when cell.Type == CellType.Boolean:
                if (cell.AsBoolean())
                {
                    column.Bits!.Set(at);
                }

                break;
            default:
                // A cell of another type would need the row path's conversions; leave such tables to it.
                _usable = false;
                SetNull(column, at);
                break;
        }
    }

    private static void WriteBytes(Column column, int at, ReadOnlySpan<byte> stored, int offset) =>
        StoreBits(column, at, ColumnarCells.ReadBits(column.Kind, stored, offset));

    // Stores a value in the form ColumnarCells reads it from a row: a FLOAT's bits go into its 4-byte slot as they are.
    private static void StoreBits(Column column, int at, long bits)
    {
        int inChunk = at & FlatChunk.SlotMask;
        if (column.Kind == StorageKind.Bit)
        {
            if (bits != 0)
            {
                column.Bits!.Set(at);
            }
        }
        else if (ColumnarCells.WidthOf(column.Kind) == sizeof(long))
        {
            column.AnyNaN |= ColumnarCells.IsNaN(column.Kind, bits);
            column.Values!.ChunkOf(at).As<long>()[inChunk] = bits;
        }
        else
        {
            column.AnyNaN |= ColumnarCells.IsNaN(column.Kind, bits);
            column.Values!.ChunkOf(at).As<int>()[inChunk] = (int)bits;
        }
    }
}

/// <summary>A storage engine that keeps a <see cref="ColumnarIndex"/> beside a table's rows and writes it as rows change.</summary>
internal interface IColumnarRowStore
{
    /// <summary>
    /// Whether the engine finds rows by key, and so can index a clustered table, whose key is not in its rows.
    /// </summary>
    bool KeyedRows { get; }

    /// <summary>The table's columnar index, or null when it has none. Takes no lock.</summary>
    ColumnarIndex? Columnar(string databaseName, string tableName);

    /// <summary>
    /// Replaces the table's columnar index with one built from the rows it holds now, and keeps it current from
    /// then on. Returns null when the engine's rows cannot be indexed this way.
    /// </summary>
    /// <param name="keyOrdinal">The table position of a clustered table's key column, or -1.</param>
    ColumnarIndex? BuildColumnar(string databaseName, string tableName, StorageKind[] tableKinds, int[] ordinals, int schemaVersion, int keyOrdinal);

    /// <summary>Stops keeping a columnar index for the table.</summary>
    void DropColumnar(string databaseName, string tableName);

    /// <summary>
    /// Counts the rows of the table a bound condition accepts and computes each aggregate over them, as
    /// <see cref="ColumnarScan.TryRun"/> does over one index. The engine decides where the columns are read
    /// from: the index alone, or the index together with columnar data it keeps on disk.
    /// </summary>
    /// <param name="index">The table's index, as <see cref="Columnar"/> returned it; the steps and folds are bound to it.</param>
    /// <returns><see langword="false"/> when no consistent result was reached; the caller then reads rows.</returns>
    bool TryScan(
        string databaseName,
        string tableName,
        ColumnarIndex index,
        ReadOnlySpan<ColumnarStep> steps,
        ReadOnlySpan<ColumnarFold> folds,
        Span<RowAggregate> totals,
        int limit,
        out int matched);
}
