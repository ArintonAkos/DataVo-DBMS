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
    public ColumnarIndex(StorageKind[] tableKinds, ReadOnlySpan<int> ordinals, int schemaVersion, bool keyed = false, int keyOrdinal = -1)
    {
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

    /// <summary>Whether values of this stored type can be indexed: the fixed-width numeric types, BIT and DATE.</summary>
    public static bool Supports(StorageKind kind) =>
        kind is StorageKind.Int or StorageKind.BigInt or StorageKind.Float or StorageKind.Double or StorageKind.Bit or StorageKind.Date;

    // A DATE is kept as its day number.
    private static int WidthOf(StorageKind kind) => kind is StorageKind.BigInt or StorageKind.Double ? sizeof(long) : sizeof(int);

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
    public bool InKeyOrder => _inKeyOrder;

    /// <summary>How many positions hold a row that was since deleted or replaced.</summary>
    public int Dead => Volatile.Read(ref _dead);

    /// <summary>Whether dead positions outnumber live ones by enough that building the index again is worth it.</summary>
    public bool WantsRebuild => Dead > FlatChunk.Slots && Dead > Count / 2;

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
    public void Append(ReadOnlySpan<byte> stored, StoredRow? typed) => AppendRow(stored, typed, default);

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

    /// <summary>Removes the row with a key, if the index holds one. Called by the writer of a keyed index.</summary>
    public void Delete(RowKey key)
    {
        if (_keys!.Remove(key, out int at))
        {
            Remove(at);
            Volatile.Write(ref _dead, _dead + 1);
        }
    }

    private int AppendRow(ReadOnlySpan<byte> stored, StoredRow? typed, RowKey key)
    {
        int at = Reserve();
        if (typed is not null)
        {
            ReadOnlySpan<CellValue> cells = typed.Cells;
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
        if (!key.IsInt64)
        {
            _usable = false;
            SetNull(column, at);
        }
        else if (column.Kind == StorageKind.BigInt)
        {
            column.Values!.ChunkOf(at).As<long>()[at & FlatChunk.SlotMask] = (long)key;
        }
        else
        {
            column.Values!.ChunkOf(at).As<int>()[at & FlatChunk.SlotMask] = (int)(long)key;
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

    private static void WriteBytes(Column column, int at, ReadOnlySpan<byte> stored, int offset)
    {
        int inChunk = at & FlatChunk.SlotMask;
        switch (column.Kind)
        {
            case StorageKind.Int:
                column.Values!.ChunkOf(at).As<int>()[inChunk] = RowBytes.Int32(stored, offset);
                break;
            case StorageKind.BigInt:
                column.Values!.ChunkOf(at).As<long>()[inChunk] = RowBytes.Int64(stored, offset);
                break;
            case StorageKind.Double:
            {
                double value = RowBytes.Double(stored, offset);
                column.AnyNaN |= double.IsNaN(value);
                column.Values!.ChunkOf(at).As<double>()[inChunk] = value;
                break;
            }

            case StorageKind.Float:
            {
                float value = RowBytes.Single(stored, offset);
                column.AnyNaN |= float.IsNaN(value);
                column.Values!.ChunkOf(at).As<float>()[inChunk] = value;
                break;
            }
            case StorageKind.Date:
                column.Values!.ChunkOf(at).As<int>()[inChunk] = RowBytes.DayNumber(stored, offset);
                break;
            case StorageKind.Bit:
                if (RowBytes.Boolean(stored, offset))
                {
                    column.Bits!.Set(at);
                }

                break;
        }
    }
}

/// <summary>A storage engine that keeps a <see cref="ColumnarIndex"/> beside a table's rows and writes it as rows change.</summary>
internal interface IColumnarRowStore
{
    /// <summary>The table's columnar index, or null when it has none. Takes no lock.</summary>
    ColumnarIndex? Columnar(string databaseName, string tableName);

    /// <summary>
    /// Replaces the table's columnar index with one built from the rows it holds now, and keeps it current from
    /// then on. Returns null when the engine's rows cannot be indexed this way.
    /// </summary>
    ColumnarIndex? BuildColumnar(string databaseName, string tableName, StorageKind[] tableKinds, int[] ordinals, int schemaVersion);

    /// <summary>Stops keeping a columnar index for the table.</summary>
    void DropColumnar(string databaseName, string tableName);
}
