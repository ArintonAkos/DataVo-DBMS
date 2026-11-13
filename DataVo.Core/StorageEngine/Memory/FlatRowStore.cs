using System.Collections.Concurrent;
using DataVo.Core.Memory.Flat;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// One row of a <see cref="FlatRowStore"/>, borrowed for reading. A packed row is its record and its run of a
/// reference array, to be read with <see cref="FlatRowCodec"/>; a row kept apart is its cells.
/// </summary>
internal readonly ref struct FlatRow
{
    /// <summary>A packed row.</summary>
    public FlatRow(ReadOnlySpan<byte> record, object?[] references, int referenceBase)
    {
        Record = record;
        References = references;
        ReferenceBase = referenceBase;
        Cells = default;
        IsKeptApart = false;
    }

    /// <summary>A row kept apart from its record.</summary>
    public FlatRow(ReadOnlySpan<CellValue> cells)
    {
        Record = default;
        References = [];
        ReferenceBase = 0;
        Cells = cells;
        IsKeptApart = true;
    }

    /// <summary>Whether the row is held as cells rather than packed; then only <see cref="Cells"/> is set.</summary>
    public bool IsKeptApart { get; }

    /// <summary>The packed row's record.</summary>
    public ReadOnlySpan<byte> Record { get; }

    /// <summary>The array holding the packed row's references.</summary>
    public object?[] References { get; }

    /// <summary>The index in <see cref="References"/> of the packed row's first reference.</summary>
    public int ReferenceBase { get; }

    /// <summary>The cells of a row kept apart.</summary>
    public ReadOnlySpan<CellValue> Cells { get; }
}

/// <summary>
/// A table's rows in memory, packed: each row is one fixed-width record in a flat chunk, next to the rows
/// before and after it, plus one object reference per string or vector column. Rows are numbered from 0 in
/// the order they were added and keep their number for good.
/// </summary>
/// <remarks>
/// One writer at a time adds and removes rows; the table's write lock sees to that. Readers take no lock. A row
/// can be read once <see cref="Add"/> has returned its number. A row is never changed in place: it is added,
/// and later it may be removed, which leaves its number unused.
/// <para>
/// <b>Reading while rows are removed.</b> Removing a row marks its record empty and then lets go of its strings
/// and vectors. A reader that copies values out of a <see cref="FlatRow"/> may therefore find a reference
/// already gone; it must call <see cref="IsLive"/> afterwards and discard what it copied when the row is no
/// longer there. <see cref="TryDecode"/> does this itself.
/// </para>
/// <para>
/// This holds on weakly ordered processors because of how each side touches memory. The writer publishes the
/// empty mark and then each cleared reference as volatile writes, in that order. A reader must take references
/// through <see cref="FlatRowCodec.ReadString"/> and <see cref="FlatRowCodec.ReadVector"/>, which are volatile
/// reads, and <see cref="IsLive"/> is one too: a reader that saw a cleared reference therefore also sees the
/// empty mark when it checks afterwards. The fixed-width bytes of a record are never rewritten once the row is
/// published, so reading them needs no ordering at all. A reader that took a reference with a plain load
/// instead would need <see cref="Interlocked.MemoryBarrier"/> before its check.
/// </para>
/// <para>
/// <b>Rows kept apart.</b> A row with a cell that is not of its column's type (a BIGINT cell in an INT column,
/// say), or without exactly one cell per column, cannot be packed. Its cells are kept as they are in a side map keyed by row number, and its record
/// only says so. Ordinary rows pay nothing for this. The map entry lives exactly as long as the row: it is
/// dropped by <see cref="Remove"/>, copied by <see cref="Clone"/>, and gone with the store when the table is
/// cleared, dropped or rebuilt for another layout.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// var store = new FlatRowStore(RowLayout.Create([CellType.Int32, CellType.String]));
/// int row = store.Add([CellValue.From(7), CellValue.From("ada")]);
///
/// Span&lt;CellValue&gt; cells = stackalloc CellValue[2];
/// bool found = store.TryDecode(row, cells);   // true; cells are 7 and "ada"
/// store.Remove(row);
/// found = store.TryDecode(row, cells);        // false
/// </code>
/// </example>
internal sealed class FlatRowStore
{
    // A table of a few rows starts with this many slots rather than a whole chunk.
    private const int FirstChunkRows = 16;

    private readonly FlatSlotList _records;
    private readonly ReferenceDirectory? _references;
    private volatile ConcurrentDictionary<int, CellValue[]>? _keptApart;

    /// <summary>Creates an empty store.</summary>
    /// <param name="layout">The shape of the table's rows.</param>
    public FlatRowStore(RowLayout layout)
    {
        Layout = layout ?? throw new ArgumentNullException(nameof(layout));
        _records = new FlatSlotList(layout.RecordBytes, FirstChunkRows);
        _references = layout.ReferenceCount > 0 ? new ReferenceDirectory(layout.ReferenceCount, FirstChunkRows) : null;
    }

    /// <summary>The shape of the rows.</summary>
    public RowLayout Layout { get; }

    /// <summary>The number of row numbers given out, removed rows and empty positions included.</summary>
    public int Count => _records.Count;

    /// <summary>The number of rows kept apart because they could not be packed.</summary>
    public int KeptApartCount => _keptApart?.Count ?? 0;

    /// <summary>Adds a row. Called by the writer.</summary>
    /// <param name="cells">
    /// The row's cells. They are copied; the caller keeps its buffer. A row that does not have one cell per
    /// column is kept apart, like a row with a cell of another type.
    /// </param>
    /// <returns>The row's number.</returns>
    public int Add(ReadOnlySpan<CellValue> cells)
    {
        int index = _records.Count;
        Span<byte> record = _records.Next();
        object?[] references = ReserveReferences(index, out int referenceBase);
        if (cells.Length == Layout.ColumnCount && FlatRowCodec.TryEncode(Layout, cells, record, references, referenceBase))
        {
            Layout.MarkLive(record);
        }
        else
        {
            KeepApart(index, cells);
            Layout.MarkKeptApart(record);
        }

        return _records.Publish();
    }

    /// <summary>
    /// Uses up a row number without adding a row, so the rows after it keep the numbers they had elsewhere.
    /// Called by the writer.
    /// </summary>
    /// <returns>The unused number.</returns>
    public int AddEmpty()
    {
        int index = _records.Count;
        _records.Next();
        ReserveReferences(index, out _);
        return _records.Publish();
    }

    /// <summary>Removes a row and lets go of its strings and vectors. Called by the writer.</summary>
    /// <returns><see langword="false"/> when there is no row with that number.</returns>
    public bool Remove(int index)
    {
        if ((uint)index >= (uint)_records.Count)
        {
            return false;
        }

        Span<byte> record = _records.Edit(index);
        if (!Layout.IsLive(record))
        {
            return false;
        }

        bool keptApart = Layout.IsKeptApart(record);
        Layout.MarkEmpty(record); // before the references go, so a reader that misses one also sees the row gone
        if (keptApart)
        {
            _keptApart!.TryRemove(index, out _);
        }
        else if (_references is { } references)
        {
            FlatRowCodec.ClearReferences(Layout, references.ChunkOf(index), references.BaseOf(index));
        }

        return true;
    }

    /// <summary>Whether there is a row with a number, as of now.</summary>
    public bool IsLive(int index) =>
        _records.TryRead(index, out ReadOnlySpan<byte> record) && Layout.IsStillLive(record);

    /// <summary>Borrows a row without a lock; see the remarks on reading while rows are removed.</summary>
    /// <returns><see langword="false"/> when there is no row with that number.</returns>
    public bool TryRead(int index, out FlatRow row)
    {
        if (!_records.TryRead(index, out ReadOnlySpan<byte> record) || !Layout.IsLive(record))
        {
            row = default;
            return false;
        }

        if (Layout.IsKeptApart(record))
        {
            return TryReadKeptApart(index, out row);
        }

        row = _references is { } references
            ? new FlatRow(record, references.ChunkOf(index), references.BaseOf(index))
            : new FlatRow(record, [], 0);
        return true;
    }

    /// <summary>Copies a row out as cells without a lock. The result is the whole row or nothing.</summary>
    /// <param name="index">The row's number.</param>
    /// <param name="cells">Receives the cells; at least as long as the column count.</param>
    /// <returns><see langword="false"/> when there is no row with that number, or it was removed meanwhile.</returns>
    public bool TryDecode(int index, Span<CellValue> cells)
    {
        if (!TryRead(index, out FlatRow row))
        {
            return false;
        }

        if (row.IsKeptApart)
        {
            row.Cells.CopyTo(cells);
            return true;
        }

        FlatRowCodec.Decode(Layout, row.Record, row.References, row.ReferenceBase, cells);
        return IsLive(index);
    }

    /// <summary>
    /// Copies the store: the same rows under the same numbers, rows kept apart included. Later changes to
    /// either store do not show in the other. Called with no writer at work.
    /// </summary>
    public FlatRowStore Clone()
    {
        var copy = new FlatRowStore(Layout);
        int count = Count;
        for (int index = 0; index < count; index++)
        {
            if (TryRead(index, out FlatRow row))
            {
                copy.AddCopy(row);
            }
            else
            {
                copy.AddEmpty();
            }
        }

        return copy;
    }

    /// <summary>Adds a row borrowed from a store of the same layout.</summary>
    private void AddCopy(FlatRow row)
    {
        if (row.IsKeptApart)
        {
            Add(row.Cells);
            return;
        }

        int index = _records.Count;
        row.Record.CopyTo(_records.Next());
        object?[] references = ReserveReferences(index, out int referenceBase);
        Array.Copy(row.References, row.ReferenceBase, references, referenceBase, Layout.ReferenceCount);
        _records.Publish();
    }

    /// <summary>Makes room for a row's references and says where they go.</summary>
    private object?[] ReserveReferences(int index, out int referenceBase)
    {
        if (_references is not { } references)
        {
            referenceBase = 0;
            return [];
        }

        references.EnsureSlots(index + 1);
        referenceBase = references.BaseOf(index);
        return references.ChunkOf(index);
    }

    /// <summary>Keeps a copy of a row's cells in the side map, before the row's number is given out.</summary>
    private void KeepApart(int index, ReadOnlySpan<CellValue> cells)
    {
        ConcurrentDictionary<int, CellValue[]> map = _keptApart ??= new ConcurrentDictionary<int, CellValue[]>();
        map[index] = cells.ToArray();
    }

    private bool TryReadKeptApart(int index, out FlatRow row)
    {
        // The entry is missing only when the row was removed after its record was looked at.
        if (_keptApart is { } map && map.TryGetValue(index, out CellValue[]? cells))
        {
            row = new FlatRow(cells);
            return true;
        }

        row = default;
        return false;
    }
}
