namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A reusable arena that accumulates a reactive delta into four flat <see cref="CellValue"/> buffers
/// (added/removed/updated/updated-before) and yields a borrowed <see cref="QueryChangeRef"/> over them.
/// Buffers are owned by the builder and reused across deltas via <see cref="Reset"/> — growth is
/// one-time and amortized, so steady-state building allocates nothing. Bound to one
/// <see cref="ReactiveRowSchema"/> and used by a single operator invocation at a time.
/// </summary>
/// <remarks>
/// Borrowed buffers must never escape to owned-API consumers; the only legal exit to the owned world
/// is <see cref="QueryChangeRef.Materialize"/>, which copies.
/// </remarks>
internal sealed class QueryChangeBuilder
{
    private readonly ReactiveRowSchema _schema;
    private CellValue[] _added;
    private CellValue[] _removed;
    private CellValue[] _updated;
    private CellValue[] _updatedBefore;
    private int _addedCount;          // counts are in cells (rows * column count)
    private int _removedCount;
    private int _updatedCount;
    private int _updatedBeforeCount;

    /// <summary>Creates an arena for the given schema.</summary>
    public QueryChangeBuilder(ReactiveRowSchema schema, int initialRowCapacity = 4)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(schema);
        _schema = schema;
        int width = Math.Max(1, schema.ColumnCount);
        int capacity = Math.Max(1, initialRowCapacity) * width;
        _added = new CellValue[capacity];
        _removed = new CellValue[capacity];
        _updated = new CellValue[capacity];
        _updatedBefore = new CellValue[capacity];
    }

    /// <summary>The schema all appended rows must match.</summary>
    public ReactiveRowSchema Schema => _schema;

    /// <summary>Appends a row to the added set.</summary>
    public void AddAddedRow(ReadOnlySpan<CellValue> cells) => Append(ref _added, ref _addedCount, cells);

    /// <summary>Appends a row to the removed set.</summary>
    public void AddRemovedRow(ReadOnlySpan<CellValue> cells) => Append(ref _removed, ref _removedCount, cells);

    /// <summary>Appends a row to the updated set.</summary>
    public void AddUpdatedRow(ReadOnlySpan<CellValue> cells) => Append(ref _updated, ref _updatedCount, cells);

    /// <summary>Appends a before-image to the updated-before set (align by index with updated rows).</summary>
    public void AddUpdatedBeforeRow(ReadOnlySpan<CellValue> cells) =>
        Append(ref _updatedBefore, ref _updatedBeforeCount, cells);

    /// <summary>Returns a borrowed delta over the current buffers. Invalid after <see cref="Reset"/>.</summary>
    public QueryChangeRef Build() => new(
        _schema,
        _added.AsSpan(0, _addedCount),
        _removed.AsSpan(0, _removedCount),
        _updated.AsSpan(0, _updatedCount),
        _updatedBefore.AsSpan(0, _updatedBeforeCount));

    /// <summary>Rewinds all sets for reuse, clearing references so the arena holds no stale rows.</summary>
    public void Reset()
    {
        Array.Clear(_added, 0, _addedCount);
        Array.Clear(_removed, 0, _removedCount);
        Array.Clear(_updated, 0, _updatedCount);
        Array.Clear(_updatedBefore, 0, _updatedBeforeCount);
        _addedCount = 0;
        _removedCount = 0;
        _updatedCount = 0;
        _updatedBeforeCount = 0;
    }

    private void Append(ref CellValue[] buffer, ref int count, ReadOnlySpan<CellValue> cells)
    {
        if (cells.Length != _schema.ColumnCount)
        {
            throw new ArgumentException(
                $"Row has {cells.Length} cells but schema has {_schema.ColumnCount} columns.",
                nameof(cells));
        }

        if (count + cells.Length > buffer.Length)
        {
            int next = Math.Max(buffer.Length * 2, count + cells.Length);
            Array.Resize(ref buffer, next); // one-time growth; amortized to zero
        }

        cells.CopyTo(buffer.AsSpan(count));
        count += cells.Length;
    }
}
