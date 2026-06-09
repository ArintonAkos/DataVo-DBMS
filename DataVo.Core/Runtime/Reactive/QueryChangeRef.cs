namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A borrowed view over a set of fixed-width rows packed into a single flat <see cref="CellValue"/>
/// span (row <c>i</c> = cells <c>[i*width, (i+1)*width)</c>, where <c>width</c> is the schema's
/// column count). A <c>ref struct</c>: valid only while the backing buffer is owned.
/// </summary>
public readonly ref struct RowSet
{
    private readonly ReactiveRowSchema _schema;
    private readonly ReadOnlySpan<CellValue> _cells;

    /// <summary>Creates a borrowed row set over a flat cell span.</summary>
    public RowSet(ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells)
    {
        _schema = schema;
        _cells = cells;
    }

    /// <summary>The number of rows.</summary>
    public int Count => _schema.ColumnCount == 0 ? 0 : _cells.Length / _schema.ColumnCount;

    /// <summary>The row at <paramref name="index"/>.</summary>
    public RowRef this[int index]
    {
        get
        {
            if ((uint)index >= (uint)Count)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            int width = _schema.ColumnCount;
            return new RowRef(_schema, _cells.Slice(index * width, width));
        }
    }
}

/// <summary>
/// A borrowed view over one reactive delta: added/removed/updated row sets (plus update before-images),
/// mirroring the shape of the owned <see cref="QueryChange"/>. A <c>ref struct</c> whose rows are valid
/// only during the synchronous callback/operator frame that owns the backing buffers. Retain via
/// <see cref="Materialize"/>.
/// </summary>
public readonly ref struct QueryChangeRef
{
    private readonly ReactiveRowSchema _schema;
    private readonly ReadOnlySpan<CellValue> _added;
    private readonly ReadOnlySpan<CellValue> _removed;
    private readonly ReadOnlySpan<CellValue> _updated;
    private readonly ReadOnlySpan<CellValue> _updatedBefore;

    /// <summary>Creates a borrowed delta over flat per-set cell spans.</summary>
    public QueryChangeRef(
        ReactiveRowSchema schema,
        ReadOnlySpan<CellValue> added,
        ReadOnlySpan<CellValue> removed,
        ReadOnlySpan<CellValue> updated,
        ReadOnlySpan<CellValue> updatedBefore)
    {
        ArgumentNullException.ThrowIfNull(schema);
        _schema = schema;
        _added = added;
        _removed = removed;
        _updated = updated;
        _updatedBefore = updatedBefore;
    }

    /// <summary>The shared row schema.</summary>
    public ReactiveRowSchema Schema => _schema;

    /// <summary>Rows that newly entered the result set.</summary>
    public RowSet Added => new(_schema, _added);

    /// <summary>Rows that left the result set.</summary>
    public RowSet Removed => new(_schema, _removed);

    /// <summary>Rows that stayed but changed value.</summary>
    public RowSet Updated => new(_schema, _updated);

    /// <summary>Before-images aligned by index with <see cref="Updated"/>.</summary>
    public RowSet UpdatedBefore => new(_schema, _updatedBefore);

    /// <summary>Whether this delta carries no added, removed, or updated rows.</summary>
    public bool IsEmpty => _added.IsEmpty && _removed.IsEmpty && _updated.IsEmpty;

    /// <summary>
    /// Compatibility-only: copies this borrowed delta into an owned <see cref="QueryChange"/> (boxes
    /// values). NOT for the hot path — this is the bridge to the ergonomic owned API.
    /// </summary>
    public QueryChange Materialize() => new(
        MaterializeSet(_schema, _added),
        MaterializeSet(_schema, _removed),
        MaterializeSet(_schema, _updated),
        MaterializeSet(_schema, _updatedBefore));

    /// <summary>Alias for <see cref="Materialize"/>.</summary>
    public QueryChange ToOwned() => Materialize();

    private static IReadOnlyList<IReadOnlyDictionary<string, object?>> MaterializeSet(
        ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells)
    {
        int width = schema.ColumnCount;
        if (width == 0 || cells.IsEmpty)
        {
            return [];
        }

        int rowCount = cells.Length / width;
        var list = new List<IReadOnlyDictionary<string, object?>>(rowCount);
        for (int r = 0; r < rowCount; r++)
        {
            var row = new RowRef(schema, cells.Slice(r * width, width));
            list.Add(row.ToOwnedDictionary());
        }

        return list;
    }
}
