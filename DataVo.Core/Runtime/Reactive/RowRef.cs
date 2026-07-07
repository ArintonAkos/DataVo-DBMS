namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A borrowed view over a single reactive row: a <see cref="ReactiveRowSchema"/> plus the row's
/// <see cref="CellValue"/> cells. Valid only for the duration of the callback/operator frame that
/// owns the backing buffer — being a <c>ref struct</c>, it cannot be stored, boxed, or captured.
/// Retain data via <see cref="ToOwnedDictionary"/>.
/// </summary>
public readonly ref struct RowRef
{
    private readonly ReactiveRowSchema _schema;
    private readonly ReadOnlySpan<CellValue> _values;

    /// <summary>Creates a borrowed row view. The cell count must equal the schema's column count.</summary>
    public RowRef(ReactiveRowSchema schema, ReadOnlySpan<CellValue> values)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(schema);
        if (values.Length != schema.ColumnCount)
        {
            throw new ArgumentException(
                $"Row has {values.Length} cells but schema has {schema.ColumnCount} columns.",
                nameof(values));
        }

        _schema = schema;
        _values = values;
    }

    /// <summary>The number of cells in this row.</summary>
    public int Count => _values.Length;

    /// <summary>The shared column names.</summary>
    public ReadOnlySpan<string> Columns => _schema.Columns;

    /// <summary>The row's cells, in column order.</summary>
    public ReadOnlySpan<CellValue> Values => _values;

    /// <summary>The cell at <paramref name="ordinal"/>.</summary>
    public ref readonly CellValue this[int ordinal] => ref _values[ordinal];

    /// <summary>Resolves a cell by column name (case-insensitive).</summary>
    public bool TryGet(string column, out CellValue value)
    {
        if (_schema.TryGetOrdinal(column, out int ordinal))
        {
            value = _values[ordinal];
            return true;
        }

        value = CellValue.Null;
        return false;
    }

    /// <summary>The cell with the given column name (case-insensitive).</summary>
    /// <exception cref="KeyNotFoundException">No such column.</exception>
    public CellValue this[string column] =>
        _schema.TryGetOrdinal(column, out int ordinal)
            ? _values[ordinal]
            : throw new KeyNotFoundException($"Column '{column}' is not in the row schema.");

    /// <summary>
    /// Compatibility-only: copies this row into a boxed, case-insensitive dictionary. NOT for the
    /// hot path — used to materialize to the owned <see cref="QueryChange"/> API.
    /// </summary>
    public IReadOnlyDictionary<string, object?> ToOwnedDictionary()
    {
        var dict = new Dictionary<string, object?>(_values.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < _values.Length; i++)
        {
            dict[_schema.ColumnAt(i)] = _values[i].ToObject();
        }

        return dict;
    }
}
