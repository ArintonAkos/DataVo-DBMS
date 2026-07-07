namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// The shared, immutable column layout for a reactive query's projected rows: one instance per
/// query/subscription, referenced by every <see cref="RowRef"/> so column names are never
/// re-allocated per row. Name lookups go through a single case-insensitive ordinal map.
/// </summary>
public sealed class ReactiveRowSchema
{
    private readonly string[] _columns;
    private readonly Dictionary<string, int> _ordinals;

    /// <summary>Creates a schema from the given column names, in projection order.</summary>
    public ReactiveRowSchema(params string[] columns)
        : this((IReadOnlyList<string>)columns)
    {
    }

    /// <summary>Creates a schema from the given column names, in projection order.</summary>
    public ReactiveRowSchema(IReadOnlyList<string> columns)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(columns);
        _columns = columns.ToArray();
        _ordinals = new Dictionary<string, int>(_columns.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < _columns.Length; i++)
        {
            _ordinals[_columns[i]] = i;
        }
    }

    /// <summary>The number of projected columns.</summary>
    public int ColumnCount => _columns.Length;

    /// <summary>The column names, in projection order.</summary>
    public ReadOnlySpan<string> Columns => _columns;

    /// <summary>The column name at <paramref name="ordinal"/>.</summary>
    public string ColumnAt(int ordinal) => _columns[ordinal];

    /// <summary>Resolves a column name to its ordinal (case-insensitive).</summary>
    public bool TryGetOrdinal(string column, out int ordinal) =>
        _ordinals.TryGetValue(column, out ordinal);
}
