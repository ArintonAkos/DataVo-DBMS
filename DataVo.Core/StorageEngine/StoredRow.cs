using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.StorageEngine;

/// <summary>
/// An owned typed storage row: a shared <see cref="ReactiveRowSchema"/> plus an owned
/// <see cref="CellValue"/> array. The constructor <b>clones</b> the incoming cells, so the row owns its
/// array and never exposes it mutably — borrowed, no-copy reads go through <see cref="AsView"/>.
/// </summary>
internal sealed class StoredRow
{
    private readonly ReactiveRowSchema _schema;
    private readonly CellValue[] _cells;

    public StoredRow(ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells)
    {
        ArgumentNullException.ThrowIfNull(schema);
        if (cells.Length != schema.ColumnCount)
        {
            throw new ArgumentException(
                $"Row has {cells.Length} cells but schema has {schema.ColumnCount} columns.",
                nameof(cells));
        }

        _schema = schema;
        _cells = cells.ToArray(); // owned clone — the caller's buffer can change without affecting this row
    }

    private StoredRow(ReactiveRowSchema schema, CellValue[] ownedCells)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(ownedCells);
        if (ownedCells.Length != schema.ColumnCount)
        {
            throw new ArgumentException(
                $"Row has {ownedCells.Length} cells but schema has {schema.ColumnCount} columns.",
                nameof(ownedCells));
        }

        _schema = schema;
        _cells = ownedCells;
    }

    internal static StoredRow FromOwnedCells(ReactiveRowSchema schema, CellValue[] ownedCells) => new(schema, ownedCells);

    /// <summary>The shared column layout.</summary>
    public ReactiveRowSchema Schema => _schema;

    /// <summary>The number of cells.</summary>
    public int Count => _cells.Length;

    /// <summary>The cell at <paramref name="ordinal"/>.</summary>
    public CellValue this[int ordinal] => _cells[ordinal];

    /// <summary>The cell for a column name (case-insensitive).</summary>
    /// <exception cref="KeyNotFoundException">No such column.</exception>
    public CellValue this[string column] =>
        _schema.TryGetOrdinal(column, out int ordinal)
            ? _cells[ordinal]
            : throw new KeyNotFoundException($"Column '{column}' is not in the row schema.");

    /// <summary>Resolves a cell by column name (case-insensitive).</summary>
    public bool TryGet(string column, out CellValue value)
    {
        if (_schema.TryGetOrdinal(column, out int ordinal))
        {
            value = _cells[ordinal];
            return true;
        }

        value = CellValue.Null;
        return false;
    }

    /// <summary>A borrowed, no-copy view over this row's cells. Valid only while this row is alive.</summary>
    public StoredRowView AsView() => new(_schema, _cells);

    /// <summary>A read-only dictionary adapter over this row (legacy/boundary consumers). Boxes on access;
    /// VECTOR values are cloned, so stored state cannot be mutated through it.</summary>
    public StoredRowDictionaryView AsDictionary() => new(_schema, _cells);
}

/// <summary>
/// A borrowed, no-copy view over a typed row's cells (a <see cref="ReactiveRowSchema"/> plus a
/// <see cref="ReadOnlySpan{T}"/> of <see cref="CellValue"/>). A <c>ref struct</c>: valid only for the
/// frame that owns the backing buffer; it never copies.
/// </summary>
internal readonly ref struct StoredRowView
{
    private readonly ReactiveRowSchema _schema;
    private readonly ReadOnlySpan<CellValue> _cells;

    public StoredRowView(ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells)
    {
        ArgumentNullException.ThrowIfNull(schema);
        _schema = schema;
        _cells = cells;
    }

    public ReactiveRowSchema Schema => _schema;

    public int Count => _cells.Length;

    /// <summary>The borrowed cells, in column order.</summary>
    public ReadOnlySpan<CellValue> Cells => _cells;

    public CellValue this[int ordinal] => _cells[ordinal];

    public CellValue this[string column] =>
        _schema.TryGetOrdinal(column, out int ordinal)
            ? _cells[ordinal]
            : throw new KeyNotFoundException($"Column '{column}' is not in the row schema.");

    public bool TryGet(string column, out CellValue value)
    {
        if (_schema.TryGetOrdinal(column, out int ordinal))
        {
            value = _cells[ordinal];
            return true;
        }

        value = CellValue.Null;
        return false;
    }
}
