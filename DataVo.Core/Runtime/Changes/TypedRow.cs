using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.Runtime.Changes;

/// <summary>
/// Owned, persistable typed row image for change capture. Use <see cref="AsRowRef"/> for
/// allocation-free reads during reactive dispatch.
/// </summary>
public readonly struct TypedRow
{
    private readonly CellValue[]? _cells;

    /// <summary>Creates an owned typed row image by copying <paramref name="cells"/>.</summary>
    public TypedRow(ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells)
    {
        ArgumentNullException.ThrowIfNull(schema);
        if (cells.Length != schema.ColumnCount)
        {
            throw new ArgumentException(
                $"Row has {cells.Length} cells but schema has {schema.ColumnCount} columns.",
                nameof(cells));
        }

        Schema = schema;
        _cells = cells.ToArray();
    }

    private TypedRow(ReactiveRowSchema schema, CellValue[] ownedCells, bool owned)
    {
        Schema = schema;
        _cells = ownedCells;
    }

    /// <summary>
    /// Wraps an already-owned cell array without copying. The caller must never mutate
    /// <paramref name="ownedCells"/> after this call (immutability invariant — see Slice 5 design).
    /// </summary>
    public static TypedRow FromOwnedCells(ReactiveRowSchema schema, CellValue[] ownedCells)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(ownedCells);
        if (ownedCells.Length != schema.ColumnCount)
        {
            throw new ArgumentException(
                $"Row has {ownedCells.Length} cells but schema has {schema.ColumnCount} columns.",
                nameof(ownedCells));
        }

        return new TypedRow(schema, ownedCells, owned: true);
    }

    /// <summary>Gets the schema that describes the row's cell order.</summary>
    public ReactiveRowSchema Schema { get; }

    /// <summary>Gets the owned cells, in schema order.</summary>
    public ReadOnlyMemory<CellValue> Cells => _cells is null
        ? ReadOnlyMemory<CellValue>.Empty
        : _cells;

    /// <summary>Gets a borrowed row view over the owned cells.</summary>
    public RowRef AsRowRef()
    {
        if (_cells is null)
        {
            throw new InvalidOperationException("TypedRow has not been initialized.");
        }

        return new RowRef(Schema, _cells);
    }
}
