using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// Turns rows borrowed from a <see cref="FlatRowStore"/> into cells for code that reads rows as cells. A packed
/// row is decoded into a buffer borrowed from <see cref="CellScratch"/>, which is reused for every row read
/// through the same reader; a row kept apart is handed out as it is. Nothing is allocated per row.
/// </summary>
/// <remarks>
/// The cells of one row are valid until the next row is read or the reader is disposed. Dispose the reader on
/// the thread that used it, in a <see langword="finally"/> block.
/// </remarks>
/// <example>
/// <code>
/// var reader = new PackedRowReader();
/// try
/// {
///     if (store.TryRead(index, out FlatRow row))
///     {
///         ReadOnlySpan&lt;CellValue&gt; cells = reader.Cells(store.Layout, row);
///     }
/// }
/// finally
/// {
///     reader.Dispose();
/// }
/// </code>
/// </example>
internal struct PackedRowReader
{
    private CellValue[]? _buffer;
    private int _cells;

    /// <summary>The cells of a borrowed row.</summary>
    /// <param name="layout">The layout of the store the row came from.</param>
    /// <param name="row">The row.</param>
    public ReadOnlySpan<CellValue> Cells(RowLayout layout, FlatRow row)
    {
        if (row.IsKeptApart)
        {
            return row.Cells;
        }

        if (_buffer is null || _cells != layout.ColumnCount)
        {
            Dispose();
            _cells = layout.ColumnCount;
            _buffer = CellScratch.Rent(_cells);
        }

        FlatRowCodec.Decode(layout, row.Record, row.References, row.ReferenceBase, _buffer);
        return new ReadOnlySpan<CellValue>(_buffer, 0, _cells);
    }

    /// <summary>Gives the borrowed buffer back, if one was borrowed.</summary>
    public void Dispose()
    {
        if (_buffer is not null)
        {
            CellScratch.Return(_buffer, _cells);
            _buffer = null;
        }
    }
}
