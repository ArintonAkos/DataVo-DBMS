using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// Lends each thread reusable cell buffers for decoding packed rows that are handed to a visitor as cells.
/// Buffers are lent as a stack, so a visitor that reads another row on the same thread while it still holds
/// the cells of the first gets a different buffer: the first row's cells are never overwritten under it.
/// </summary>
/// <remarks>
/// Every <see cref="Rent"/> must be matched by a <see cref="Return"/> on the same thread, innermost first; use
/// <see langword="try"/>/<see langword="finally"/>. A returned buffer is cleared, so it does not keep the
/// strings and vectors of the last row alive.
/// </remarks>
/// <example>
/// <code>
/// CellValue[] cells = CellScratch.Rent(layout.ColumnCount);
/// try
/// {
///     FlatRowCodec.Decode(layout, record, references, referenceBase, cells);
///     visit(cells.AsSpan(0, layout.ColumnCount));
/// }
/// finally
/// {
///     CellScratch.Return(cells, layout.ColumnCount);
/// }
/// </code>
/// </example>
internal static class CellScratch
{
    private const int SmallestBuffer = 16;

    [ThreadStatic]
    private static CellValue[]?[]? _buffers;

    [ThreadStatic]
    private static int _depth;

    /// <summary>Borrows a buffer of at least <paramref name="cells"/> cells for the current thread.</summary>
    public static CellValue[] Rent(int cells)
    {
        CellValue[]?[] buffers = _buffers ??= new CellValue[]?[4];
        int depth = _depth;
        if (depth == buffers.Length)
        {
            Array.Resize(ref buffers, depth * 2);
            _buffers = buffers;
        }

        CellValue[]? buffer = buffers[depth];
        if (buffer is null || buffer.Length < cells)
        {
            buffers[depth] = buffer = new CellValue[Math.Max(cells, SmallestBuffer)];
        }

        _depth = depth + 1;
        return buffer;
    }

    /// <summary>Gives back the buffer most recently borrowed on this thread.</summary>
    /// <param name="buffer">The buffer.</param>
    /// <param name="cells">How many of its cells were used, and are cleared.</param>
    public static void Return(CellValue[] buffer, int cells)
    {
        _depth--;
        Array.Clear(buffer, 0, cells);
    }
}
