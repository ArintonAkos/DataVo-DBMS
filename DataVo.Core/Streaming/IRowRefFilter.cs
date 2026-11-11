using System.ComponentModel;

namespace DataVo.Core;

/// <summary>
/// A row condition compiled from a query's lambda by <c>DataVo.Generators</c>. It reads the columns it tests
/// straight from a <see cref="RowRef"/>, so a scan that only counts or tests rows builds no row objects.
/// Application code does not implement this interface.
/// </summary>
/// <remarks>
/// Implementations are structs, and the scan is generic over the struct, so the condition is compiled into the
/// scan loop itself instead of being called through a delegate.
/// </remarks>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface IRowRefFilter
{
    /// <summary>Receives the table position of each column the condition reads, in the order the plan named them.</summary>
    /// <param name="ordinals">One position per named column.</param>
    void Bind(ReadOnlySpan<int> ordinals);

    /// <summary>
    /// Receives, for each named column, the byte offset of its value in a stored row whose cells up to the last
    /// named column are all present. Called only for tables where those offsets are the same in every such row.
    /// </summary>
    /// <param name="offsets">One offset per named column.</param>
    void BindOffsets(ReadOnlySpan<int> offsets);

    /// <summary>Tests one row.</summary>
    /// <param name="row">The row, valid only for this call.</param>
    /// <returns><see langword="true"/> when the row meets the condition.</returns>
    bool Matches(in RowRef row);

    /// <summary>
    /// Tests one row from its stored bytes, reading each value at the offset given to <see cref="BindOffsets"/>.
    /// The scan calls this only for a row it has checked: every cell up to the last named column is present
    /// where the offsets say, so no named column is NULL.
    /// </summary>
    /// <param name="row">The row's stored bytes, valid only for this call.</param>
    /// <returns><see langword="true"/> when the row meets the condition.</returns>
    bool MatchesBytes(ReadOnlySpan<byte> row);

    /// <summary>
    /// Tests one row of an in-memory table from its cells, indexed by the positions given to <see cref="Bind"/>.
    /// The scan calls this only for a row it has checked: every named column's cell holds a value of the
    /// column's own type, so none is NULL.
    /// </summary>
    /// <param name="cells">The row's cells in table order, valid only for this call.</param>
    /// <returns><see langword="true"/> when the row meets the condition.</returns>
    bool MatchesCells(ReadOnlySpan<Runtime.Reactive.CellValue> cells);
}
