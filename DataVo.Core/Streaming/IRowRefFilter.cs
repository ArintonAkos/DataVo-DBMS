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

    /// <summary>Tests one row.</summary>
    /// <param name="row">The row, valid only for this call.</param>
    /// <returns><see langword="true"/> when the row meets the condition.</returns>
    bool Matches(in RowRef row);
}
