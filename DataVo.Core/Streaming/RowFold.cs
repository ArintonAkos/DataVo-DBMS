using System.ComponentModel;

namespace DataVo.Core;

/// <summary>
/// How a compiled scan combines one column's values over the rows its condition accepts. Used by the code
/// <c>DataVo.Generators</c> writes; application code calls <c>Sum</c>, <c>Min</c> and <c>Max</c> on a query instead.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public enum RowFold : byte
{
    /// <summary>Adds the values up.</summary>
    Sum,

    /// <summary>Keeps the smallest value.</summary>
    Min,

    /// <summary>Keeps the largest value.</summary>
    Max,
}

/// <summary>
/// What a compiled scan found in one column of the matching rows. Used by the code <c>DataVo.Generators</c> writes.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public struct RowFoldTotals
{
    /// <summary>How many matching rows held a value in the column. NULL cells are not counted.</summary>
    public int Values;

    /// <summary>
    /// The result over an <c>INT</c>, <c>BIGINT</c> or <c>DATE</c> column; a date is its day number. Meaningless
    /// for <see cref="RowFold.Min"/> and <see cref="RowFold.Max"/> when <see cref="Values"/> is 0.
    /// </summary>
    public long Whole;

    /// <summary>
    /// The result over a <c>FLOAT</c> or <c>DOUBLE</c> column. Meaningless for <see cref="RowFold.Min"/> and
    /// <see cref="RowFold.Max"/> when <see cref="Values"/> is 0.
    /// </summary>
    public double Real;
}
