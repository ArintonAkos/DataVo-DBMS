using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core;

/// <summary>How a scan reads the column it sums.</summary>
internal enum RowSumKind
{
    /// <summary>The scan only counts.</summary>
    None,

    /// <summary>An <c>INT</c> column, added to <see cref="RowAggregate.Whole"/>.</summary>
    Int32,

    /// <summary>A <c>BIGINT</c> column, added to <see cref="RowAggregate.Whole"/>.</summary>
    Int64,

    /// <summary>A <c>FLOAT</c> column, added to <see cref="RowAggregate.Real"/>.</summary>
    Float,

    /// <summary>A <c>DOUBLE</c> column, added to <see cref="RowAggregate.Real"/>.</summary>
    Double,
}

/// <summary>
/// What a compiled scan accumulates over the rows its condition accepts: how many there are, and the sum of one
/// column. The totals live in this struct inside the scan loop, so nothing is allocated per row.
/// </summary>
internal struct RowAggregate
{
    /// <summary>The number of matching rows.</summary>
    public int Count;

    /// <summary>The sum of an integer column. NULL cells add nothing.</summary>
    public long Whole;

    /// <summary>The sum of a floating-point column, accumulated as a double. NULL cells add nothing.</summary>
    public double Real;

    /// <summary>
    /// Chooses the reader for a summed column, or <see cref="RowSumKind.None"/> when the column's type does not
    /// fit the requested sum (an integer sum over a floating-point column, or the reverse).
    /// </summary>
    public static RowSumKind KindFor(StorageKind column, bool floating) => (column, floating) switch
    {
        (StorageKind.Int, false) => RowSumKind.Int32,
        (StorageKind.BigInt, false) => RowSumKind.Int64,
        (StorageKind.Float, true) => RowSumKind.Float,
        (StorageKind.Double, true) => RowSumKind.Double,
        _ => RowSumKind.None,
    };

    /// <summary>Counts one matching row and adds its summed column.</summary>
    /// <exception cref="OverflowException">An integer sum left the range of <see cref="long"/>.</exception>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public void Add(in RowRef row, RowSumKind kind, int ordinal)
    {
        Count++;
        switch (kind)
        {
            case RowSumKind.Int32:
                if (row.TryGetInt32(ordinal, out int small))
                {
                    Whole = checked(Whole + small);
                }

                break;
            case RowSumKind.Int64:
                if (row.TryGetInt64(ordinal, out long large))
                {
                    Whole = checked(Whole + large);
                }

                break;
            case RowSumKind.Float:
                if (row.TryGetFloat(ordinal, out float single))
                {
                    Real += single;
                }

                break;
            case RowSumKind.Double:
                if (row.TryGetDouble(ordinal, out double real))
                {
                    Real += real;
                }

                break;
        }
    }
}
