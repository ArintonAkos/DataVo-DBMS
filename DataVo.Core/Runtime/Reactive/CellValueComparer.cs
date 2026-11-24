using DataVo.Core.Parser.Utils;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Allocation-free ordering comparer for <see cref="CellValue"/> matching SQL <c>ORDER BY</c> semantics
/// (<c>NULL</c> sorts lowest, numeric types compare by numeric value, strings compare ordinally).
/// </summary>
/// <example>
/// <code>
/// int order = CellValueComparer.Compare(CellValue.From(10), CellValue.From(20L));
/// </code>
/// </example>
internal sealed class CellValueComparer : IComparer<CellValue>
{
    /// <summary>Gets the shared comparer instance.</summary>
    public static CellValueComparer Instance { get; } = new();

    /// <inheritdoc />
    int IComparer<CellValue>.Compare(CellValue x, CellValue y) => Compare(x, y);

    /// <summary>
    /// Compares two <see cref="CellValue"/> instances without boxing common scalar types.
    /// </summary>
    /// <param name="left">The left cell value.</param>
    /// <param name="right">The right cell value.</param>
    /// <returns>A negative value if <paramref name="left"/> precedes <paramref name="right"/>, zero if equal, or positive otherwise.</returns>
    public static int Compare(CellValue left, CellValue right)
    {
        if (left.IsNull)
        {
            return right.IsNull ? 0 : -1;
        }

        if (right.IsNull)
        {
            return 1;
        }

        if (left.Type == right.Type)
        {
            return CompareSameType(left, right);
        }

        if (TryAsInt64(left, out long leftInt) && TryAsInt64(right, out long rightInt))
        {
            return leftInt.CompareTo(rightInt);
        }

        if (TryAsDouble(left, out double leftDouble) && TryAsDouble(right, out double rightDouble))
        {
            return leftDouble.CompareTo(rightDouble);
        }

        return ExpressionValueComparer.Compare(left.ToObject(), right.ToObject(), trimQuotedStrings: false);
    }

    private static int CompareSameType(CellValue left, CellValue right) => left.Type switch
    {
        CellType.Boolean => left.AsBoolean().CompareTo(right.AsBoolean()),
        CellType.Int32 => left.AsInt32().CompareTo(right.AsInt32()),
        CellType.Int64 => left.AsInt64().CompareTo(right.AsInt64()),
        CellType.Double => left.AsDouble().CompareTo(right.AsDouble()),
        CellType.Decimal => left.AsDecimal().CompareTo(right.AsDecimal()),
        CellType.String => string.CompareOrdinal(left.AsString(), right.AsString()),
        CellType.Date => left.AsDate().DayNumber.CompareTo(right.AsDate().DayNumber),
        CellType.DateTime => left.AsUtcTicks().CompareTo(right.AsUtcTicks()),
        CellType.Guid => left.AsGuid().CompareTo(right.AsGuid()),
        CellType.Vector => CompareVectors(left.AsVectorReadOnlySpan(), right.AsVectorReadOnlySpan()),
        _ => ExpressionValueComparer.Compare(left.ToObject(), right.ToObject(), trimQuotedStrings: false),
    };

    private static int CompareVectors(ReadOnlySpan<float> left, ReadOnlySpan<float> right)
    {
        int len = Math.Min(left.Length, right.Length);
        for (int i = 0; i < len; i++)
        {
            int cmp = left[i].CompareTo(right[i]);
            if (cmp != 0)
            {
                return cmp;
            }
        }

        return left.Length.CompareTo(right.Length);
    }

    internal static bool TryAsInt64(CellValue cell, out long value)
    {
        switch (cell.Type)
        {
            case CellType.Int32:
                value = cell.AsInt32();
                return true;
            case CellType.Int64:
                value = cell.AsInt64();
                return true;
            default:
                value = 0;
                return false;
        }
    }

    internal static bool TryAsDouble(CellValue cell, out double value)
    {
        switch (cell.Type)
        {
            case CellType.Int32:
                value = cell.AsInt32();
                return true;
            case CellType.Int64:
                value = cell.AsInt64();
                return true;
            case CellType.Double:
                value = cell.AsDouble();
                return true;
            case CellType.Decimal:
                value = (double)cell.AsDecimal();
                return true;
            default:
                value = 0;
                return false;
        }
    }
}

/// <summary>
/// Allocation-free equality comparer for <see cref="CellValue"/> keys and projected cells, normalizing
/// integer widths (<see cref="CellType.Int32"/> and <see cref="CellType.Int64"/>) to a single 64-bit identity.
/// </summary>
/// <example>
/// <code>
/// var map = new Dictionary&lt;CellValue, int&gt;(CellValueEqualityComparer.Instance);
/// </code>
/// </example>
internal sealed class CellValueEqualityComparer : IEqualityComparer<CellValue>
{
    /// <summary>Gets the shared equality comparer instance.</summary>
    public static CellValueEqualityComparer Instance { get; } = new();

    /// <inheritdoc />
    public bool Equals(CellValue x, CellValue y) => AreEqual(x, y);

    /// <inheritdoc />
    public int GetHashCode(CellValue obj) => Hash(obj);

    /// <summary>
    /// Determines whether two <see cref="CellValue"/> instances represent the same value without boxing.
    /// </summary>
    /// <param name="left">The first cell value.</param>
    /// <param name="right">The second cell value.</param>
    /// <returns><see langword="true"/> when both cells hold equal values; otherwise <see langword="false"/>.</returns>
    public static bool AreEqual(CellValue left, CellValue right)
    {
        if (left.IsNull || right.IsNull)
        {
            return left.IsNull && right.IsNull;
        }

        if (left.Type == right.Type)
        {
            return left.Type switch
            {
                CellType.Boolean => left.AsBoolean() == right.AsBoolean(),
                CellType.Int32 => left.AsInt32() == right.AsInt32(),
                CellType.Int64 => left.AsInt64() == right.AsInt64(),
                CellType.Double => left.AsDouble().Equals(right.AsDouble()),
                CellType.Decimal => left.AsDecimal() == right.AsDecimal(),
                CellType.String => string.Equals(left.AsString(), right.AsString(), StringComparison.Ordinal),
                CellType.Date => left.AsDate() == right.AsDate(),
                CellType.DateTime => left.AsUtcTicks() == right.AsUtcTicks(),
                CellType.Guid => left.AsGuid() == right.AsGuid(),
                CellType.Vector => left.AsVectorReadOnlySpan().SequenceEqual(right.AsVectorReadOnlySpan()),
                _ => false,
            };
        }

        if (CellValueComparer.TryAsInt64(left, out long leftInt) && CellValueComparer.TryAsInt64(right, out long rightInt))
        {
            return leftInt == rightInt;
        }

        if (CellValueComparer.TryAsDouble(left, out double leftDouble) && CellValueComparer.TryAsDouble(right, out double rightDouble))
        {
            return leftDouble.Equals(rightDouble);
        }

        return ExpressionValueComparer.AreEqual(left.ToObject(), right.ToObject(), trimQuotedStrings: false);
    }

    /// <summary>
    /// Computes an unboxed hash code consistent with <see cref="AreEqual"/>.
    /// </summary>
    /// <param name="cell">The cell value to hash.</param>
    /// <returns>The hash code.</returns>
    public static int Hash(CellValue cell) => cell.Type switch
    {
        CellType.Null => 0,
        CellType.Boolean => cell.AsBoolean() ? 1 : 0,
        CellType.Int32 => ((long)cell.AsInt32()).GetHashCode(),
        CellType.Int64 => cell.AsInt64().GetHashCode(),
        CellType.Double => cell.AsDouble().GetHashCode(),
        CellType.Decimal => cell.AsDecimal().GetHashCode(),
        CellType.String => StringComparer.Ordinal.GetHashCode(cell.AsString() ?? string.Empty),
        CellType.Date => cell.AsDate().DayNumber.GetHashCode(),
        CellType.DateTime => cell.AsUtcTicks().GetHashCode(),
        CellType.Guid => cell.AsGuid().GetHashCode(),
        CellType.Vector => HashVector(cell.AsVectorReadOnlySpan()),
        _ => 0,
    };

    private static int HashVector(ReadOnlySpan<float> values)
    {
        int hash = 17;
        for (int i = 0; i < values.Length; i++)
        {
            hash = unchecked((hash * 31) + values[i].GetHashCode());
        }

        return hash;
    }
}
