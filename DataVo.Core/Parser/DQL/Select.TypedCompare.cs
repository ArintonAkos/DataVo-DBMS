using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// Comparisons of typed cells that give the order the general path gives the same values once boxed
/// (<see cref="DynamicObjectComparer"/>: NULL first, then the value's own <c>CompareTo</c>). The typed paths use
/// them to reject a row against the rows they already keep (a top-N, a MIN or MAX) without boxing its values:
/// almost every row of a scan is rejected, and boxing each first cost 84 bytes a row on an
/// <c>ORDER BY x DESC, Id LIMIT 10</c>.
/// </summary>
internal partial class Select
{
    /// <summary>
    /// Orders two cells of <paramref name="column"/> as their boxed values order. Integers and floating-point
    /// numbers are compared in place; other types are converted and compared as before.
    /// </summary>
    private static int CompareCells(Column column, CellValue left, CellValue right)
    {
        if (left.IsNull || right.IsNull)
        {
            return left.IsNull ? (right.IsNull ? 0 : -1) : 1;
        }

        if (left.Type == right.Type)
        {
            switch (left.Type)
            {
                case CellType.Int32:
                    return left.AsInt32().CompareTo(right.AsInt32());
                case CellType.Int64:
                    return left.AsInt64().CompareTo(right.AsInt64());
                case CellType.Double:
                    // A FLOAT column boxes as float: compare at that precision, as the boxed values would.
                    return IsFloatColumn(column)
                        ? ((float)left.AsDouble()).CompareTo((float)right.AsDouble())
                        : left.AsDouble().CompareTo(right.AsDouble());
            }
        }

        return DynamicObjectComparer.Instance.Compare(
            StorageContext.ConvertCellForLegacyDictionary(column, left),
            StorageContext.ConvertCellForLegacyDictionary(column, right));
    }

    /// <summary>
    /// Orders a cell of <paramref name="column"/> against a value boxed from the same column, without boxing the
    /// cell. False when the pair is not a number of one type; the caller then boxes and compares as before.
    /// </summary>
    private static bool TryCompareCellToBoxed(Column column, CellValue cell, object? boxed, out int order)
    {
        switch (cell.Type)
        {
            case CellType.Int32 when boxed is int value:
                order = cell.AsInt32().CompareTo(value);
                return true;
            case CellType.Int64 when boxed is long value:
                order = cell.AsInt64().CompareTo(value);
                return true;
            case CellType.Double when boxed is float value && IsFloatColumn(column):
                order = ((float)cell.AsDouble()).CompareTo(value);
                return true;
            case CellType.Double when boxed is double value && !IsFloatColumn(column):
                order = cell.AsDouble().CompareTo(value);
                return true;
            default:
                order = 0;
                return false;
        }
    }

    private static bool IsFloatColumn(Column column) => column.Type.Equals("FLOAT", StringComparison.OrdinalIgnoreCase);
}
