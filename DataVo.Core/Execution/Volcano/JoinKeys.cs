using System.Globalization;

namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Canonical equality keys for Volcano equi-joins, shared by the hash and nested-loop join operators.
/// </summary>
internal static class JoinKeys
{
    /// <summary>
    /// Builds the comparison key of a join value. A <c>NULL</c> value has no key: under SQL three-valued
    /// logic <c>NULL = x</c> is never true, so a row whose join value is <c>NULL</c> matches nothing.
    /// </summary>
    /// <param name="value">The join column's value.</param>
    /// <param name="key">The canonical key; numeric values of any CLR type compare by decimal value.</param>
    /// <returns><see langword="false"/> when the value is <c>NULL</c>.</returns>
    public static bool TryNormalize(object? value, out string key)
    {
        if (value is null or DBNull)
        {
            key = string.Empty;
            return false;
        }

        key = Normalize(value);
        return true;
    }

    private static string Normalize(object value)
    {
        if (value is string text)
        {
            return text.Trim('\'');
        }

        if (value is IConvertible && value is not bool && value is not char)
        {
            try
            {
                decimal numeric = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                return numeric.ToString(CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                // Not numeric: compare by its invariant text below.
            }
        }

        return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
    }

    /// <summary>
    /// Merges a left and right typed execution row into a single qualified <see cref="ExecutionRow"/>.
    /// </summary>
    /// <param name="leftRow">The left-hand input row.</param>
    /// <param name="rightRow">The right-hand input row.</param>
    /// <param name="leftTableName">The table qualifier applied to unqualified columns from <paramref name="leftRow"/>.</param>
    /// <param name="rightTableName">The table qualifier applied to unqualified columns from <paramref name="rightRow"/>.</param>
    /// <param name="outputRowId">The row identifier assigned to the merged row.</param>
    /// <returns>A merged <see cref="ExecutionRow"/> containing columns from both sides.</returns>
    public static ExecutionRow MergeRows(
        TypedExecutionRow leftRow,
        TypedExecutionRow rightRow,
        string leftTableName,
        string rightTableName,
        long outputRowId)
    {
        int estimatedCapacity = leftRow.Values.Count + rightRow.Values.Count;
        var values = new Dictionary<string, object?>(Math.Max(estimatedCapacity, 1));

        foreach (var cell in leftRow.Values)
        {
            string key = cell.Key.Contains('.') ? cell.Key : $"{leftTableName}.{cell.Key}";
            values[key] = cell.Value;
        }

        foreach (var cell in rightRow.Values)
        {
            string key = cell.Key.Contains('.') ? cell.Key : $"{rightTableName}.{cell.Key}";
            values[key] = cell.Value;
        }

        return new ExecutionRow(outputRowId, values);
    }
}
