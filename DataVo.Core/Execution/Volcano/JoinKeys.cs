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
}
