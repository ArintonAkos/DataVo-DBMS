namespace DataVo.Core.Models.Catalog;

/// <summary>
/// The declared length of VARCHAR(n) columns. Over-long strings are rejected on every write path;
/// they are never truncated, because silent truncation is data loss.
/// </summary>
internal static class ColumnLengthRule
{
    /// <summary>Returns true when <paramref name="value"/> is a string longer than the column allows.</summary>
    public static bool Exceeds(Column column, object? value, out int length)
    {
        length = value is string text ? text.Length : 0;
        return column.Length > 0
            && column.Type.Equals("VARCHAR", StringComparison.OrdinalIgnoreCase)
            && length > column.Length;
    }

    /// <summary>The violation message; the error contract maps "too long for column" to DV4002_VALUE_TOO_LONG.</summary>
    public static string Message(string columnName, int declaredLength, int actualLength, int? rowNumber = null) =>
        $"Value of length {actualLength} is too long for column {columnName} (VARCHAR({declaredLength}))"
        + (rowNumber is int row ? $" in row {row}!" : ".");
}
