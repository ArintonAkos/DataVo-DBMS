using System.Globalization;
using System.Text.RegularExpressions;

namespace DataVo.Core.Parser.Utils;

internal static class ExpressionValueComparer
{
    /// <summary>
    /// Compares two expression values for equality with optional trimming of quoted strings and numeric tolerance. Handles nulls, numeric types, and string comparisons.
    /// </summary>
    /// <param name="leftValue">The left value to compare.</param>
    /// <param name="rightValue">The right value to compare.</param>
    /// <param name="trimQuotedStrings">Whether to trim single quotes from string values before comparison.</param>
    /// <param name="useNumericTolerance">Whether to use numeric tolerance for floating-point comparisons.</param>
    /// <param name="numericTolerance">The tolerance for numeric comparisons.</param>
    /// <returns>True if the values are considered equal, false otherwise.</returns>
    public static bool AreEqual(object? leftValue, object? rightValue, bool trimQuotedStrings = false, bool useNumericTolerance = false, double numericTolerance = 0.0001)
    {
        if (trimQuotedStrings && leftValue is string leftString && rightValue is string rightString)
        {
            leftValue = leftString.Trim('\'');
            rightValue = rightString.Trim('\'');
        }

        if (leftValue == null && rightValue == null) return true;
        if (leftValue == null || rightValue == null) return false;

        if (string.Equals(
            Convert.ToString(leftValue, CultureInfo.InvariantCulture),
            Convert.ToString(rightValue, CultureInfo.InvariantCulture),
            StringComparison.Ordinal))
        {
            return true;
        }

        if (TryConvertToDouble(leftValue, out double leftNumber) &&
            TryConvertToDouble(rightValue, out double rightNumber))
        {
            if (useNumericTolerance)
            {
                return Math.Abs(leftNumber - rightNumber) < numericTolerance;
            }

            return leftNumber == rightNumber;
        }

        return string.Equals(
            Convert.ToString(leftValue, CultureInfo.InvariantCulture),
            Convert.ToString(rightValue, CultureInfo.InvariantCulture),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Compares two expression values with optional trimming of quoted strings and numeric tolerance.
    /// Returns -1 if left is less than right, 0 if equal, and 1 if left is greater than right.
    /// </summary>
    /// <param name="leftValue">The left value to compare.</param>
    /// <param name="rightValue">The right value to compare.</param>
    /// <param name="trimQuotedStrings">Whether to trim single quotes from string values before comparison.</param>
    /// <returns>-1 if leftValue is less than rightValue, 0 if they are equal, and 1 if leftValue is greater than rightValue.</returns>
    public static int Compare(object? leftValue, object? rightValue, bool trimQuotedStrings = false)
    {
        if (trimQuotedStrings && leftValue is string leftString && rightValue is string rightString)
        {
            leftValue = leftString.Trim('\'');
            rightValue = rightString.Trim('\'');
        }

        if (leftValue == null && rightValue == null) return 0;
        if (leftValue == null) return -1;
        if (rightValue == null) return 1;

        if (TryConvertToDouble(leftValue, out double leftNumber) &&
            TryConvertToDouble(rightValue, out double rightNumber))
        {
            return leftNumber.CompareTo(rightNumber);
        }

        if (leftValue is IComparable comparable && leftValue.GetType() == rightValue.GetType())
        {
            return comparable.CompareTo(rightValue);
        }

        return string.Compare(
            Convert.ToString(leftValue, CultureInfo.InvariantCulture),
            Convert.ToString(rightValue, CultureInfo.InvariantCulture),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Evaluates SQL LIKE pattern matching using '%' and '_' wildcards.
    /// </summary>
    /// <param name="inputValue">The input value being tested.</param>
    /// <param name="patternValue">The SQL LIKE pattern.</param>
    /// <param name="trimQuotedStrings">Whether to trim single quotes from string values first.</param>
    /// <returns><see langword="true"/> when the input matches the pattern; otherwise <see langword="false"/>.</returns>
    public static bool MatchesLike(object? inputValue, object? patternValue, bool trimQuotedStrings = false)
    {
        if (inputValue == null || patternValue == null)
        {
            return false;
        }

        string input = Convert.ToString(inputValue) ?? string.Empty;
        string pattern = Convert.ToString(patternValue) ?? string.Empty;

        if (trimQuotedStrings)
        {
            input = input.Trim('\'');
            pattern = pattern.Trim('\'');
        }

        string regexPattern = "^" + Regex.Escape(pattern)
            .Replace("%", ".*")
            .Replace("_", ".") + "$";

        return Regex.IsMatch(input, regexPattern, RegexOptions.Singleline);
    }

    private static bool TryConvertToDouble(object value, out double number)
    {
        switch (value)
        {
            case byte typed:
                number = typed;
                return true;
            case sbyte typed:
                number = typed;
                return true;
            case short typed:
                number = typed;
                return true;
            case ushort typed:
                number = typed;
                return true;
            case int typed:
                number = typed;
                return true;
            case uint typed:
                number = typed;
                return true;
            case long typed:
                number = typed;
                return true;
            case ulong typed:
                number = typed;
                return true;
            case float typed:
                number = typed;
                return true;
            case double typed:
                number = typed;
                return true;
            case decimal typed:
                number = (double)typed;
                return true;
            case string text:
                return double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out number)
                    || double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.CurrentCulture, out number);
            default:
                number = default;
                return false;
        }
    }
}
