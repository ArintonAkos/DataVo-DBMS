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

        if (leftValue is IConvertible leftConvertible && rightValue is IConvertible rightConvertible)
        {
            try
            {
                if (Convert.ToString(leftValue) == Convert.ToString(rightValue)) return true;

                double leftNumber = leftConvertible.ToDouble(null);
                double rightNumber = rightConvertible.ToDouble(null);

                if (useNumericTolerance)
                {
                    return Math.Abs(leftNumber - rightNumber) < numericTolerance;
                }

                return leftNumber == rightNumber;
            }
            catch (Exception)
            {
            }
        }

        return Convert.ToString(leftValue) == Convert.ToString(rightValue);
    }

    /// <summary>
    /// Compares two expression values with optional trimming of quoted strings and numeric tolerance. Returns -1 if left < right, 0 if equal, and 1 if left > right.
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

        if (leftValue is IConvertible leftConvertible && rightValue is IConvertible rightConvertible)
        {
            try
            {
                double leftNumber = leftConvertible.ToDouble(null);
                double rightNumber = rightConvertible.ToDouble(null);
                return leftNumber.CompareTo(rightNumber);
            }
            catch (Exception)
            {
            }
        }

        try
        {
            return Comparer<object?>.Default.Compare(leftValue, rightValue);
        }
        catch
        {
            return string.Compare(Convert.ToString(leftValue), Convert.ToString(rightValue), StringComparison.Ordinal);
        }
    }

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
}
