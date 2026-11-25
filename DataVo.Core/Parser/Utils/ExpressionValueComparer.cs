using System.Globalization;
using DataVo.Core.Compat;

namespace DataVo.Core.Parser.Utils;

internal static class ExpressionValueComparer
{
    /// <summary>
    /// SQL <c>&lt;&gt;</c>: a NULL operand makes the result unknown, which a filter treats as false.
    /// (Plain <c>!AreEqual</c> made <c>B &lt;&gt; 8</c> true for NULL B, so DELETE removed those rows.)
    /// </summary>
    public static bool SqlNotEquals(object? leftValue, object? rightValue, bool trimQuotedStrings = false, bool useNumericTolerance = false) =>
        leftValue is not null && rightValue is not null
        && !AreEqual(leftValue, rightValue, trimQuotedStrings, useNumericTolerance);

    /// <summary>SQL <c>=</c>: NULL equals nothing, not even NULL (unlike <see cref="AreEqual"/>, used for grouping).</summary>
    public static bool SqlEquals(object? leftValue, object? rightValue, bool trimQuotedStrings = false, bool useNumericTolerance = false) =>
        leftValue is not null && rightValue is not null
        && AreEqual(leftValue, rightValue, trimQuotedStrings, useNumericTolerance);

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
        if (trimQuotedStrings)
        {
            if (leftValue is string leftString)
            {
                leftValue = UnquoteSingleQuoted(leftString);
            }

            if (rightValue is string rightString)
            {
                rightValue = UnquoteSingleQuoted(rightString);
            }
        }

        if (leftValue == null && rightValue == null) return true;
        if (leftValue == null || rightValue == null) return false;

        if (TryAsGuidPair(leftValue, rightValue, out Guid leftGuid, out Guid rightGuid))
        {
            return leftGuid == rightGuid;
        }

        if (TryAsBooleanPair(leftValue, rightValue, out bool leftBoolean, out bool rightBoolean))
        {
            return leftBoolean == rightBoolean;
        }

        if (TryAsDatePair(leftValue, rightValue, out DataVoDate leftDate, out DataVoDate rightDate))
        {
            return leftDate == rightDate;
        }

        if (TryAsDateTimePair(leftValue, rightValue, out long leftTicks, out long rightTicks))
        {
            return leftTicks == rightTicks;
        }

        if (TryConvertToInt64(leftValue, out long leftInteger) && TryConvertToInt64(rightValue, out long rightInteger))
        {
            return leftInteger == rightInteger; // a double cannot tell large integers apart
        }

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
        if (trimQuotedStrings)
        {
            if (leftValue is string leftString)
            {
                leftValue = UnquoteSingleQuoted(leftString);
            }

            if (rightValue is string rightString)
            {
                rightValue = UnquoteSingleQuoted(rightString);
            }
        }

        if (leftValue == null && rightValue == null) return 0;
        if (leftValue == null) return -1;
        if (rightValue == null) return 1;

        if (TryAsGuidPair(leftValue, rightValue, out Guid leftGuid, out Guid rightGuid))
        {
            return leftGuid.CompareTo(rightGuid);
        }

        if (TryAsBooleanPair(leftValue, rightValue, out bool leftBoolean, out bool rightBoolean))
        {
            return leftBoolean.CompareTo(rightBoolean);
        }

        if (TryAsDatePair(leftValue, rightValue, out DataVoDate leftDate, out DataVoDate rightDate))
        {
            return leftDate.CompareTo(rightDate);
        }

        if (TryAsDateTimePair(leftValue, rightValue, out long leftTicks, out long rightTicks))
        {
            return leftTicks.CompareTo(rightTicks);
        }

        if (TryConvertToInt64(leftValue, out long leftInteger) && TryConvertToInt64(rightValue, out long rightInteger))
        {
            return leftInteger.CompareTo(rightInteger);
        }

        if (TryConvertToDouble(leftValue, out double leftNumber) &&
            TryConvertToDouble(rightValue, out double rightNumber))
        {
            return leftNumber.CompareTo(rightNumber);
        }

        // Text compares ordinally, as the typed paths and indexes do: string.CompareTo would use the current culture.
        if (leftValue is string leftText && rightValue is string rightText)
        {
            return string.CompareOrdinal(leftText, rightText);
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
    /// A BIT value compared with a literal: TRUE/FALSE arrive as booleans, but <c>1</c>/<c>0</c> arrive as
    /// integers (or as text when written before the column) and <c>'true'</c>/<c>'false'</c> as text, which
    /// INSERT also accepts for a BIT column. When one
    /// side is a boolean and the other is one of those, both are read as booleans. Any other pairing (a
    /// boolean against <c>2</c> or <c>'yes'</c>) is not a boolean pair and stays unequal.
    /// </summary>
    private static bool TryAsBooleanPair(object leftValue, object rightValue, out bool left, out bool right)
    {
        left = right = false;
        return (leftValue is bool || rightValue is bool)
            && TryAsBoolean(leftValue, out left)
            && TryAsBoolean(rightValue, out right);
    }

    private static bool TryAsBoolean(object value, out bool result)
    {
        switch (value)
        {
            case bool boolean:
                result = boolean;
                return true;
            case ulong unsigned:
                result = unsigned == 1;
                return unsigned <= 1;
            case int or long or short or byte or sbyte or uint or ushort:
                long integer = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                result = integer == 1;
                return integer is 0 or 1;
            // A number written before the column (1 = IsActive) reaches the comparer as its text.
            case string text:
                string trimmed = text.Trim();
                result = trimmed == "1";
                return trimmed is "1" or "0" || bool.TryParse(trimmed, out result);
            default:
                result = false;
                return false;
        }
    }

    /// <summary>
    /// A GUID column compared with a literal: the literal arrives as (possibly quoted) text, so compare it
    /// as a GUID. Comparing the texts missed differences in case and quoting, which made
    /// <c>Id &lt;&gt; '...'</c> true for the very row it names.
    /// </summary>
    private static bool TryAsGuidPair(object leftValue, object rightValue, out Guid left, out Guid right)
    {
        left = default;
        right = default;
        if (leftValue is not Guid && rightValue is not Guid)
        {
            return false;
        }

        return TryAsGuid(leftValue, out left) && TryAsGuid(rightValue, out right);
    }

    private static bool TryAsGuid(object value, out Guid guid)
    {
        switch (value)
        {
            case Guid g:
                guid = g;
                return true;
            case string text:
                return Guid.TryParse(text.Trim().Trim('\''), out guid);
            default:
                guid = default;
                return false;
        }
    }

    /// <summary>
    /// Attempts to interpret a pair of values as <see cref="DataVoDate"/> instances when at least
    /// one operand is already a date or convertible to date representation.
    /// </summary>
    /// <param name="leftValue">The first operand, typically a column value or date literal.</param>
    /// <param name="rightValue">The second operand, typically a comparison literal or parameter.</param>
    /// <param name="left">The resolved date for the left operand when successful.</param>
    /// <param name="right">The resolved date for the right operand when successful.</param>
    /// <returns><see langword="true"/> if both operands were successfully converted to dates; otherwise, <see langword="false"/>.</returns>
    private static bool TryAsDatePair(object leftValue, object rightValue, out DataVoDate left, out DataVoDate right)
    {
        left = default;
        right = default;
        if (leftValue is not DataVoDate && rightValue is not DataVoDate)
        {
            return false;
        }

        return TryAsDate(leftValue, out left) && TryAsDate(rightValue, out right);
    }

    /// <summary>
    /// Interprets a pair as two DATETIME instants (UTC ticks) when either operand is a <see cref="DateTime"/> or
    /// <see cref="DateTimeOffset"/> and the other converts (a point in time, a DATE at midnight UTC, or ISO 8601 text).
    /// </summary>
    private static bool TryAsDateTimePair(object leftValue, object rightValue, out long left, out long right)
    {
        left = 0;
        right = 0;
        if (leftValue is not (DateTime or DateTimeOffset) && rightValue is not (DateTime or DateTimeOffset))
        {
            return false;
        }

        return DataVoDateTime.TryConvert(leftValue, out left) && DataVoDateTime.TryConvert(rightValue, out right);
    }

    /// <summary>
    /// Converts a boxed value, string representation, or date adapter candidate into a <see cref="DataVoDate"/>.
    /// </summary>
    /// <param name="value">The value to convert.</param>
    /// <param name="date">The parsed date when conversion succeeds.</param>
    /// <returns><see langword="true"/> if the value could be converted; otherwise, <see langword="false"/>.</returns>
    private static bool TryAsDate(object value, out DataVoDate date)
    {
        switch (value)
        {
            case DataVoDate d:
                date = d;
                return true;
            case DateTime dt:
                date = DataVoDate.FromDateTime(dt);
                return true;
            case string text:
                string trimmed = text.Trim().Trim('\'');
                return DataVoDate.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.None, out date)
                    || DataVoDate.TryParse(trimmed, CultureInfo.CurrentCulture, DateTimeStyles.None, out date);
            default:
                if (DateObjectAdapter.TryConvert(value, out DataVoDate adapted))
                {
                    date = adapted;
                    return true;
                }

                date = default;
                return false;
        }
    }

    /// <summary>SQL <c>NOT LIKE</c>: unknown (false) when either side is NULL, otherwise the negated match.</summary>
    public static bool MatchesNotLike(object? inputValue, object? patternValue, bool trimQuotedStrings = false) =>
        inputValue is not null && patternValue is not null && !MatchesLike(inputValue, patternValue, trimQuotedStrings);

    /// <summary>
    /// Evaluates SQL LIKE pattern matching using '%' and '_' wildcards (see <see cref="SqlLikePattern"/>).
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
            input = UnquoteSingleQuoted(input);
            pattern = UnquoteSingleQuoted(pattern);
        }

        return SqlLikePattern.IsMatch(input, pattern);
    }

    private static string UnquoteSingleQuoted(string value)
        => value.Length >= 2 && value[0] == '\'' && value[^1] == '\'' ? value[1..^1] : value;

    private static bool TryConvertToInt64(object value, out long number)
    {
        switch (value)
        {
            case int typed:
                number = typed;
                return true;
            case long typed:
                number = typed;
                return true;
            case string text:
                return long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out number);
            default:
                number = 0;
                return false;
        }
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
