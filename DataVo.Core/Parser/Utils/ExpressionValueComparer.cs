namespace DataVo.Core.Parser.Utils;

internal static class ExpressionValueComparer
{
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
}
