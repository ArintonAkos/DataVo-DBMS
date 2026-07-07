namespace DataVo.Core.Compat;

internal static class ThrowHelper
{
    public static void ThrowIfNull<T>(T? value, string? paramName = null)
    {
        if (value is null)
        {
            throw new ArgumentNullException(paramName);
        }
    }

    public static void ThrowIfNullOrWhiteSpace(string? value, string? paramName = null)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("Value cannot be null or whitespace.", paramName);
        }
    }

    public static void ThrowIfDisposed(bool condition, object instance)
    {
        if (condition)
        {
            throw new ObjectDisposedException(instance.GetType().Name);
        }
    }

    public static void ThrowIfNegative(int value, string? paramName = null)
    {
        if (value < 0)
        {
            throw new ArgumentOutOfRangeException(paramName, value, "Value cannot be negative.");
        }
    }
}
