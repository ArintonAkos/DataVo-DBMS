#if NETSTANDARD2_1
namespace DataVo.Core.Compat;

internal static class ScalarKeyFormatter
{
    public static string FormatBracketed(int value, IFormatProvider provider) =>
        string.Format(provider, "[{0}]", value);

    public static string FormatBracketed(long value, IFormatProvider provider) =>
        string.Format(provider, "[{0}]", value);

    public static string FormatBracketed(double value, IFormatProvider provider) =>
        string.Format(provider, "[{0}]", value);

    public static string FormatBracketed(float value, IFormatProvider provider) =>
        string.Format(provider, "[{0}]", value);

    public static string FormatBracketed(decimal value, IFormatProvider provider) =>
        string.Format(provider, "[{0}]", value);
}
#endif
