#if NET10_0_OR_GREATER
namespace DataVo.Core.Compat;

internal static class ScalarKeyFormatter
{
    public static string FormatBracketed(int value, IFormatProvider provider) =>
        string.Create(provider, $"[{value}]");

    public static string FormatBracketed(long value, IFormatProvider provider) =>
        string.Create(provider, $"[{value}]");

    public static string FormatBracketed(double value, IFormatProvider provider) =>
        string.Create(provider, $"[{value}]");

    public static string FormatBracketed(float value, IFormatProvider provider) =>
        string.Create(provider, $"[{value}]");

    public static string FormatBracketed(decimal value, IFormatProvider provider) =>
        string.Create(provider, $"[{value}]");
}
#endif
